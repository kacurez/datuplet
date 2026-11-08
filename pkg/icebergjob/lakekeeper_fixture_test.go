//go:build integration

package icebergjob

// Integration fixture: Postgres + Lakekeeper (authn off, allowall) + silo on
// the host network via the docker CLI, container names prefixed ij-it-. A
// copy of pkg/datagateway/goplane/fixture_test.go (goplane imports this
// package, so it cannot be shared). Local only, not in PR CI.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/iceberg-go/catalog"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/datuplet/datuplet/pkg/catalogwriter"
)

const (
	siloRootUser     = "ij-it-root"
	siloRootPassword = "ij-it-secret-pw"
	siloBucket       = "ij-it-bucket"
	pgPassword       = "ij-it-pg-pw"
	lkEncryptionKey  = "ij-it-throwaway-encryption-key"
	lakekeeperImage  = "quay.io/lakekeeper/catalog:v0.14.2"
	// dummyCatalogJWT is never validated (Lakekeeper authn is off).
	dummyCatalogJWT = "eyJhbGciOiJub25lIn0.eyJzdWIiOiJndyJ9.c2ln"
	// siloImage: the pinned drop-in from charts/datuplet-infra/values.yaml.
	siloImage = "docker.io/pgsty/silo:RELEASE.2026-10-26T00-00-00Z@sha256:7bb2aff6b89efe1cb1303c10fc21d579447434ed06649ac9ec05a5e2dbf30ad7"
)

var fixtureContainers = []string{"ij-it-lk", "ij-it-lk-migrate", "ij-it-silo", "ij-it-pg"}

type fixture struct {
	pgPort, siloPort, lkPort, metricsPort int
	projectID, warehouseName              string
}

func (f *fixture) lakekeeperBase() string { return fmt.Sprintf("http://127.0.0.1:%d", f.lkPort) }
func (f *fixture) catalogURL() string     { return f.lakekeeperBase() + "/catalog" }
func (f *fixture) siloEndpoint() string   { return fmt.Sprintf("http://127.0.0.1:%d", f.siloPort) }

// siloCore is a root-credential client of the fixture's object store (for
// test assertions only).
func (f *fixture) siloCore(t *testing.T) *minio.Core {
	t.Helper()
	c, err := minio.NewCore(fmt.Sprintf("127.0.0.1:%d", f.siloPort), &minio.Options{
		Creds: credentials.NewStaticV4(siloRootUser, siloRootPassword, ""), Secure: false, Region: "local-01",
	})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// provision starts postgres, silo and lakekeeper on the host network, waits
// for each, bootstraps lakekeeper and creates an S3 warehouse.
func provision(ctx context.Context, t *testing.T) *fixture {
	removeContainers()
	f := &fixture{
		pgPort: freePort(t), siloPort: freePort(t), lkPort: freePort(t), metricsPort: freePort(t),
		warehouseName: "ij-it-wh",
	}
	t.Logf("ports: pg=%d silo=%d lakekeeper=%d", f.pgPort, f.siloPort, f.lkPort)

	runDetached(t, "ij-it-pg", []string{"POSTGRES_PASSWORD=" + pgPassword, "POSTGRES_DB=lakekeeper"},
		"postgres:16", "-c", fmt.Sprintf("port=%d", f.pgPort))
	waitPostgres(ctx, t, f.pgPort)

	runDetached(t, "ij-it-silo", []string{"MINIO_ROOT_USER=" + siloRootUser, "MINIO_ROOT_PASSWORD=" + siloRootPassword},
		siloImage, "server", "/data", "--address", fmt.Sprintf(":%d", f.siloPort))
	waitHTTP(ctx, t, f.siloEndpoint()+"/minio/health/live")
	if err := f.siloCore(t).MakeBucket(ctx, siloBucket, minio.MakeBucketOptions{Region: "local-01"}); err != nil {
		t.Fatalf("make bucket: %v", err)
	}

	db := fmt.Sprintf("postgres://postgres:%s@127.0.0.1:%d/lakekeeper", pgPassword, f.pgPort)
	lkEnv := []string{
		"LAKEKEEPER__PG_DATABASE_URL_READ=" + db,
		"LAKEKEEPER__PG_DATABASE_URL_WRITE=" + db,
		"LAKEKEEPER__PG_ENCRYPTION_KEY=" + lkEncryptionKey,
		fmt.Sprintf("LAKEKEEPER__LISTEN_PORT=%d", f.lkPort),
		fmt.Sprintf("LAKEKEEPER__METRICS_PORT=%d", f.metricsPort),
	}
	runOneShot(ctx, t, "ij-it-lk-migrate", lkEnv, lakekeeperImage, "migrate")
	runDetached(t, "ij-it-lk", lkEnv, lakekeeperImage, "serve")
	waitHTTP(ctx, t, f.lakekeeperBase()+"/health")

	postJSON(ctx, t, f.lakekeeperBase()+"/management/v1/bootstrap", map[string]any{"accept-terms-of-use": true}, http.StatusNoContent)
	f.projectID = createWarehouse(ctx, t, f)
	t.Logf("provisioned: project=%s warehouse=%s", f.projectID, f.warehouseName)
	return f
}

func removeContainers() {
	for _, name := range fixtureContainers {
		_ = exec.Command("docker", "rm", "-f", "-v", name).Run()
	}
}

func runDetached(t *testing.T, name string, env []string, image string, args ...string) {
	t.Helper()
	full := []string{"run", "-d", "--name", name, "--network", "host"}
	for _, e := range env {
		full = append(full, "-e", e)
	}
	full = append(append(full, image), args...)
	if out, err := exec.Command("docker", full...).CombinedOutput(); err != nil {
		t.Fatalf("docker run %s: %v\n%s", name, err, out)
	}
}

func runOneShot(ctx context.Context, t *testing.T, name string, env []string, image string, args ...string) {
	t.Helper()
	full := []string{"run", "--rm", "--name", name, "--network", "host"}
	for _, e := range env {
		full = append(full, "-e", e)
	}
	full = append(append(full, image), args...)
	if out, err := exec.CommandContext(ctx, "docker", full...).CombinedOutput(); err != nil {
		t.Fatalf("docker run (one-shot) %s: %v\n%s", name, err, out)
	}
}

func waitHTTP(ctx context.Context, t *testing.T, url string) {
	t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		resp, err := (&http.Client{Timeout: 3 * time.Second}).Do(req)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(time.Second)
	}
	t.Fatalf("timed out waiting for %s", url)
}

func waitPostgres(ctx context.Context, t *testing.T, port int) {
	t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		out, err := exec.CommandContext(ctx, "docker", "exec", "ij-it-pg",
			"pg_isready", "-h", "127.0.0.1", "-p", fmt.Sprint(port)).CombinedOutput()
		if err == nil && strings.Contains(string(out), "accepting connections") {
			return
		}
		time.Sleep(time.Second)
	}
	t.Fatalf("timed out waiting for postgres on :%d", port)
}

// createWarehouse creates an S3 warehouse on silo (as cmd/pipeline-api
// admin_lakekeeper.go does) and returns its project id.
func createWarehouse(ctx context.Context, t *testing.T, f *fixture) string {
	t.Helper()
	postJSON(ctx, t, f.lakekeeperBase()+"/management/v1/warehouse", map[string]any{
		"warehouse-name": f.warehouseName,
		"storage-profile": map[string]any{
			"type": "s3", "bucket": siloBucket, "region": "local-01", "sts-enabled": true,
			"flavor": "s3-compat", "endpoint": f.siloEndpoint(), "path-style-access": true,
		},
		"storage-credential": map[string]any{
			"type": "s3", "credential-type": "access-key",
			"aws-access-key-id": siloRootUser, "aws-secret-access-key": siloRootPassword,
		},
		"delete-profile":          map[string]any{"type": "hard"},
		"allowed-format-versions": []int{1, 2, 3},
		"default-format-version":  3,
	}, http.StatusCreated)

	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, f.lakekeeperBase()+"/management/v1/warehouse", nil)
	resp, err := (&http.Client{Timeout: 15 * time.Second}).Do(req)
	if err != nil {
		t.Fatalf("list warehouses: %v", err)
	}
	defer resp.Body.Close()
	rb, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	var parsed struct {
		Warehouses []struct {
			Name      string `json:"name"`
			ProjectID string `json:"project-id"`
		} `json:"warehouses"`
	}
	if resp.StatusCode != http.StatusOK || json.Unmarshal(rb, &parsed) != nil {
		t.Fatalf("list warehouses HTTP %d: %s", resp.StatusCode, rb)
	}
	for _, w := range parsed.Warehouses {
		if w.Name == f.warehouseName {
			if w.ProjectID == "" {
				return "00000000-0000-0000-0000-000000000000"
			}
			return w.ProjectID
		}
	}
	t.Fatalf("warehouse %q not in list: %s", f.warehouseName, rb)
	return ""
}

func postJSON(ctx context.Context, t *testing.T, url string, body any, want int) {
	t.Helper()
	buf := &bytes.Buffer{}
	if err := json.NewEncoder(buf).Encode(body); err != nil {
		t.Fatal(err)
	}
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, url, buf)
	req.Header.Set("content-type", "application/json")
	resp, err := (&http.Client{Timeout: 30 * time.Second}).Do(req)
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	defer resp.Body.Close()
	rb, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode != want {
		t.Fatalf("POST %s: HTTP %d (want %d): %s", url, resp.StatusCode, want, rb)
	}
}

// freePort returns a currently unused TCP port (small TOCTOU window accepted
// for an opt-in local fixture).
func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

var (
	lkOnce sync.Once
	lkFx   *fixture
	lkErr  string
)

func TestMain(m *testing.M) {
	code := m.Run()
	if lkFx != nil {
		removeContainers()
	}
	os.Exit(code)
}

// sharedLakekeeper provisions the fixture once per test binary.
func sharedLakekeeper(t *testing.T) *fixture {
	t.Helper()
	lkOnce.Do(func() {
		if out, err := exec.Command("docker", "info").CombinedOutput(); err != nil {
			lkErr = fmt.Sprintf("BLOCKED: docker not available: %v\n%s", err, out)
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
		defer cancel()
		lkFx = provision(ctx, t)
	})
	if lkFx == nil {
		if lkErr == "" {
			lkErr = "integration fixture failed in an earlier test"
		}
		t.Fatal(lkErr)
	}
	return lkFx
}

// lkCatalog opens a REST catalog on the fixture's warehouse; tables loaded
// through it carry vended S3 credentials.
func lkCatalog(t *testing.T, f *fixture) catalog.Catalog {
	t.Helper()
	cli, err := catalogwriter.NewClient(context.Background(), catalogwriter.ClientConfig{
		URI:           f.catalogURL(),
		Warehouse:     f.warehouseName,
		ProjectID:     f.projectID,
		TokenProvider: func(context.Context) (string, error) { return dummyCatalogJWT, nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	return cli.Catalog
}
