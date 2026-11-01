//go:build integration && duckdb_arrow

package duckplane

// Integration fixture (copied from components/queryengine/integration_test.go):
// Postgres + Lakekeeper (authn off, allowall) + silo on the host network via
// the docker CLI, container names prefixed dg-it-. Local only, not in PR CI.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os/exec"
	"strings"
	"testing"
	"time"
)

const (
	minioRootUser     = "dg-it-root"
	minioRootPassword = "dg-it-secret-pw"
	minioBucket       = "dg-it-bucket"
	pgPassword        = "dg-it-pg-pw"
	lkEncryptionKey   = "dg-it-throwaway-encryption-key"
	lakekeeperImage   = "quay.io/lakekeeper/catalog:v0.12.1"
	// dummyCatalogJWT is never validated (Lakekeeper authn is off).
	dummyCatalogJWT = "eyJhbGciOiJub25lIn0.eyJzdWIiOiJkZyJ9.c2ln"
	// siloImage: the pinned drop-in from charts/datuplet-infra/values.yaml.
	siloImage = "docker.io/pgsty/silo:RELEASE.2026-10-26T00-00-00Z@sha256:7bb2aff6b89efe1cb1303c10fc21d579447434ed06649ac9ec05a5e2dbf30ad7"
)

// fixture holds the provisioned endpoints + identifiers needed by the test.
type fixture struct {
	t             *testing.T
	pgPort        int
	minioPort     int
	lkPort        int
	metricsPort   int
	projectID     string
	warehouseName string
}

func (f *fixture) lakekeeperBase() string { return fmt.Sprintf("http://127.0.0.1:%d", f.lkPort) }
func (f *fixture) catalogURL() string     { return f.lakekeeperBase() + "/catalog" }
func (f *fixture) minioEndpoint() string  { return fmt.Sprintf("http://127.0.0.1:%d", f.minioPort) }

// provision spins up postgres + minio + lakekeeper on the host network, polls
// each for readiness, then bootstraps lakekeeper and creates an S3 warehouse.
// It registers teardown so containers are removed even on failure.
func provision(ctx context.Context, t *testing.T) *fixture {
	removeLeftovers()

	f := &fixture{
		t:             t,
		pgPort:        freePort(t),
		minioPort:     freePort(t),
		lkPort:        freePort(t),
		metricsPort:   freePort(t),
		warehouseName: "dg-it-wh",
	}
	t.Logf("ports: pg=%d minio=%d lakekeeper=%d", f.pgPort, f.minioPort, f.lkPort)

	// 1. postgres (lakekeeper metadata DB) on a non-default port. Env (password
	//    + db) must be set at launch; the `-c port=` arg moves it off 5432 to
	//    avoid clashing with any developer postgres on the host network.
	runDetachedEnv(t, "dg-it-pg",
		[]string{"POSTGRES_PASSWORD=" + pgPassword, "POSTGRES_DB=lakekeeper"},
		"postgres:16",
		"-c", fmt.Sprintf("port=%d", f.pgPort),
	)
	waitPostgres(ctx, t, f.pgPort)

	// 2. MinIO + bucket.
	runDetachedEnv(t, "dg-it-minio",
		[]string{"MINIO_ROOT_USER=" + minioRootUser, "MINIO_ROOT_PASSWORD=" + minioRootPassword},
		siloImage,
		"server", "/data",
		"--address", fmt.Sprintf(":%d", f.minioPort),
	)
	waitHTTP(ctx, t, fmt.Sprintf("%s/minio/health/live", f.minioEndpoint()))
	makeBucket(ctx, t, f)

	// 3. lakekeeper: migrate (one-shot) then serve. No OPENID / OPENFGA config
	//    → anonymous + allowall.
	lkEnv := []string{
		fmt.Sprintf("LAKEKEEPER__PG_DATABASE_URL_READ=postgres://postgres:%s@127.0.0.1:%d/lakekeeper", pgPassword, f.pgPort),
		fmt.Sprintf("LAKEKEEPER__PG_DATABASE_URL_WRITE=postgres://postgres:%s@127.0.0.1:%d/lakekeeper", pgPassword, f.pgPort),
		"LAKEKEEPER__PG_ENCRYPTION_KEY=" + lkEncryptionKey,
		fmt.Sprintf("LAKEKEEPER__LISTEN_PORT=%d", f.lkPort),
		// Off the default 9000: on the host network another Lakekeeper or an
		// S3 server may already hold it.
		fmt.Sprintf("LAKEKEEPER__METRICS_PORT=%d", f.metricsPort),
	}
	runOneShot(ctx, t, "dg-it-lk-migrate", lkEnv, lakekeeperImage, "migrate")
	runDetachedEnv(t, "dg-it-lk", lkEnv, lakekeeperImage, "serve")
	waitHTTP(ctx, t, f.lakekeeperBase()+"/health")

	// 4. Bootstrap + create the S3 warehouse (no JWT — authn disabled).
	bootstrapLakekeeper(ctx, t, f)
	f.projectID = createWarehouse(ctx, t, f)
	t.Logf("provisioned: project=%s warehouse=%s", f.projectID, f.warehouseName)

	return f
}

func (f *fixture) teardown() {
	for _, name := range []string{"dg-it-lk", "dg-it-lk-migrate", "dg-it-minio", "dg-it-pg", "dg-it-mc"} {
		_ = exec.Command("docker", "rm", "-f", name).Run()
	}
}

func removeLeftovers() {
	for _, name := range []string{"dg-it-lk", "dg-it-lk-migrate", "dg-it-minio", "dg-it-pg", "dg-it-mc"} {
		_ = exec.Command("docker", "rm", "-f", name).Run()
	}
}

// --- docker helpers ---------------------------------------------------------

// runDetachedEnv starts a host-network container in the background with the
// given env, image, and command args.
func runDetachedEnv(t *testing.T, name string, env []string, image string, args ...string) {
	t.Helper()
	full := []string{"run", "-d", "--name", name, "--network", "host"}
	for _, e := range env {
		full = append(full, "-e", e)
	}
	full = append(full, image)
	full = append(full, args...)
	out, err := exec.Command("docker", full...).CombinedOutput()
	if err != nil {
		t.Fatalf("docker run %s: %v\n%s", name, err, out)
	}
}

// runOneShot runs a container to completion (e.g. lakekeeper migrate) and fails
// the test on non-zero exit.
func runOneShot(ctx context.Context, t *testing.T, name string, env []string, image string, args ...string) {
	t.Helper()
	full := []string{"run", "--rm", "--name", name, "--network", "host"}
	for _, e := range env {
		full = append(full, "-e", e)
	}
	full = append(full, image)
	full = append(full, args...)
	out, err := exec.CommandContext(ctx, "docker", full...).CombinedOutput()
	if err != nil {
		t.Fatalf("docker run (one-shot) %s: %v\n%s", name, err, out)
	}
}

// --- readiness probes -------------------------------------------------------

func waitHTTP(ctx context.Context, t *testing.T, url string) {
	t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		resp, err := (&http.Client{Timeout: 3 * time.Second}).Do(req)
		if err == nil {
			resp.Body.Close()
			// Both probed endpoints (lakekeeper /health, MinIO
			// /minio/health/live) return 200 only when fully ready. A stricter
			// == 200 avoids treating a transient 4xx during startup as ready.
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(2 * time.Second)
	}
	t.Fatalf("timed out waiting for %s", url)
}

func waitPostgres(ctx context.Context, t *testing.T, port int) {
	t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		out, err := exec.CommandContext(ctx, "docker", "exec", "dg-it-pg",
			"pg_isready", "-h", "127.0.0.1", "-p", fmt.Sprint(port)).CombinedOutput()
		if err == nil && strings.Contains(string(out), "accepting connections") {
			return
		}
		time.Sleep(2 * time.Second)
	}
	t.Fatalf("timed out waiting for postgres on :%d", port)
}

// makeBucket creates the warehouse bucket via a one-shot silo container, which
// bundles mc.
func makeBucket(ctx context.Context, t *testing.T, f *fixture) {
	t.Helper()
	script := fmt.Sprintf(
		"mc alias set local %s %s %s && mc mb --ignore-existing local/%s",
		f.minioEndpoint(), minioRootUser, minioRootPassword, minioBucket,
	)
	out, err := exec.CommandContext(ctx, "docker", "run", "--rm", "--name", "dg-it-mc",
		"--network", "host", "--entrypoint", "sh",
		siloImage, "-c", script).CombinedOutput()
	if err != nil {
		t.Fatalf("create bucket: %v\n%s", err, out)
	}
}

// --- lakekeeper REST (no auth) ---------------------------------------------

func bootstrapLakekeeper(ctx context.Context, t *testing.T, f *fixture) {
	t.Helper()
	postJSONNoAuth(ctx, t, f.lakekeeperBase()+"/management/v1/bootstrap",
		map[string]any{"accept-terms-of-use": true}, http.StatusNoContent)
}

// createWarehouse POSTs an S3/MinIO warehouse mirroring buildWarehouseBody in
// cmd/pipeline-api/admin_lakekeeper.go, then reads the project-id back via the
// warehouse list. Returns the lakekeeper project id (the default nil-UUID
// project, since this fixture creates no extra projects).
func createWarehouse(ctx context.Context, t *testing.T, f *fixture) string {
	t.Helper()
	body := map[string]any{
		"warehouse-name": f.warehouseName,
		"storage-profile": map[string]any{
			"type":              "s3",
			"bucket":            minioBucket,
			"region":            "local-01",
			"sts-enabled":       true,
			"flavor":            "s3-compat",
			"endpoint":          f.minioEndpoint(),
			"path-style-access": true,
		},
		"storage-credential": map[string]any{
			"type":                  "s3",
			"credential-type":       "access-key",
			"aws-access-key-id":     minioRootUser,
			"aws-secret-access-key": minioRootPassword,
		},
		"delete-profile": map[string]any{"type": "hard"},
	}
	postJSONNoAuth(ctx, t, f.lakekeeperBase()+"/management/v1/warehouse", body, http.StatusCreated)

	// Read the project id off the warehouse list. With authn disabled the
	// warehouse lands in lakekeeper's default project (nil UUID); read it back
	// rather than hardcoding, so a future lakekeeper default change is caught.
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, f.lakekeeperBase()+"/management/v1/warehouse", nil)
	resp, err := (&http.Client{Timeout: 15 * time.Second}).Do(req)
	if err != nil {
		t.Fatalf("list warehouses: %v", err)
	}
	defer resp.Body.Close()
	rb, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("list warehouses HTTP %d: %s", resp.StatusCode, rb)
	}
	var parsed struct {
		Warehouses []struct {
			Name      string `json:"name"`
			ProjectID string `json:"project-id"`
		} `json:"warehouses"`
	}
	if err := json.Unmarshal(rb, &parsed); err != nil {
		t.Fatalf("decode warehouses: %v\n%s", err, rb)
	}
	for _, w := range parsed.Warehouses {
		if w.Name == f.warehouseName {
			if w.ProjectID == "" {
				return "00000000-0000-0000-0000-000000000000"
			}
			return w.ProjectID
		}
	}
	t.Fatalf("warehouse %q not found in list: %s", f.warehouseName, rb)
	return ""
}

func postJSONNoAuth(ctx context.Context, t *testing.T, url string, body any, want int) {
	t.Helper()
	buf := &bytes.Buffer{}
	if err := json.NewEncoder(buf).Encode(body); err != nil {
		t.Fatalf("encode body for %s: %v", url, err)
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

// freePort asks the kernel for an unused TCP port, then closes the listener so
// the container can bind it. Small race window between close and container
// bind (a classic TOCTOU): explicitly accepted here because this is a
// manual / CI-opt-in integration fixture (behind the `integration` build tag),
// not hot production code, and it avoids hardcoded clashes (e.g. a kubectl
// port-forward on 8181).
func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("freePort: %v", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}
