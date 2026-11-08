package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"

	datupletv1 "github.com/datuplet/datuplet/pkg/k8s/api/v1"
	"github.com/datuplet/datuplet/pkg/pipeline/config"
	"github.com/datuplet/datuplet/pkg/pipeline/validate"
	"github.com/datuplet/datuplet/pkg/pipelineapi/auth"
	"github.com/datuplet/datuplet/pkg/pipelineapi/authz/authztest"
	pipelineapidb "github.com/datuplet/datuplet/pkg/pipelineapi/db"
	"github.com/datuplet/datuplet/pkg/pipelineapi/projectgate"
	"github.com/datuplet/datuplet/pkg/pipelineapi/runbackend"
	"github.com/datuplet/datuplet/pkg/pipelineapi/store"
	"github.com/datuplet/datuplet/pkg/pipelineapi/tokens"
)

func TestSystemCompactPipelineName(t *testing.T) {
	a := SystemCompactPipelineName("raw", "events")
	if a != SystemCompactPipelineName("raw", "events") {
		t.Fatal("not deterministic")
	}
	if !strings.HasPrefix(a, "sys-compact-") || len(a) != len("sys-compact-")+16 {
		t.Errorf("name = %q", a)
	}
	if errs := validation.IsDNS1123Label(a); len(errs) > 0 {
		t.Errorf("%q is not a DNS-1123 label: %v", a, errs)
	}
	if SystemCompactPipelineName("a", "b.c") == SystemCompactPipelineName("a.b", "c") {
		t.Error("(a, b.c) and (a.b, c) must not collide")
	}
}

// fakeRunBackend stands in for runbackend.K8sBackend: it reserves the run row
// the way the real backend does, optionally blocking first so tests can hold
// a request inside TriggerRun.
type fakeRunBackend struct {
	pool    *pgxpool.Pool
	entered chan struct{}
	release chan struct{}

	mu  sync.Mutex
	err error
}

func (f *fakeRunBackend) setErr(err error) { f.mu.Lock(); f.err = err; f.mu.Unlock() }

func (f *fakeRunBackend) TriggerRun(ctx context.Context, req runbackend.TriggerRequest) (runbackend.TriggerResponse, error) {
	if f.entered != nil {
		select {
		case f.entered <- struct{}{}:
		default:
		}
	}
	if f.release != nil {
		<-f.release
	}
	f.mu.Lock()
	err := f.err
	f.mu.Unlock()
	if err != nil {
		return runbackend.TriggerResponse{}, err
	}
	id := uuid.New()
	if _, err := store.CreateRun(ctx, f.pool, store.CreateRunOpts{ID: id, ProjectID: req.ProjectID, PipelineID: req.PipelineID}); err != nil {
		return runbackend.TriggerResponse{}, err
	}
	return runbackend.TriggerResponse{RunID: id}, nil
}

func (f *fakeRunBackend) CancelRun(context.Context, runbackend.CancelRequest) error { return nil }

func compactorRegistry(t *testing.T) validate.StaticRegistry {
	t.Helper()
	schema, err := os.ReadFile("../../../components/iceberg-compactor/schema.json")
	if err != nil {
		t.Fatalf("read compactor schema: %v", err)
	}
	return validate.StaticRegistry{
		"iceberg-compactor": datupletv1.ComponentDefinition{
			ObjectMeta: metav1.ObjectMeta{Name: "iceberg-compactor"},
			Spec: datupletv1.ComponentDefinitionSpec{
				DefaultVersion: "v1.0.0",
				Versions:       []datupletv1.VersionSpec{{Version: "v1.0.0", Image: "x/iceberg-compactor:v1.0.0", ConfigSchema: string(schema)}},
				IO:             &datupletv1.ComponentIO{Inputs: "none", Outputs: "required"},
			},
		},
	}
}

// newFakeLakekeeper serves just enough of the REST catalog for a LoadTable
// of raw.events; every other table is a NoSuchTable 404.
func newFakeLakekeeper(t *testing.T) string {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/config", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"defaults":{},"overrides":{}}`))
	})
	mux.HandleFunc("/v1/namespaces/raw/tables/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if strings.TrimPrefix(r.URL.Path, "/v1/namespaces/raw/tables/") == "events" {
			_, _ = w.Write([]byte(fakeTableMetadataJSON("s3://datuplet/m.json", fakeUUID(1))))
			return
		}
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"error":{"message":"Table does not exist","type":"NoSuchTableException","code":404}}`))
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv.URL
}

type compactEnv struct {
	srv       *httptest.Server
	pool      *pgxpool.Pool
	backend   *fakeRunBackend
	projectID uuid.UUID
}

type compactEnvOpts struct {
	db       bool
	maxConns int32
	authzr   *authztest.Fake
	registry validate.RegistryView
	noReg    bool
}

func openCompactTestPool(t *testing.T, maxConns int32) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	if maxConns > 0 {
		cfg.MaxConns = maxConns
	}
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	t.Cleanup(pool.Close)
	if _, err := pool.Exec(context.Background(), "DROP SCHEMA public CASCADE; CREATE SCHEMA public;"); err != nil {
		t.Fatalf("reset: %v", err)
	}
	if err := pipelineapidb.Migrate(context.Background(), pool); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return pool
}

func newCompactEnv(t *testing.T, o compactEnvOpts) *compactEnv {
	t.Helper()
	env := &compactEnv{projectID: uuid.MustParse(fixtureProjectID)}
	if o.db {
		env.pool = openCompactTestPool(t, o.maxConns)
		proj, err := store.CreateProject(context.Background(), env.pool, "proj")
		if err != nil {
			t.Fatalf("create project: %v", err)
		}
		env.projectID = proj.ID
	}
	env.backend = &fakeRunBackend{pool: env.pool}
	if o.authzr == nil {
		o.authzr = dataAdminFake()
	}
	reg := o.registry
	if reg == nil && !o.noReg {
		reg = compactorRegistry(t)
	}
	h := &HTTPHandlers{
		Svc: &Service{
			LakekeeperURL: newFakeLakekeeper(t),
			Minter:        func(context.Context) (tokens.ImpersonationToken, error) { return "tok", nil },
		},
		Gate: &projectgate.Gate{
			LakekeeperProjectIDFor: func(context.Context, uuid.UUID) (string, error) { return fixtureLakekeeperProjectID, nil },
			Authorizer:             o.authzr,
			WarehouseFor:           func(context.Context, string) (string, error) { return "datuplet", nil },
		},
		Registry: reg,
		Runs:     env.backend,
	}
	if env.pool != nil {
		h.DB = env.pool
	}
	mux := http.NewServeMux()
	mux.Handle("POST /api/v1/storage/projects/{pid}/tables/{ns}/{t}/compact",
		auth.WithUser(stubResolver{}, http.HandlerFunc(h.CompactTable)))
	env.srv = httptest.NewServer(mux)
	t.Cleanup(env.srv.Close)
	return env
}

type compactResp struct {
	status int
	body   map[string]any
}

func (e *compactEnv) compact(t *testing.T, ns, table, body string) compactResp {
	t.Helper()
	url := e.srv.URL + "/api/v1/storage/projects/" + e.projectID.String() + "/tables/" + ns + "/" + table + "/compact"
	resp, err := http.Post(url, "application/json", bytes.NewReader([]byte(body)))
	if err != nil {
		t.Errorf("POST compact: %v", err)
		return compactResp{}
	}
	defer resp.Body.Close()
	out := compactResp{status: resp.StatusCode}
	_ = json.NewDecoder(resp.Body).Decode(&out.body)
	return out
}

func storedTargetSize(t *testing.T, pool *pgxpool.Pool, projectID uuid.UUID, name string) any {
	t.Helper()
	p, err := store.GetPipelineByName(context.Background(), pool, projectID, name)
	if err != nil {
		t.Fatalf("get pipeline %s: %v", name, err)
	}
	doc, err := config.Parse(p.Doc)
	if err != nil {
		t.Fatalf("parse stored doc: %v", err)
	}
	return doc.Stages[0].Components[0].Config["target_file_size_mb"]
}

func TestCompactTable_Handler(t *testing.T) {
	sysName := SystemCompactPipelineName("raw", "events")

	t.Run("no data_admin is forbidden", func(t *testing.T) {
		env := newCompactEnv(t, compactEnvOpts{authzr: allowedFake()})
		if r := env.compact(t, "raw", "events", ""); r.status != http.StatusForbidden {
			t.Fatalf("status = %d, want 403", r.status)
		}
	})
	t.Run("unknown table is 404", func(t *testing.T) {
		env := newCompactEnv(t, compactEnvOpts{})
		if r := env.compact(t, "raw", "nope", ""); r.status != http.StatusNotFound {
			t.Fatalf("status = %d, want 404", r.status)
		}
	})
	t.Run("compactor not registered is 503", func(t *testing.T) {
		env := newCompactEnv(t, compactEnvOpts{registry: validate.StaticRegistry{}})
		r := env.compact(t, "raw", "events", "")
		if r.status != http.StatusServiceUnavailable || r.body["error"] != "compactor_unavailable" {
			t.Fatalf("got %d %v, want 503 compactor_unavailable", r.status, r.body)
		}
	})
	t.Run("no registry wired is 503", func(t *testing.T) {
		env := newCompactEnv(t, compactEnvOpts{noReg: true})
		if r := env.compact(t, "raw", "events", ""); r.status != http.StatusServiceUnavailable {
			t.Fatalf("status = %d, want 503", r.status)
		}
	})
	t.Run("config out of schema range is 400", func(t *testing.T) {
		env := newCompactEnv(t, compactEnvOpts{})
		if r := env.compact(t, "raw", "events", `{"target_file_size_mb":1}`); r.status != http.StatusBadRequest {
			t.Fatalf("status = %d, want 400", r.status)
		}
	})
	t.Run("unknown body field is 400", func(t *testing.T) {
		env := newCompactEnv(t, compactEnvOpts{})
		if r := env.compact(t, "raw", "events", `{"dry_run":true}`); r.status != http.StatusBadRequest {
			t.Fatalf("status = %d, want 400", r.status)
		}
	})
	t.Run("happy path saves a system pipeline", func(t *testing.T) {
		env := newCompactEnv(t, compactEnvOpts{db: true})
		r := env.compact(t, "raw", "events", `{"target_file_size_mb":64}`)
		if r.status != http.StatusAccepted {
			t.Fatalf("status = %d %v, want 202", r.status, r.body)
		}
		if r.body["pipeline"] != sysName || r.body["target"] != "raw.events" || r.body["run_id"] == "" {
			t.Errorf("body = %v", r.body)
		}
		p, err := store.GetPipelineByName(context.Background(), env.pool, env.projectID, sysName)
		if err != nil || !p.System {
			t.Fatalf("system pipeline: %+v err=%v", p, err)
		}
		if p.Description != "Compaction of raw.events" {
			t.Errorf("description = %q", p.Description)
		}
		doc, _ := config.Parse(p.Doc)
		c := doc.Stages[0].Components[0]
		if doc.Stages[0].Name != "compact" || c.Name != "compact" || c.Component != "iceberg-compactor" ||
			c.Outputs == nil || len(c.Outputs.Tables) != 1 || c.Outputs.Tables[0].Bucket != "raw" || c.Outputs.Tables[0].Name != "events" {
			t.Errorf("doc = %+v", doc)
		}
	})
	t.Run("running compaction is 409 and the doc is unchanged", func(t *testing.T) {
		env := newCompactEnv(t, compactEnvOpts{db: true})
		first := env.compact(t, "raw", "events", `{"target_file_size_mb":64}`)
		if first.status != http.StatusAccepted {
			t.Fatalf("first: %d %v", first.status, first.body)
		}
		second := env.compact(t, "raw", "events", `{"target_file_size_mb":256}`)
		if second.status != http.StatusConflict || second.body["error"] != "compaction_running" || second.body["run_id"] != first.body["run_id"] {
			t.Fatalf("second: %d %v", second.status, second.body)
		}
		if got := storedTargetSize(t, env.pool, env.projectID, sysName); got != float64(64) {
			t.Errorf("stored target_file_size_mb = %v, want 64", got)
		}
	})
}

func TestCompactTable_ReusesPipeline(t *testing.T) {
	env := newCompactEnv(t, compactEnvOpts{db: true})
	ctx := context.Background()
	first := env.compact(t, "raw", "events", "")
	if first.status != http.StatusAccepted {
		t.Fatalf("first: %d %v", first.status, first.body)
	}
	if _, err := store.UpdateRunPhase(ctx, env.pool, uuid.MustParse(first.body["run_id"].(string)), store.UpdateRunPhaseOpts{Phase: "Succeeded"}); err != nil {
		t.Fatalf("finish run: %v", err)
	}
	if second := env.compact(t, "raw", "events", ""); second.status != http.StatusAccepted {
		t.Fatalf("second: %d %v", second.status, second.body)
	}
	var pipelines, runs int
	name := SystemCompactPipelineName("raw", "events")
	_ = env.pool.QueryRow(ctx, `SELECT count(*) FROM pipelines WHERE name = $1`, name).Scan(&pipelines)
	_ = env.pool.QueryRow(ctx, `SELECT count(*) FROM runs r JOIN pipelines p ON p.id = r.pipeline_id WHERE p.name = $1`, name).Scan(&runs)
	if pipelines != 1 || runs != 2 {
		t.Errorf("pipelines = %d, runs = %d; want 1 and 2", pipelines, runs)
	}
}

func TestCompactTable_ConcurrentRequests(t *testing.T) {
	env := newCompactEnv(t, compactEnvOpts{db: true})
	env.backend.entered = make(chan struct{}, 1)
	env.backend.release = make(chan struct{})

	winner := make(chan compactResp, 1)
	go func() { winner <- env.compact(t, "raw", "events", `{"target_file_size_mb":64}`) }()
	select {
	case <-env.backend.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("first request never reached TriggerRun")
	}
	loser := env.compact(t, "raw", "events", `{"target_file_size_mb":256}`)
	close(env.backend.release)
	w := <-winner

	if w.status != http.StatusAccepted || loser.status != http.StatusConflict || loser.body["error"] != "compaction_running" {
		t.Fatalf("winner %d %v, loser %d %v; want 202 and 409", w.status, w.body, loser.status, loser.body)
	}
	if _, ok := loser.body["run_id"]; ok {
		t.Errorf("loser was admitted past the lock: %v", loser.body)
	}
	if got := storedTargetSize(t, env.pool, env.projectID, SystemCompactPipelineName("raw", "events")); got != float64(64) {
		t.Errorf("stored target_file_size_mb = %v, want the winner's 64", got)
	}
}

func TestCompactTable_SmallPoolNoStall(t *testing.T) {
	env := newCompactEnv(t, compactEnvOpts{db: true, maxConns: 2})
	env.backend.release = make(chan struct{})

	const n = 10
	results := make(chan compactResp, n)
	for i := 0; i < n; i++ {
		go func() { results <- env.compact(t, "raw", "events", "") }()
	}
	deadline := time.After(5 * time.Second)
	var got []compactResp
	for len(got) < n-1 {
		select {
		case r := <-results:
			got = append(got, r)
		case <-deadline:
			close(env.backend.release)
			t.Fatalf("only %d of %d losers returned within 5s while the winner was held", len(got), n-1)
		}
	}
	close(env.backend.release)
	select {
	case r := <-results:
		got = append(got, r)
	case <-time.After(5 * time.Second):
		t.Fatal("winner did not return after release")
	}
	var accepted, conflicts int
	for _, r := range got {
		switch r.status {
		case http.StatusAccepted:
			accepted++
		case http.StatusConflict:
			conflicts++
		}
	}
	if accepted != 1 || conflicts != n-1 {
		t.Errorf("accepted = %d, conflicts = %d; want 1 and %d", accepted, conflicts, n-1)
	}
}

func TestCompactTable_LockReleasedOnError(t *testing.T) {
	env := newCompactEnv(t, compactEnvOpts{db: true})
	env.backend.setErr(errors.New("apply pipeline CRD: boom"))
	if r := env.compact(t, "raw", "events", ""); r.status < 500 {
		t.Fatalf("status = %d, want 5xx", r.status)
	}
	env.backend.setErr(nil)
	if r := env.compact(t, "raw", "events", ""); r.status != http.StatusAccepted {
		t.Fatalf("after error: %d %v, want 202", r.status, r.body)
	}
}
