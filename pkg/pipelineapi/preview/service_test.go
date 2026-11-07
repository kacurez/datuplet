package preview

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/datuplet/datuplet/pkg/catalogwriter"
	datupletv1 "github.com/datuplet/datuplet/pkg/k8s/api/v1"
	"github.com/datuplet/datuplet/pkg/pipelineapi/auth"
	"github.com/datuplet/datuplet/pkg/pipelineapi/authz"
	pipelineapidb "github.com/datuplet/datuplet/pkg/pipelineapi/db"
	pkg8s "github.com/datuplet/datuplet/pkg/pipelineapi/k8s"
	"github.com/datuplet/datuplet/pkg/pipelineapi/store"
	"github.com/datuplet/datuplet/pkg/pipelineapi/tokens"
)

const (
	testK8sNS     = "datuplet-ensured-ns"
	testLK        = "lk-project-1"
	testWarehouse = "wh"
)

// recorder is the shared, ordered event log of the fake authorizer and catalog.
type recorder struct {
	mu     sync.Mutex
	events []string
}

func (r *recorder) add(e string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, e)
}

func (r *recorder) list() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.events...)
}

func (r *recorder) reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = nil
}

func indexOf(events []string, pred func(string) bool, last bool) int {
	idx := -1
	for i, e := range events {
		if pred(e) {
			idx = i
			if !last {
				return idx
			}
		}
	}
	return idx
}

type fakeAuthz struct {
	rec       *recorder
	mu        sync.Mutex
	tuples    map[authz.Tuple]bool
	deleteErr func(user string) error
}

func (f *fakeAuthz) Check(context.Context, string, string, authz.Object) (bool, error) {
	return false, nil
}

func (f *fakeAuthz) BatchCheck(_ context.Context, q []authz.CheckQuery) ([]bool, []error) {
	return make([]bool, len(q)), make([]error, len(q))
}

func (f *fakeAuthz) ListObjects(context.Context, string, string, authz.ObjectType) ([]authz.Object, error) {
	return nil, nil
}

func (f *fakeAuthz) WriteTuples(_ context.Context, ts []authz.Tuple) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, t := range ts {
		f.rec.add("write " + t.User)
		f.tuples[t] = true
	}
	return nil
}

func (f *fakeAuthz) DeleteTuples(_ context.Context, ts []authz.Tuple) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, t := range ts {
		if f.deleteErr != nil {
			if err := f.deleteErr(t.User); err != nil {
				return err
			}
		}
		if !f.tuples[t] {
			return errors.New("cannot delete a tuple which does not exist")
		}
		f.rec.add("delete " + t.User)
		delete(f.tuples, t)
	}
	return nil
}

func (f *fakeAuthz) has(subject string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.tuples[authz.Tuple{
		User: authz.UserObject(subject).String(), Relation: authz.RelationEditor, Object: authz.ProjectObject(testLK),
	}]
}

func (f *fakeAuthz) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.tuples)
}

type fakeCatalog struct {
	rec      *recorder
	mu       sync.Mutex
	tables   map[string]map[string]bool
	purgeErr func(ns, table string) error
	listHook func(ctx context.Context) error
}

func (c *fakeCatalog) seed(ns string, tables ...string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.tables[ns] == nil {
		c.tables[ns] = map[string]bool{}
	}
	for _, t := range tables {
		c.tables[ns][t] = true
	}
}

func (c *fakeCatalog) total() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, ts := range c.tables {
		n += len(ts)
	}
	return n
}

func (c *fakeCatalog) ListTables(ctx context.Context, ns string) ([]string, error) {
	if c.listHook != nil {
		if err := c.listHook(ctx); err != nil {
			return nil, err
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.rec.add("list " + ns)
	var out []string
	for t := range c.tables[ns] {
		out = append(out, t)
	}
	sort.Strings(out)
	return out, nil
}

func (c *fakeCatalog) LoadTable(context.Context, string, string) (*TableInfo, error) {
	return nil, errors.New("not implemented")
}

func (c *fakeCatalog) PurgeTable(_ context.Context, ns, table string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.rec.add("purge " + ns + "." + table)
	if c.purgeErr != nil {
		if err := c.purgeErr(ns, table); err != nil {
			return err
		}
	}
	delete(c.tables[ns], table)
	return nil
}

func (c *fakeCatalog) DropNamespace(_ context.Context, ns string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.rec.add("drop " + ns)
	if len(c.tables[ns]) > 0 {
		return errors.New("namespace not empty")
	}
	delete(c.tables, ns)
	return nil
}

type fixedNS string

func (n fixedNS) Ensure(context.Context, uuid.UUID) (string, error) { return string(n), nil }

type env struct {
	t    *testing.T
	ctx  context.Context
	pool *pgxpool.Pool
	k8s  client.Client
	az   *fakeAuthz
	cat  *fakeCatalog
	rec  *recorder
	svc  *Service
	pid  uuid.UUID
	uid  uuid.UUID
}

func testSigner(t *testing.T) *tokens.Signer {
	t.Helper()
	dir := t.TempDir()
	s, err := tokens.EnsureSigningKey(filepath.Join(dir, "priv.pem"), filepath.Join(dir, "pub.pem"), "test")
	if err != nil {
		t.Fatalf("signer: %v", err)
	}
	return s
}

func testPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping DB-backed test")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pgx pool: %v", err)
	}
	t.Cleanup(pool.Close)
	if _, err := pool.Exec(ctx, "DROP SCHEMA public CASCADE; CREATE SCHEMA public;"); err != nil {
		t.Fatalf("reset: %v", err)
	}
	if err := pipelineapidb.Migrate(ctx, pool); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return pool
}

func newEnv(t *testing.T, caps store.PreviewCaps) *env {
	t.Helper()
	pool := testPool(t)
	ctx := context.Background()
	proj, err := store.CreateProject(ctx, pool, "proj")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.SetLakekeeperProjectID(ctx, pool, proj.ID, testLK); err != nil {
		t.Fatal(err)
	}
	user, err := store.CreateUser(ctx, pool, "alice@example.com", "h")
	if err != nil {
		t.Fatal(err)
	}
	rec := &recorder{}
	e := &env{
		t: t, ctx: auth.WithCtxUser(ctx, user), pool: pool, rec: rec, pid: proj.ID, uid: user.ID,
		k8s: fake.NewClientBuilder().WithScheme(pkg8s.Scheme()).Build(),
		az:  &fakeAuthz{rec: rec, tuples: map[authz.Tuple]bool{}},
		cat: &fakeCatalog{rec: rec, tables: map[string]map[string]bool{}},
	}
	e.svc = New(Deps{
		DB: pool, K8s: e.k8s, Authz: e.az, Signer: testSigner(t), ProjectNS: fixedNS(testK8sNS),
		WarehouseResolver: func(_ context.Context, lk string) (string, error) {
			if lk != testLK {
				return "", errors.New("unexpected lakekeeper project " + lk)
			}
			return testWarehouse, nil
		},
		Catalogs: func(ctx context.Context, tp catalogwriter.TokenProvider, lk, wh string) (Catalog, error) {
			tok, err := tp(ctx)
			if err != nil || tok == "" || lk != testLK || wh != testWarehouse {
				return nil, errors.New("bad catalog args")
			}
			rec.add("open")
			return e.cat, nil
		},
		Caps:      caps,
		PurgeWait: 100 * time.Millisecond, PollInterval: 10 * time.Millisecond,
	})
	return e
}

var roomyCaps = store.PreviewCaps{RunningPerWorkspace: 10, RunningPerProject: 10, MaxPerProject: 50}

func (e *env) create(ws string) *CreateResponse {
	e.t.Helper()
	resp, err := e.svc.Create(e.ctx, CreateRequest{ProjectID: e.pid, UserID: e.uid, Doc: twoStageDoc(), Workspace: ws})
	if err != nil {
		e.t.Fatalf("Create: %v", err)
	}
	return resp
}

func (e *env) get(id uuid.UUID) *store.Preview {
	e.t.Helper()
	p, err := store.GetPreview(context.Background(), e.pool, e.pid, id)
	if err != nil {
		e.t.Fatalf("GetPreview: %v", err)
	}
	return p
}

func (e *env) finish(id uuid.UUID, completed time.Time) {
	e.t.Helper()
	if _, err := store.UpdatePreviewPhase(context.Background(), e.pool, id, store.UpdatePreviewPhaseOpts{
		Phase: "Succeeded", CompletedAt: &completed,
	}); err != nil {
		e.t.Fatal(err)
	}
}

func (e *env) seedTables(resp *CreateResponse) {
	for _, ns := range resp.ScratchNamespaces {
		e.cat.seed(ns, "t1")
	}
}

func (e *env) addPod(id uuid.UUID) {
	e.t.Helper()
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: "pod-" + id.String()[:8], Namespace: testK8sNS,
		Labels: map[string]string{"datuplet.io/run-id": id.String()},
	}}
	if err := e.k8s.Create(context.Background(), pod); err != nil {
		e.t.Fatal(err)
	}
}

func (e *env) pipelineRunExists(id uuid.UUID) bool {
	err := e.k8s.Get(context.Background(), types.NamespacedName{
		Name: pkg8s.PreviewPipelineRunName(id), Namespace: testK8sNS,
	}, &datupletv1.PipelineRun{})
	return err == nil
}

func TestCreate_RejectsOutOfRange(t *testing.T) {
	r, err := withDefaults(CreateRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if r.RowLimit != 1000 || r.TimeoutSeconds != 300 || r.TTLSeconds != 3600 || r.Workspace != "default" {
		t.Errorf("defaults = %+v", r)
	}

	svc := New(Deps{})
	for name, req := range map[string]CreateRequest{
		"row_limit":   {RowLimit: 200000},
		"negative":    {RowLimit: -1},
		"timeout":     {TimeoutSeconds: 10},
		"ttl":         {TTLSeconds: 86401},
		"workspace":   {Workspace: "Bad_WS"},
		"until_stage": {UntilStage: "nope"},
	} {
		req.Doc = twoStageDoc()
		if _, err := svc.Create(context.Background(), req); !errors.Is(err, ErrBadRequest) {
			t.Errorf("%s: want ErrBadRequest, got %v", name, err)
		}
	}
}

func TestCreate_HappyPath(t *testing.T) {
	e := newEnv(t, roomyCaps)
	resp, err := e.svc.Create(e.ctx, CreateRequest{ProjectID: e.pid, UserID: e.uid, Doc: twoStageDoc(), Workspace: "etl", RowLimit: 50})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	id8 := resp.ID.String()[:8]
	wantNS := []string{"pv-" + id8 + "-curated", "pv-" + id8 + "-raw"}
	if resp.Phase != "Pending" || resp.Workspace != "etl" || strings.Join(resp.ScratchNamespaces, ",") != strings.Join(wantNS, ",") {
		t.Errorf("response = %+v", resp)
	}

	p := e.get(resp.ID)
	if p.Workspace != "etl" || strings.Join(p.ScratchNamespaces, ",") != strings.Join(wantNS, ",") ||
		p.Warehouse != testWarehouse || p.LakekeeperProjectID != testLK || p.K8sNamespace != testK8sNS ||
		p.RowLimit != 50 || p.Phase != "Pending" || p.TriggeredBy != e.uid {
		t.Errorf("row = %+v", p)
	}
	if !strings.Contains(string(p.Doc), `"orig"`) {
		t.Errorf("row doc should be the original doc, got %s", p.Doc)
	}
	if !p.ExpiresAt.Equal(resp.ExpiresAt) || resp.ExpiresAt.IsZero() {
		t.Errorf("expires_at row=%v resp=%v", p.ExpiresAt, resp.ExpiresAt)
	}

	if e.az.count() != 1 || !e.az.has(resp.ID.String()) {
		t.Errorf("want exactly the preview's editor tuple, got %v", e.az.tuples)
	}

	pr := &datupletv1.PipelineRun{}
	if err := e.k8s.Get(e.ctx, types.NamespacedName{Name: pkg8s.PreviewPipelineRunName(resp.ID), Namespace: testK8sNS}, pr); err != nil {
		t.Fatalf("PipelineRun: %v", err)
	}
	if pr.Spec.Preview == nil || pr.Spec.Preview.RowLimit != 50 || pr.Spec.Preview.TimeoutSeconds != 300 {
		t.Errorf("preview spec = %+v", pr.Spec.Preview)
	}
	if pr.Spec.PipelineSpec == nil || len(pr.Spec.PipelineSpec.Stages) != 2 ||
		pr.Spec.PipelineSpec.Stages[0].Components[0].Outputs.DefaultBucket != wantNS[1] {
		t.Errorf("inline spec not rewritten: %+v", pr.Spec.PipelineSpec)
	}
	sec := &corev1.Secret{}
	if err := e.k8s.Get(e.ctx, types.NamespacedName{Name: "runtoken-" + resp.ID.String(), Namespace: testK8sNS}, sec); err != nil {
		t.Fatalf("Secret: %v", err)
	}
	if len(sec.Data[pkg8s.RunTokenSecretKey]) == 0 {
		t.Error("run token missing")
	}
}

func TestCreate_RunningCapPerWorkspace(t *testing.T) {
	e := newEnv(t, store.PreviewCaps{RunningPerWorkspace: 2, RunningPerProject: 8, MaxPerProject: 50})
	e.create("a")
	e.create("a")
	_, err := e.svc.Create(e.ctx, CreateRequest{ProjectID: e.pid, Doc: twoStageDoc(), Workspace: "a"})
	if !errors.Is(err, store.ErrPreviewCap) {
		t.Fatalf("want ErrPreviewCap, got %v", err)
	}
	if e.az.count() != 2 {
		t.Errorf("tuples = %d, want 2", e.az.count())
	}
	prs := &datupletv1.PipelineRunList{}
	if err := e.k8s.List(e.ctx, prs, client.InNamespace(testK8sNS)); err != nil {
		t.Fatal(err)
	}
	if len(prs.Items) != 2 {
		t.Errorf("PipelineRuns = %d, want 2", len(prs.Items))
	}
}

func TestCancel_RevokesAndStops(t *testing.T) {
	e := newEnv(t, roomyCaps)
	resp := e.create("a")
	e.addPod(resp.ID)

	if err := e.svc.Cancel(e.ctx, e.pid, resp.ID); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	p := e.get(resp.ID)
	if p.Phase != "Cancelled" || p.Message != "cancelled by user" || p.CompletedAt == nil || p.FGATupleDeletedAt == nil {
		t.Errorf("row = %+v", p)
	}
	if want := p.CompletedAt.Add(time.Hour); p.ExpiresAt.Sub(want).Abs() > time.Second {
		t.Errorf("expires_at = %v, want ~%v", p.ExpiresAt, want)
	}
	if e.az.has(resp.ID.String()) {
		t.Error("preview tuple still present")
	}
	if e.pipelineRunExists(resp.ID) {
		t.Error("PipelineRun still present")
	}
	pod := &corev1.Pod{}
	if err := e.k8s.Get(e.ctx, types.NamespacedName{Name: "pod-" + resp.ID.String()[:8], Namespace: testK8sNS}, pod); err != nil {
		t.Fatal(err)
	}
	if pod.Annotations["datuplet.io/cancel"] != "true" {
		t.Errorf("pod annotations = %v", pod.Annotations)
	}
	if err := e.svc.Cancel(e.ctx, e.pid, uuid.New()); !errors.Is(err, store.ErrPreviewNotFound) {
		t.Errorf("unknown preview: %v", err)
	}
}

func TestDeleteWorkspace_OnlyThatWorkspace(t *testing.T) {
	e := newEnv(t, roomyCaps)
	a1, a2, b := e.create("a"), e.create("a"), e.create("b")
	for _, r := range []*CreateResponse{a1, a2, b} {
		e.seedTables(r)
	}

	deleted, failed := e.svc.DeleteWorkspace(e.ctx, e.pid, "a")
	if len(failed) != 0 || len(deleted) != 2 {
		t.Fatalf("deleted=%v failed=%v", deleted, failed)
	}
	for _, r := range []*CreateResponse{a1, a2} {
		p := e.get(r.ID)
		if p.CleanedAt == nil || p.Phase != "Cancelled" || p.FGATupleDeletedAt == nil {
			t.Errorf("workspace a preview not torn down: %+v", p)
		}
	}
	if p := e.get(b.ID); p.CleanedAt != nil || p.Phase != "Pending" {
		t.Errorf("workspace b preview touched: %+v", p)
	}
	if !e.pipelineRunExists(b.ID) || !e.az.has(b.ID.String()) {
		t.Error("workspace b PipelineRun or tuple removed")
	}
	if e.cat.total() != len(b.ScratchNamespaces) {
		t.Errorf("tables left = %d, want only workspace b's %d", e.cat.total(), len(b.ScratchNamespaces))
	}
}

func TestCreate_FailureAfterInsertMarksFailed(t *testing.T) {
	e := newEnv(t, roomyCaps)
	e.svc.d.K8s = fake.NewClientBuilder().WithScheme(pkg8s.Scheme()).WithInterceptorFuncs(interceptor.Funcs{
		Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			if _, ok := obj.(*datupletv1.PipelineRun); ok {
				return errors.New("apiserver down")
			}
			return c.Create(ctx, obj, opts...)
		},
	}).Build()

	_, err := e.svc.Create(e.ctx, CreateRequest{ProjectID: e.pid, Doc: twoStageDoc()})
	if err == nil || !strings.Contains(err.Error(), "apiserver down") {
		t.Fatalf("Create err = %v", err)
	}
	rows, _, err := e.svc.List(e.ctx, e.pid, "*", 10, "")
	if err != nil || len(rows) != 1 {
		t.Fatalf("rows = %d, err = %v", len(rows), err)
	}
	p := rows[0]
	if p.Phase != "FailedApplication" || !strings.Contains(p.Message, "apiserver down") ||
		p.CompletedAt == nil || p.FGATupleDeletedAt == nil {
		t.Errorf("row = phase %q message %q completed %v tuple_deleted %v", p.Phase, p.Message, p.CompletedAt, p.FGATupleDeletedAt)
	}
	if e.az.count() != 0 {
		t.Errorf("tuple left behind: %v", e.az.tuples)
	}
	sec := &corev1.Secret{}
	err = e.svc.d.K8s.Get(e.ctx, types.NamespacedName{Name: "runtoken-" + p.ID.String(), Namespace: testK8sNS}, sec)
	if !apierrors.IsNotFound(err) {
		t.Errorf("run-token Secret left behind after a failed create: %v", err)
	}
}
