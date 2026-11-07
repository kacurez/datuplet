package preview

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sfake "k8s.io/client-go/kubernetes/fake"

	"github.com/datuplet/datuplet/pkg/catalogwriter"
	datupletv1 "github.com/datuplet/datuplet/pkg/k8s/api/v1"
	"github.com/datuplet/datuplet/pkg/pipelineapi/store"
)

func TestColumnsHint(t *testing.T) {
	hint, warns := ColumnsHint([]Column{
		{Name: "id", Type: "long"}, {Name: "ts", Type: "timestamptz"}, {Name: "v", Type: "variant"},
	})
	want := []ColumnHint{{Name: "id", Type: "long"}, {Name: "ts", Type: "string"}, {Name: "v", Type: "variant"}}
	if !reflect.DeepEqual(hint, want) {
		t.Errorf("hint = %+v", hint)
	}
	if len(warns) != 1 || !strings.Contains(warns[0], `"ts"`) {
		t.Errorf("warnings = %q", warns)
	}
}

type resultCatalog struct {
	mu     sync.Mutex
	tables map[string]map[string]*TableInfo
	lists  map[string]int
	loads  int
	// gate, when set, blocks ListTables until closed or ctx is done;
	// entered is closed on the first blocked call.
	gate, entered chan struct{}
	enterOnce     sync.Once
}

func (c *resultCatalog) ListTables(ctx context.Context, ns string) ([]string, error) {
	time.Sleep(20 * time.Millisecond)
	if c.gate != nil {
		c.enterOnce.Do(func() { close(c.entered) })
		select {
		case <-c.gate:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lists[ns]++
	var out []string
	for t := range c.tables[ns] {
		out = append(out, t)
	}
	return out, nil
}

func (c *resultCatalog) LoadTable(_ context.Context, ns, table string) (*TableInfo, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.loads++
	if info := c.tables[ns][table]; info != nil {
		return info, nil
	}
	return nil, errors.New("no such table")
}

func (c *resultCatalog) PurgeTable(context.Context, string, string) error { return nil }
func (c *resultCatalog) DropNamespace(context.Context, string) error      { return nil }

func (c *resultCatalog) calls() (lists map[string]int, loads int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	lists = map[string]int{}
	for k, v := range c.lists {
		lists[k] = v
	}
	return lists, c.loads
}

type fakeSampler struct {
	mu    sync.Mutex
	err   error
	calls int
	args  []string
}

func (f *fakeSampler) Sample(_ context.Context, qw, ns, table string, variantCols []string, rows int) (*Sample, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.args = append(f.args, qw+" "+ns+"."+table+" "+strings.Join(variantCols, ","))
	if f.err != nil {
		return nil, f.err
	}
	return &Sample{Columns: []SampleColumn{{Name: "id", Type: "BIGINT"}}, Rows: [][]any{{float64(rows)}}}, nil
}

func (f *fakeSampler) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

type fakeTailer struct {
	mu    sync.Mutex
	calls []string
}

func (f *fakeTailer) Tail(_ context.Context, ns, job, container string, lines int64) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, ns+" "+job+" "+container)
	if lines != 200 {
		return "", errors.New("want 200 lines")
	}
	return container + " log of " + job, nil
}

type resultEnv struct {
	*env
	rcat    *resultCatalog
	sampler *fakeSampler
	tailer  *fakeTailer
}

func newResultEnv(t *testing.T) *resultEnv {
	t.Helper()
	e := newEnv(t, roomyCaps)
	r := &resultEnv{
		env:     e,
		rcat:    &resultCatalog{tables: map[string]map[string]*TableInfo{}, lists: map[string]int{}},
		sampler: &fakeSampler{},
		tailer:  &fakeTailer{},
	}
	e.svc.d.UserCatalogToken = func(context.Context) (string, error) { return "user-token", nil }
	e.svc.d.Catalogs = func(ctx context.Context, tp catalogwriter.TokenProvider, lk, wh string) (Catalog, error) {
		if tok, err := tp(ctx); err != nil || tok != "user-token" || lk != testLK || wh != testWarehouse {
			return nil, errors.New("bad catalog args")
		}
		return r.rcat, nil
	}
	e.svc.d.Sampler = r.sampler
	e.svc.d.Logs = r.tailer
	return r
}

func (r *resultEnv) seed(ns, table string, info *TableInfo) {
	if r.rcat.tables[ns] == nil {
		r.rcat.tables[ns] = map[string]*TableInfo{}
	}
	r.rcat.tables[ns][table] = info
}

// insert records the twoStageDoc preview under testID and moves it to phase.
func (r *resultEnv) insert(phase string, stages []datupletv1.StageStatus) {
	r.t.Helper()
	ctx := context.Background()
	doc := twoStageDoc()
	rw, err := Rewrite(doc, testID, "")
	if err != nil {
		r.t.Fatal(err)
	}
	raw, _ := json.Marshal(doc)
	p := &store.Preview{
		ID: testID, ProjectID: r.pid, TriggeredBy: r.uid, Workspace: "etl", Doc: raw,
		RowLimit: 1000, TimeoutSeconds: 300, TTLSeconds: 3600, ScratchNamespaces: rw.Namespaces,
		LakekeeperProjectID: testLK, Warehouse: testWarehouse, K8sNamespace: testK8sNS,
	}
	if err := store.InsertPreview(ctx, r.pool, p, roomyCaps); err != nil {
		r.t.Fatal(err)
	}
	ss, _ := json.Marshal(stages)
	o := store.UpdatePreviewPhaseOpts{Phase: phase, StageStatuses: ss}
	if isTerminal(phase) {
		now := time.Now()
		o.CompletedAt = &now
	}
	if _, err := store.UpdatePreviewPhase(ctx, r.pool, testID, o); err != nil {
		r.t.Fatal(err)
	}
}

func (r *resultEnv) mustGet(resample bool) *Result {
	r.t.Helper()
	res, err := r.svc.Get(r.ctx, r.pid, testID, resample)
	if err != nil {
		r.t.Fatalf("Get: %v", err)
	}
	return res
}

func succeededStages() []datupletv1.StageStatus {
	start := metav1.NewTime(time.Unix(1000, 0))
	end := metav1.NewTime(time.Unix(1008, 0)) // metav1.Time serializes at second precision
	zero := int32(0)
	return []datupletv1.StageStatus{
		{Name: "extract", Phase: "Succeeded", StartTime: &start, CompletionTime: &end,
			ComponentStatuses: []datupletv1.ComponentStatus{{Name: "gen", Phase: "Succeeded", ExitCode: &zero, PodName: "job-extract-gen"}}},
		{Name: "transform", Phase: "Succeeded",
			ComponentStatuses: []datupletv1.ComponentStatus{{Name: "sql", Phase: "Succeeded", ExitCode: &zero, PodName: "job-transform-sql"}}},
	}
}

func TestGet_AssemblesOnceAndCaches(t *testing.T) {
	r := newResultEnv(t)
	r.seed("pv-ab12cd34-raw", "data", &TableInfo{
		Columns: []Column{{Name: "id", Type: "long", Nullable: true}, {Name: "v", Type: "variant", Nullable: true}}, TotalRecords: 1000, VariantColumns: []string{"v"},
	})
	r.seed("pv-ab12cd34-curated", "s", &TableInfo{Columns: []Column{{Name: "n", Type: "int"}}, TotalRecords: 3})
	r.insert("Succeeded", succeededStages())

	res := r.mustGet(false)
	if !res.ResultReady || res.Phase != "Succeeded" || res.DocName != "orig" || res.Workspace != "etl" || res.RowLimit != 1000 || res.UntilStage != nil {
		t.Errorf("header = %+v", res)
	}
	if len(res.Stages) != 2 || res.Stages[0].DurationMS == nil || *res.Stages[0].DurationMS != 8000 ||
		res.Stages[0].Components[0].Name != "gen" || res.Stages[0].Components[0].LogTail != nil {
		t.Errorf("stages = %+v", res.Stages)
	}
	if len(res.Outputs) != 2 {
		t.Fatalf("outputs = %+v", res.Outputs)
	}
	var raw, cur *Output
	for i := range res.Outputs {
		switch res.Outputs[i].Bucket {
		case "raw":
			raw = &res.Outputs[i]
		case "curated":
			cur = &res.Outputs[i]
		}
	}
	if raw == nil || cur == nil {
		t.Fatalf("outputs = %+v", res.Outputs)
	}
	if raw.Table != "data" || raw.ScratchTable != "pv-ab12cd34-raw.data" || raw.Rows != 1000 || !raw.AtCap ||
		raw.Stage == nil || *raw.Stage != "extract" || raw.Component == nil || *raw.Component != "gen" || raw.Sample == nil {
		t.Errorf("raw output = %+v", raw)
	}
	if cur.AtCap || cur.Rows != 3 || cur.Stage == nil || *cur.Stage != "transform" || cur.Component == nil || *cur.Component != "sql" {
		t.Errorf("curated output = %+v", cur)
	}
	if !reflect.DeepEqual(raw.ColumnsHint, []ColumnHint{{Name: "id", Type: "long"}, {Name: "v", Type: "variant"}}) {
		t.Errorf("columns_hint = %+v", raw.ColumnsHint)
	}
	if len(res.Warnings) != 0 {
		t.Errorf("warnings = %q", res.Warnings)
	}
	wantArg := testLK + "/" + testWarehouse + " pv-ab12cd34-raw.data v"
	if r.sampler.count() != 2 || !contains(r.sampler.args, wantArg) {
		t.Errorf("sampler args = %q", r.sampler.args)
	}

	js, _ := json.Marshal(res)
	for _, k := range []string{`"result_ready":true`, `"scratch_table":"pv-ab12cd34-raw.data"`, `"at_cap":true`,
		`"columns_hint":[`, `"schema":[{"name":"id","type":"long","nullable":true}`, `"until_stage":null`,
		`"duration_ms":8000`, `"log_tail":null`, `"gateway_log_tail":null`, `"exit_code":0`, `"doc_name":"orig"`,
		`"sample":{"columns":[{"name":"id","type":"BIGINT"}]`, `"warnings":[]`} {
		if !strings.Contains(string(js), k) {
			t.Errorf("JSON lacks %s: %s", k, js)
		}
	}

	lists, loads := r.rcat.calls()
	res2 := r.mustGet(false)
	lists2, loads2 := r.rcat.calls()
	if !reflect.DeepEqual(lists, lists2) || loads != loads2 || r.sampler.count() != 2 {
		t.Errorf("second Get hit the catalog or sampler: lists %v→%v loads %d→%d samples %d", lists, lists2, loads, loads2, r.sampler.count())
	}
	js2, _ := json.Marshal(res2)
	if string(js) != string(js2) {
		t.Errorf("cached document differs:\n%s\n%s", js, js2)
	}
}

func contains(xs []string, x string) bool {
	for _, s := range xs {
		if s == x {
			return true
		}
	}
	return false
}

func TestGet_SamplerUnavailable_Warns(t *testing.T) {
	t.Run("sampler error", func(t *testing.T) {
		r := newResultEnv(t)
		r.sampler.err = errors.New("query_disabled")
		r.seed("pv-ab12cd34-raw", "data", &TableInfo{Columns: []Column{{Name: "id", Type: "long"}}, TotalRecords: 5})
		r.insert("Succeeded", succeededStages())
		res := r.mustGet(false)
		if len(res.Outputs) != 1 || res.Outputs[0].Sample != nil {
			t.Fatalf("outputs = %+v", res.Outputs)
		}
		if len(res.Warnings) != 1 || res.Warnings[0] != "sample skipped for pv-ab12cd34-raw.data: query_disabled" {
			t.Errorf("warnings = %q", res.Warnings)
		}
	})
	t.Run("no sampler", func(t *testing.T) {
		r := newResultEnv(t)
		r.svc.d.Sampler = nil
		r.seed("pv-ab12cd34-raw", "data", &TableInfo{Columns: []Column{{Name: "id", Type: "long"}}, TotalRecords: 5})
		r.insert("Succeeded", succeededStages())
		res := r.mustGet(false)
		if len(res.Outputs) != 1 || res.Outputs[0].Sample != nil {
			t.Fatalf("outputs = %+v", res.Outputs)
		}
		if len(res.Warnings) != 1 || res.Warnings[0] != "samples unavailable: query service disabled" {
			t.Errorf("warnings = %q", res.Warnings)
		}
	})
}

func TestGet_ResampleFillsMissingSamples(t *testing.T) {
	r := newResultEnv(t)
	r.sampler.err = errors.New("rate_limited")
	r.seed("pv-ab12cd34-raw", "data", &TableInfo{
		Columns: []Column{{Name: "id", Type: "long"}, {Name: "v", Type: "variant"}}, TotalRecords: 5, VariantColumns: []string{"v"},
	})
	r.seed("pv-ab12cd34-raw", "ts", &TableInfo{Columns: []Column{{Name: "at", Type: "timestamptz"}}, TotalRecords: 5})
	r.insert("Succeeded", succeededStages())
	res := r.mustGet(false)
	if len(res.Warnings) != 3 {
		t.Fatalf("warnings = %q", res.Warnings)
	}

	r.sampler.err = nil
	_, loads := r.rcat.calls()
	res = r.mustGet(true)
	for _, o := range res.Outputs {
		if o.Sample == nil {
			t.Errorf("resample left %s without a sample", o.ScratchTable)
		}
	}
	if len(res.Warnings) != 1 || !strings.Contains(res.Warnings[0], `"at"`) {
		t.Errorf("warnings after resample = %q", res.Warnings)
	}
	if _, loads2 := r.rcat.calls(); loads2 != loads {
		t.Errorf("resample reloaded tables: %d→%d", loads, loads2)
	}
	if !contains(r.sampler.args, testLK+"/"+testWarehouse+" pv-ab12cd34-raw.data v") {
		t.Errorf("resample lost variant columns: %q", r.sampler.args)
	}

	n := r.sampler.count()
	res = r.mustGet(false)
	if res.Outputs[0].Sample == nil || res.Outputs[1].Sample == nil || r.sampler.count() != n {
		t.Errorf("resampled document not cached: %+v (samples %d→%d)", res.Outputs, n, r.sampler.count())
	}
	if r.mustGet(true); r.sampler.count() != n {
		t.Errorf("resample with every sample present sampled again")
	}
}

func TestGet_FailedComponentLogTails(t *testing.T) {
	r := newResultEnv(t)
	one := int32(1)
	r.insert("FailedUser", []datupletv1.StageStatus{{
		Name: "extract", Phase: "FailedUser", Message: "component failed",
		ComponentStatuses: []datupletv1.ComponentStatus{{Name: "gen", Phase: "FailedUser", ExitCode: &one, PodName: "job-extract-gen", Message: "bad config"}},
	}})
	res := r.mustGet(false)
	if !res.ResultReady || res.Phase != "FailedUser" || len(res.Stages) != 1 {
		t.Fatalf("result = %+v", res)
	}
	c := res.Stages[0].Components[0]
	if c.LogTail == nil || *c.LogTail != "component log of job-extract-gen" ||
		c.GatewayLogTail == nil || *c.GatewayLogTail != "gateway log of job-extract-gen" ||
		c.ExitCode == nil || *c.ExitCode != 1 || c.Message != "bad config" {
		t.Errorf("component = %+v", c)
	}
	want := []string{testK8sNS + " job-extract-gen component", testK8sNS + " job-extract-gen gateway"}
	if !reflect.DeepEqual(r.tailer.calls, want) {
		t.Errorf("tail calls = %q", r.tailer.calls)
	}
	if len(res.Outputs) != 0 || res.Outputs == nil {
		t.Errorf("outputs = %#v", res.Outputs)
	}
}

func TestGet_RunningHasNoOutputs(t *testing.T) {
	r := newResultEnv(t)
	r.seed("pv-ab12cd34-raw", "data", &TableInfo{TotalRecords: 1})
	r.insert("Running", []datupletv1.StageStatus{{Name: "extract", Phase: "Running",
		ComponentStatuses: []datupletv1.ComponentStatus{{Name: "gen", Phase: "FailedUser", PodName: "job-extract-gen"}}}})
	res := r.mustGet(false)
	if res.ResultReady || res.Phase != "Running" || len(res.Stages) != 1 || res.Stages[0].Components[0].LogTail != nil {
		t.Errorf("result = %+v", res)
	}
	js, _ := json.Marshal(res)
	if !strings.Contains(string(js), `"outputs":[]`) || !strings.Contains(string(js), `"result_ready":false`) {
		t.Errorf("JSON = %s", js)
	}
	if lists, loads := r.rcat.calls(); len(lists) != 0 || loads != 0 || len(r.tailer.calls) != 0 {
		t.Errorf("running preview touched catalog/logs: %v %d %q", lists, loads, r.tailer.calls)
	}
	if p := r.get(testID); p.Result != nil {
		t.Errorf("running preview cached a result: %s", p.Result)
	}
}

func TestGet_SucceededNoTables_ResultReady(t *testing.T) {
	r := newResultEnv(t)
	r.insert("Succeeded", succeededStages())
	res := r.mustGet(false)
	if !res.ResultReady || res.Outputs == nil || len(res.Outputs) != 0 {
		t.Errorf("result = %+v", res)
	}
	if p := r.get(testID); p.Result == nil {
		t.Error("result not cached")
	}
}

func TestGet_ConcurrentAssembleOnce(t *testing.T) {
	r := newResultEnv(t)
	r.seed("pv-ab12cd34-raw", "data", &TableInfo{Columns: []Column{{Name: "id", Type: "long"}}, TotalRecords: 1})
	r.insert("Succeeded", succeededStages())
	var wg sync.WaitGroup
	errs := make(chan error, 5)
	for range 5 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := r.svc.Get(r.ctx, r.pid, testID, false)
			if err == nil && (!res.ResultReady || len(res.Outputs) != 1) {
				err = errors.New("incomplete result")
			}
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Error(err)
		}
	}
	lists, _ := r.rcat.calls()
	for _, ns := range []string{"pv-ab12cd34-raw", "pv-ab12cd34-curated"} {
		if lists[ns] != 1 {
			t.Errorf("ListTables(%s) called %d times", ns, lists[ns])
		}
	}
}

func TestGet_CleanedBeforeAssembly(t *testing.T) {
	r := newResultEnv(t)
	r.seed("pv-ab12cd34-raw", "data", &TableInfo{TotalRecords: 1})
	r.insert("Succeeded", succeededStages())
	if err := store.MarkPreviewCleaned(context.Background(), r.pool, testID, time.Now(), false); err != nil {
		t.Fatal(err)
	}
	res := r.mustGet(false)
	if !res.ResultReady || res.Outputs == nil || len(res.Outputs) != 0 || len(res.Stages) != 2 {
		t.Errorf("result = %+v", res)
	}
	if len(res.Warnings) != 1 || res.Warnings[0] != "outputs purged before the result was assembled" {
		t.Errorf("warnings = %q", res.Warnings)
	}
	if lists, loads := r.rcat.calls(); len(lists) != 0 || loads != 0 || r.sampler.count() != 0 {
		t.Errorf("cleaned preview touched catalog/sampler: %v %d %d", lists, loads, r.sampler.count())
	}
	if p := r.get(testID); p.Result == nil {
		t.Error("result not cached")
	}
}

func TestGet_FirstCallerCancelledStillAssembles(t *testing.T) {
	r := newResultEnv(t)
	r.seed("pv-ab12cd34-raw", "data", &TableInfo{Columns: []Column{{Name: "id", Type: "long"}}, TotalRecords: 1})
	r.insert("Succeeded", succeededStages())
	r.rcat.gate, r.rcat.entered = make(chan struct{}), make(chan struct{})

	ctx1, cancel := context.WithCancel(r.ctx)
	defer cancel()
	go func() { _, _ = r.svc.Get(ctx1, r.pid, testID, false) }()
	<-r.rcat.entered

	second := make(chan error, 1)
	go func() {
		res, err := r.svc.Get(r.ctx, r.pid, testID, false)
		if err == nil && (!res.ResultReady || len(res.Outputs) != 1) {
			err = errors.New("incomplete result")
		}
		second <- err
	}()
	time.Sleep(50 * time.Millisecond)
	cancel()
	time.Sleep(50 * time.Millisecond)
	close(r.rcat.gate)

	if err := <-second; err != nil {
		t.Fatalf("second Get: %v", err)
	}
	if lists, _ := r.rcat.calls(); lists["pv-ab12cd34-raw"] != 1 {
		t.Errorf("ListTables(raw) called %d times; the cancelled flight was redone", lists["pv-ab12cd34-raw"])
	}
	if p := r.get(testID); p.Result == nil {
		t.Error("result not cached")
	}
}

func TestGet_NotFoundInOtherProject(t *testing.T) {
	r := newResultEnv(t)
	r.insert("Succeeded", succeededStages())
	if _, err := r.svc.Get(r.ctx, uuid.New(), testID, false); !errors.Is(err, store.ErrPreviewNotFound) {
		t.Errorf("want ErrPreviewNotFound, got %v", err)
	}
}

func TestClientsetLogTailer(t *testing.T) {
	ctx := context.Background()
	pod := func(name string, created time.Time) *corev1.Pod {
		return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: "ns", CreationTimestamp: metav1.NewTime(created),
			Labels: map[string]string{"batch.kubernetes.io/job-name": "job-a"},
		}}
	}
	other := pod("job-b-1", time.Unix(300, 0))
	other.Labels = map[string]string{"batch.kubernetes.io/job-name": "job-b"}
	cs := k8sfake.NewClientset(pod("job-a-old", time.Unix(100, 0)), pod("job-a-new", time.Unix(200, 0)), other)
	tl := NewClientsetLogTailer(cs)
	got, err := tl.Tail(ctx, "ns", "job-a", "component", 200)
	if err != nil || got != "fake logs" {
		t.Errorf("Tail = %q, %v", got, err)
	}
	if got, err := tl.Tail(ctx, "ns", "job-missing", "component", 200); err != nil || got != "" {
		t.Errorf("missing job: %q, %v", got, err)
	}
}

func TestNewQuerySampler_NilCore(t *testing.T) {
	if s := NewQuerySampler(nil); s != nil {
		t.Errorf("want nil Sampler for a nil core, got %#v", s)
	}
}
