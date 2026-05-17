package pipeline

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/datuplet/datuplet/pkg/lib/orchestrator"
	"github.com/datuplet/datuplet/pkg/pipeline/config"
)

// fakeOrchestrator is a test double for orchestrator.Orchestrator. It records
// the sequence of calls made to it and lets tests inject per-call behavior.
//
// To customize results:
//   - ExecResult / ExecErr drive ExecuteComponent (one entry per expected call,
//     consumed in order; a single entry is reused for all calls).
//   - CommitErr drives ExecuteTableCommit (same consumption semantics).
type fakeOrchestrator struct {
	// Inputs
	ExecResults []*orchestrator.ExecutionResult
	ExecErrs    []error
	CommitErrs  []error

	// Recorded calls
	ExecCalls    []orchestrator.ComponentSpec
	CommitCalls  []orchestrator.TableCommitSpec
	CleanupCalls int

	// Counters to index into slices
	execIdx   int
	commitIdx int
}

func (f *fakeOrchestrator) ExecuteComponent(_ context.Context, spec orchestrator.ComponentSpec) (*orchestrator.ExecutionResult, error) {
	f.ExecCalls = append(f.ExecCalls, spec)
	idx := f.execIdx
	if idx >= len(f.ExecResults) && len(f.ExecResults) > 0 {
		idx = len(f.ExecResults) - 1
	}
	errIdx := f.execIdx
	if errIdx >= len(f.ExecErrs) && len(f.ExecErrs) > 0 {
		errIdx = len(f.ExecErrs) - 1
	}
	f.execIdx++

	var res *orchestrator.ExecutionResult
	if len(f.ExecResults) > 0 {
		res = f.ExecResults[idx]
	}
	var err error
	if len(f.ExecErrs) > 0 {
		err = f.ExecErrs[errIdx]
	}
	if res == nil && err == nil {
		res = &orchestrator.ExecutionResult{Success: true}
	}
	return res, err
}

func (f *fakeOrchestrator) ExecuteTableCommit(_ context.Context, spec orchestrator.TableCommitSpec) error {
	f.CommitCalls = append(f.CommitCalls, spec)
	idx := f.commitIdx
	if idx >= len(f.CommitErrs) && len(f.CommitErrs) > 0 {
		idx = len(f.CommitErrs) - 1
	}
	f.commitIdx++
	if len(f.CommitErrs) > 0 {
		return f.CommitErrs[idx]
	}
	return nil
}

func (f *fakeOrchestrator) ForceStop(_ context.Context, _ string) error {
	return nil
}

func (f *fakeOrchestrator) Cleanup(_ context.Context) error {
	f.CleanupCalls++
	return nil
}

// Ensure fakeOrchestrator satisfies the interface at compile time.
var _ orchestrator.Orchestrator = (*fakeOrchestrator)(nil)

// newTestPipeline builds a minimal pipeline with one stage/one component,
// writing to a single bucket in FULL_LOAD mode. Tests override fields as
// needed.
func newTestPipeline() *config.Pipeline {
	return &config.Pipeline{
		APIVersion: config.DefaultAPIVersion,
		Kind:       config.DefaultKind,
		Metadata:   config.Metadata{Name: "test"},
		Spec: config.Spec{
			Stages: []config.Stage{
				{
					Name: "stage1",
					Components: []config.Component{
						{
							Name:  "comp1",
							Image: "datuplet/comp1:latest",
							Outputs: &config.OutputSpec{
								DefaultBucket:    "raw",
								DefaultWriteMode: config.WriteModeFullLoad,
							},
						},
					},
				},
			},
		},
	}
}

func TestRun_HappyPath_SingleStage(t *testing.T) {
	f := &fakeOrchestrator{}
	c := New(f)
	c.config = newTestPipeline()

	if err := c.Run(context.Background()); err != nil {
		t.Fatalf("Run returned unexpected error: %v", err)
	}

	if len(f.ExecCalls) != 1 {
		t.Fatalf("expected 1 component call, got %d", len(f.ExecCalls))
	}
	if len(f.CommitCalls) != 1 {
		t.Fatalf("expected 1 commit call, got %d", len(f.CommitCalls))
	}
	if got := f.CommitCalls[0].Bucket; got != "raw" {
		t.Errorf("commit bucket=%q, want %q", got, "raw")
	}
	if got := f.CommitCalls[0].WriteMode; got != config.WriteModeFullLoad {
		t.Errorf("commit writeMode=%q, want %q", got, config.WriteModeFullLoad)
	}
	// RunID must be threaded through both component and commit specs.
	if f.ExecCalls[0].RunID == "" || f.ExecCalls[0].RunID != f.CommitCalls[0].RunID {
		t.Errorf("runID mismatch: exec=%q commit=%q", f.ExecCalls[0].RunID, f.CommitCalls[0].RunID)
	}
}

func TestRun_MultiStage_SequencedCommits(t *testing.T) {
	f := &fakeOrchestrator{}
	c := New(f)
	p := newTestPipeline()
	p.Spec.Stages = append(p.Spec.Stages, config.Stage{
		Name: "stage2",
		Components: []config.Component{{
			Name:  "comp2",
			Image: "datuplet/comp2:latest",
			Outputs: &config.OutputSpec{
				DefaultBucket:    "curated",
				DefaultWriteMode: config.WriteModeAppend,
			},
		}},
	})
	c.config = p

	if err := c.Run(context.Background()); err != nil {
		t.Fatalf("Run returned error: %v", err)
	}

	if len(f.ExecCalls) != 2 {
		t.Fatalf("expected 2 exec calls, got %d", len(f.ExecCalls))
	}
	if len(f.CommitCalls) != 2 {
		t.Fatalf("expected 2 commit calls, got %d", len(f.CommitCalls))
	}
	// Execution order: comp1 must run before comp2 (same ctx, sequential stages).
	if f.ExecCalls[0].Name != "comp1" || f.ExecCalls[1].Name != "comp2" {
		t.Errorf("exec order: %q, %q; want comp1, comp2",
			f.ExecCalls[0].Name, f.ExecCalls[1].Name)
	}
	// Commits run at end of each stage, so stage1 commit precedes stage2 exec.
	// Our fake doesn't interleave across call kinds, but we can check buckets map to the right stage.
	if f.CommitCalls[0].Bucket != "raw" {
		t.Errorf("stage1 commit bucket=%q, want raw", f.CommitCalls[0].Bucket)
	}
	if f.CommitCalls[1].Bucket != "curated" {
		t.Errorf("stage2 commit bucket=%q, want curated", f.CommitCalls[1].Bucket)
	}
	if f.CommitCalls[1].WriteMode != config.WriteModeAppend {
		t.Errorf("stage2 writeMode=%q, want APPEND", f.CommitCalls[1].WriteMode)
	}
}

func TestRun_ComponentExecError_SkipsCommit(t *testing.T) {
	f := &fakeOrchestrator{
		ExecErrs: []error{errors.New("docker daemon unreachable")},
	}
	c := New(f)
	c.config = newTestPipeline()

	err := c.Run(context.Background())
	if err == nil {
		t.Fatal("expected Run to fail, got nil")
	}
	if !strings.Contains(err.Error(), "docker daemon unreachable") {
		t.Errorf("error should wrap underlying cause: %v", err)
	}
	if len(f.CommitCalls) != 0 {
		t.Errorf("expected 0 commit calls on exec failure, got %d", len(f.CommitCalls))
	}
}

func TestRun_ComponentResultFailure_FormatsStatusMessage(t *testing.T) {
	tests := []struct {
		name      string
		result    *orchestrator.ExecutionResult
		wantSub   []string
		wantNoSub []string
	}{
		{
			name: "with status message (exit 20 = FailedApplication)",
			result: &orchestrator.ExecutionResult{
				Success:       false,
				ExitCode:      20,
				FailureType:   "FailedApplication",
				StatusMessage: "S3 connection timeout",
			},
			wantSub: []string{"exit 20", "FailedApplication", "S3 connection timeout"},
		},
		{
			name: "exit 1 = FailedUser",
			result: &orchestrator.ExecutionResult{
				Success:       false,
				ExitCode:      1,
				FailureType:   "FailedUser",
				StatusMessage: "invalid config key",
			},
			wantSub: []string{"exit 1", "FailedUser", "invalid config key"},
		},
		{
			name: "no status message falls back to Error field",
			result: &orchestrator.ExecutionResult{
				Success: false,
				Error:   "something broke",
			},
			wantSub:   []string{"something broke"},
			wantNoSub: []string{"exit "}, // formatter only includes exit info when StatusMessage is set
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &fakeOrchestrator{ExecResults: []*orchestrator.ExecutionResult{tt.result}}
			c := New(f)
			c.config = newTestPipeline()

			err := c.Run(context.Background())
			if err == nil {
				t.Fatal("expected Run to fail, got nil")
			}
			for _, sub := range tt.wantSub {
				if !strings.Contains(err.Error(), sub) {
					t.Errorf("error %q missing substring %q", err.Error(), sub)
				}
			}
			for _, sub := range tt.wantNoSub {
				if strings.Contains(err.Error(), sub) {
					t.Errorf("error %q unexpectedly contains %q", err.Error(), sub)
				}
			}
			if len(f.CommitCalls) != 0 {
				t.Errorf("commit must not run on component failure, got %d", len(f.CommitCalls))
			}
		})
	}
}

func TestRun_CommitError_Propagates(t *testing.T) {
	f := &fakeOrchestrator{
		CommitErrs: []error{errors.New("snapshot conflict")},
	}
	c := New(f)
	c.config = newTestPipeline()

	err := c.Run(context.Background())
	if err == nil {
		t.Fatal("expected commit failure to propagate")
	}
	if !strings.Contains(err.Error(), "snapshot conflict") {
		t.Errorf("error must wrap underlying cause, got: %v", err)
	}
	if len(f.ExecCalls) != 1 || len(f.CommitCalls) != 1 {
		t.Errorf("exec=%d commit=%d, want 1/1", len(f.ExecCalls), len(f.CommitCalls))
	}
}

func TestRun_MultiBucket_CommitDedupsAndPrefersAppend(t *testing.T) {
	// Two components in one stage, both writing to bucket "raw":
	//   - comp1 writes FULL_LOAD
	//   - comp2 writes APPEND
	// APPEND is the "more conservative" write mode per commitStage and should win.
	f := &fakeOrchestrator{}
	c := New(f)
	p := &config.Pipeline{
		APIVersion: config.DefaultAPIVersion,
		Kind:       config.DefaultKind,
		Metadata:   config.Metadata{Name: "test"},
		Spec: config.Spec{
			Stages: []config.Stage{{
				Name: "s",
				Components: []config.Component{
					{
						Name:  "comp1",
						Image: "img1",
						Outputs: &config.OutputSpec{
							DefaultBucket:    "raw",
							DefaultWriteMode: config.WriteModeFullLoad,
						},
					},
					{
						Name:  "comp2",
						Image: "img2",
						Outputs: &config.OutputSpec{
							DefaultBucket:    "raw",
							DefaultWriteMode: config.WriteModeAppend,
						},
					},
				},
			}},
		},
	}
	c.config = p

	if err := c.Run(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(f.CommitCalls) != 1 {
		t.Fatalf("expected 1 dedup'd commit, got %d", len(f.CommitCalls))
	}
	if f.CommitCalls[0].WriteMode != config.WriteModeAppend {
		t.Errorf("dedup'd writeMode=%q, want APPEND (more conservative wins)",
			f.CommitCalls[0].WriteMode)
	}
}

// --- Pure helper tests ------------------------------------------------------

func TestBuildOutputConfig(t *testing.T) {
	t.Run("nil spec returns empty", func(t *testing.T) {
		got := buildOutputConfig(nil)
		if !reflect.DeepEqual(got, orchestrator.BucketOutputConfig{}) {
			t.Errorf("nil OutputSpec should yield zero value, got %+v", got)
		}
	})

	t.Run("default bucket mode", func(t *testing.T) {
		got := buildOutputConfig(&config.OutputSpec{
			DefaultBucket:    "raw",
			DefaultWriteMode: "FULL_LOAD",
		})
		if got.DefaultBucket != "raw" || got.DefaultWriteMode != "FULL_LOAD" {
			t.Errorf("default bucket not set: %+v", got)
		}
	})

	t.Run("explicit buckets and tables with partition fields", func(t *testing.T) {
		got := buildOutputConfig(&config.OutputSpec{
			Buckets: []config.OutputBucketSpec{
				{Name: "raw", WriteMode: "APPEND"},
			},
			Tables: []config.OutputTableSpec{
				{Name: "events", Bucket: "curated", WriteMode: "FULL_LOAD",
					PartitionSpec: []config.PartitionFieldSpec{
						{SourceColumn: "ts", Transform: "day"},
					},
				},
			},
			Processors: []config.Processor{
				{Type: "drop", Columns: []string{"secret"}},
			},
		})
		if len(got.Buckets) != 1 || got.Buckets[0].Name != "raw" {
			t.Errorf("buckets not forwarded: %+v", got.Buckets)
		}
		if len(got.Tables) != 1 || got.Tables[0].Name != "events" {
			t.Errorf("tables not forwarded: %+v", got.Tables)
		}
		if len(got.Tables[0].PartitionFields) != 1 ||
			got.Tables[0].PartitionFields[0].SourceColumn != "ts" ||
			got.Tables[0].PartitionFields[0].Transform != "day" {
			t.Errorf("partition fields not forwarded: %+v", got.Tables[0].PartitionFields)
		}
		if len(got.Processors) != 1 || got.Processors[0].Type != "drop" {
			t.Errorf("processors not forwarded: %+v", got.Processors)
		}
	})
}

func TestBuildInputTables(t *testing.T) {
	snap := int64(77)
	in := &config.InputSpec{
		Tables: []config.InputTableSpec{
			{Bucket: "raw", Table: "orders"},
			{Bucket: "raw", Table: "users", As: "u"},
			{Bucket: "raw", Table: "events", SinceSnapshot: &snap},
			{Bucket: "raw", Table: "logs", Since: "30m"},
		},
	}
	got := buildInputTables(in)
	if len(got) != 4 {
		t.Fatalf("want 4 tables, got %d", len(got))
	}
	if got[1].As != "u" {
		t.Errorf("As field not forwarded, got %q", got[1].As)
	}
	if got[2].SinceSnapshot != 77 {
		t.Errorf("SinceSnapshot=%d, want 77", got[2].SinceSnapshot)
	}
	if got[3].SinceTimestampMs <= 0 {
		t.Errorf("SinceTimestampMs should be positive for Since=30m, got %d", got[3].SinceTimestampMs)
	}
}

func TestConvertProcessors(t *testing.T) {
	if convertProcessors(nil) != nil {
		t.Error("nil input should yield nil output")
	}
	got := convertProcessors([]config.Processor{
		{Type: "drop", Columns: []string{"a", "b"}},
	})
	if len(got) != 1 || got[0].Type != "drop" ||
		!reflect.DeepEqual(got[0].Columns, []string{"a", "b"}) {
		t.Errorf("unexpected conversion: %+v", got)
	}
}

func TestExtractVolumeMounts(t *testing.T) {
	tmp := t.TempDir()
	f := filepath.Join(tmp, "input.csv")
	if err := os.WriteFile(f, []byte("a,b\n1,2\n"), 0o644); err != nil {
		t.Fatalf("write temp file: %v", err)
	}

	t.Run("absolute existing path produces volume and rewrites to /data", func(t *testing.T) {
		cfg := map[string]any{"source": f}
		c := &Controller{}
		vols := c.extractVolumeMounts(cfg)
		if vols[tmp] != "/data" {
			t.Errorf("expected volume %q -> /data, got %v", tmp, vols)
		}
		if cfg["source"] != "/data/input.csv" {
			t.Errorf("source not rewritten: %v", cfg["source"])
		}
	})

	t.Run("non-existent path is skipped", func(t *testing.T) {
		cfg := map[string]any{"source": "/nonexistent/path.csv"}
		c := &Controller{}
		vols := c.extractVolumeMounts(cfg)
		if len(vols) != 0 {
			t.Errorf("expected no volumes, got %v", vols)
		}
		if cfg["source"] != "/nonexistent/path.csv" {
			t.Errorf("source should not be rewritten for missing file: %v", cfg["source"])
		}
	})

	t.Run("non-path config value is ignored", func(t *testing.T) {
		cfg := map[string]any{"source": "some-bucket-name"}
		c := &Controller{}
		vols := c.extractVolumeMounts(cfg)
		if len(vols) != 0 {
			t.Errorf("non-path value should produce no volumes, got %v", vols)
		}
	})
}

func TestLoadInfraConfigFromEnv(t *testing.T) {
	t.Run("filesystem mode clears S3 defaults", func(t *testing.T) {
		t.Setenv("DATUPLET_STORAGE_TYPE", "filesystem")
		t.Setenv("DATUPLET_STORAGE_ROOT", "/tmp/warehouse")
		cfg := loadInfraConfigFromEnv()
		if cfg.StorageType != "filesystem" {
			t.Errorf("StorageType=%q, want filesystem", cfg.StorageType)
		}
		if cfg.StorageRoot != "/tmp/warehouse" {
			t.Errorf("StorageRoot=%q, want /tmp/warehouse", cfg.StorageRoot)
		}
		if cfg.Endpoint != "" || cfg.Bucket != "" || cfg.AccessKey != "" || cfg.SecretKey != "" {
			t.Errorf("S3 defaults must be cleared in filesystem mode: %+v", cfg)
		}
	})

	t.Run("S3 mode applies defaults when env unset", func(t *testing.T) {
		t.Setenv("DATUPLET_STORAGE_TYPE", "")
		t.Setenv("DATUPLET_STORAGE_ENDPOINT", "")
		t.Setenv("DATUPLET_STORAGE_BUCKET", "")
		t.Setenv("DATUPLET_STORAGE_ACCESS_KEY", "")
		t.Setenv("DATUPLET_STORAGE_SECRET_KEY", "")
		t.Setenv("DATUPLET_STORAGE_REGION", "")
		cfg := loadInfraConfigFromEnv()
		if cfg.Endpoint != "localhost:9000" {
			t.Errorf("Endpoint default=%q, want localhost:9000", cfg.Endpoint)
		}
		if cfg.Bucket != "datuplet" {
			t.Errorf("Bucket default=%q, want datuplet", cfg.Bucket)
		}
		if cfg.AccessKey != "minioadmin" {
			t.Errorf("AccessKey default=%q, want minioadmin", cfg.AccessKey)
		}
	})

	t.Run("S3 mode uses env overrides", func(t *testing.T) {
		t.Setenv("DATUPLET_STORAGE_TYPE", "")
		t.Setenv("DATUPLET_STORAGE_ENDPOINT", "custom.example.com:443")
		t.Setenv("DATUPLET_STORAGE_BUCKET", "my-bucket")
		t.Setenv("DATUPLET_STORAGE_USE_SSL", "true")
		cfg := loadInfraConfigFromEnv()
		if cfg.Endpoint != "custom.example.com:443" {
			t.Errorf("Endpoint=%q, want custom.example.com:443", cfg.Endpoint)
		}
		if cfg.Bucket != "my-bucket" {
			t.Errorf("Bucket=%q, want my-bucket", cfg.Bucket)
		}
		if !cfg.UseSSL {
			t.Errorf("UseSSL should be true when env is 'true'")
		}
	})
}

// --- ProgressFn hook tests ---------------------------------------------------

// newProgressTestController builds a Controller wired to a successful
// fakeOrchestrator and a pipeline containing one component per named stage.
// Each stage writes to its own bucket in FULL_LOAD mode, so the stage
// sequence emitted by Run directly reflects the names passed in.
func newProgressTestController(_ *testing.T, stageNames ...string) *Controller {
	f := &fakeOrchestrator{}
	c := New(f)
	p := &config.Pipeline{
		APIVersion: config.DefaultAPIVersion,
		Kind:       config.DefaultKind,
		Metadata:   config.Metadata{Name: "progress-test"},
	}
	for _, name := range stageNames {
		p.Spec.Stages = append(p.Spec.Stages, config.Stage{
			Name: name,
			Components: []config.Component{{
				Name:  name + "-comp",
				Image: "img",
				Outputs: &config.OutputSpec{
					DefaultBucket:    name + "-bucket",
					DefaultWriteMode: config.WriteModeFullLoad,
				},
			}},
		})
	}
	c.config = p
	return c
}

func TestControllerEmitsProgressEventsInOrder(t *testing.T) {
	// Controller with a fake orchestrator that succeeds every stage should
	// emit (in order):
	//   Phase=Running (initial)
	//   Phase=Running, CurrentStage=s1
	//   Phase=Running, CurrentStage=s2
	//   Phase=Succeeded
	ctrl := newProgressTestController(t, "s1", "s2")
	var got []ProgressEvent
	ctrl.WithProgress(func(e ProgressEvent) {
		got = append(got, e)
	})
	if err := ctrl.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(got) < 4 {
		t.Fatalf("got %d events, want >=4: %#v", len(got), got)
	}
	if got[0].Phase != PhaseRunning || got[0].CurrentStage != "" {
		t.Errorf("got[0]=%+v, want Phase=Running CurrentStage=\"\"", got[0])
	}
	// Stage-entry events: Phase=Running with a non-empty CurrentStage, in order.
	var stages []string
	for _, e := range got {
		if e.CurrentStage != "" && e.Phase == PhaseRunning {
			stages = append(stages, e.CurrentStage)
		}
	}
	if len(stages) < 2 || stages[0] != "s1" || stages[1] != "s2" {
		t.Errorf("stages = %v, want [s1 s2]", stages)
	}
	last := got[len(got)-1]
	if last.Phase != PhaseSucceeded {
		t.Errorf("last.Phase=%q, want Succeeded", last.Phase)
	}
	if last.Message == "" {
		t.Errorf("terminal Succeeded event should carry a Message, got empty")
	}
}

func TestControllerProgressNilFnIsNoop(t *testing.T) {
	// Controller with no progress fn must not panic when emit is called
	// from Run. Uses a successful pipeline.
	ctrl := newProgressTestController(t, "s1")
	if err := ctrl.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
}

func TestControllerEmitsFailedApplicationOnComponentError(t *testing.T) {
	// Component error should produce a terminal FailedApplication event
	// that carries the underlying error message.
	f := &fakeOrchestrator{
		ExecErrs: []error{errors.New("boom")},
	}
	c := New(f)
	c.config = newTestPipeline()

	var got []ProgressEvent
	c.WithProgress(func(e ProgressEvent) { got = append(got, e) })

	if err := c.Run(context.Background()); err == nil {
		t.Fatal("expected Run to fail")
	}
	if len(got) == 0 {
		t.Fatalf("expected progress events, got none")
	}
	last := got[len(got)-1]
	if last.Phase != PhaseFailedApplication {
		t.Errorf("last.Phase=%q, want FailedApplication", last.Phase)
	}
	if !strings.Contains(last.Message, "boom") {
		t.Errorf("last.Message=%q, want to contain underlying error 'boom'", last.Message)
	}
	if last.CurrentStage != "stage1" {
		t.Errorf("last.CurrentStage=%q, want stage1", last.CurrentStage)
	}
}

func TestControllerEmitsFailedApplicationOnCommitError(t *testing.T) {
	f := &fakeOrchestrator{
		CommitErrs: []error{errors.New("snapshot conflict")},
	}
	c := New(f)
	c.config = newTestPipeline()

	var got []ProgressEvent
	c.WithProgress(func(e ProgressEvent) { got = append(got, e) })

	if err := c.Run(context.Background()); err == nil {
		t.Fatal("expected Run to fail")
	}
	last := got[len(got)-1]
	if last.Phase != PhaseFailedApplication {
		t.Errorf("last.Phase=%q, want FailedApplication", last.Phase)
	}
	if !strings.Contains(last.Message, "snapshot conflict") {
		t.Errorf("last.Message=%q should contain 'snapshot conflict'", last.Message)
	}
}

func TestTranslateEndpointForContainer(t *testing.T) {
	c := &Controller{}
	cases := map[string]string{
		"localhost:9000":    "host.docker.internal:9000",
		"127.0.0.1:8080":    "host.docker.internal:8080",
		"s3.amazonaws.com":  "s3.amazonaws.com",
		"":                  "",
	}
	// Iterate deterministically for easier failure output.
	keys := make([]string, 0, len(cases))
	for k := range cases {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, in := range keys {
		want := cases[in]
		if got := c.translateEndpointForContainer(in); got != want {
			t.Errorf("translateEndpointForContainer(%q) = %q, want %q", in, got, want)
		}
	}
}
