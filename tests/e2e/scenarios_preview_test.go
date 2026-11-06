// Package e2e — pipeline preview mode agent loop.
//
// Drives the real `datuplet preview` CLI against a live cluster: a capped
// two-stage preview of preview-etl.yaml, its scratch tables (listed and
// queryable, never the doc's real buckets), --until-stage, workspace
// isolation, a user-error preview, bulk delete, and the TTL sweeper.
package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/datuplet/datuplet/tests/e2e/framework"
)

type previewDoc struct {
	ID          string `json:"id"`
	Phase       string `json:"phase"`
	ResultReady bool   `json:"result_ready"`
	Stages      []struct {
		Name       string `json:"name"`
		Components []struct {
			Name    string  `json:"name"`
			LogTail *string `json:"log_tail"`
		} `json:"components"`
	} `json:"stages"`
	Outputs []struct {
		Bucket       string            `json:"bucket"`
		Table        string            `json:"table"`
		ScratchTable string            `json:"scratch_table"`
		Rows         int64             `json:"rows"`
		AtCap        bool              `json:"at_cap"`
		Schema       []json.RawMessage `json:"schema"`
		ColumnsHint  []json.RawMessage `json:"columns_hint"`
		Sample       *struct {
			Rows [][]any `json:"rows"`
		} `json:"sample"`
	} `json:"outputs"`
}

func TestPreview_AgentLoop(t *testing.T) {
	if os.Getenv("E2E_K8S") != "1" {
		t.Skip("E2E_K8S=1 required")
	}
	h := framework.SharedHarness()
	if h == nil {
		framework.SkipOrFail(t, "SharedHarness nil — E2E_K8S=1 + bootstrap (incl. component registration) must have run in TestMain")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Minute)
	defer cancel()

	root := repoRoot(t)
	bin := filepath.Join(t.TempDir(), "datuplet")
	build := exec.CommandContext(ctx, "go", "build", "-o", bin, "./cmd/datuplet")
	build.Dir = root
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build datuplet CLI: %v\n%s", err, string(out))
	}
	pid, err := framework.ResolveDatupletProjectID(ctx, h)
	if err != nil {
		t.Fatalf("resolve Datuplet project id: %v", err)
	}
	token, err := framework.MintAdminCLIToken(ctx, h, time.Hour)
	if err != nil {
		t.Fatalf("mint admin cli-api token: %v", err)
	}
	env := append(os.Environ(),
		"DATUPLET_REMOTE="+framework.PipelineAPIBaseURL(),
		"DATUPLET_API_TOKEN="+token,
		"DATUPLET_PROJECT="+pid,
	)
	const wsA, wsB, wsTTL = "e2e-a", "e2e-b", "e2e-ttl"
	t.Cleanup(func() {
		cctx, ccancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer ccancel()
		for _, ws := range []string{wsA, wsTTL} {
			runCLI(cctx, t, bin, env, "preview", "delete", "--workspace", ws, "--all")
		}
	})

	docPath, err := framework.RenderPipeline("pipelines/k8s/preview-etl.yaml", framework.TemplateVars{RunPrefix: runPrefix})
	if err != nil {
		t.Fatalf("render preview-etl.yaml: %v", err)
	}
	t.Cleanup(func() { os.Remove(docPath) })
	rawBucket, etlBucket := runPrefix+"-raw", runPrefix+"-etl"

	runPreview := func(wantExit int, args ...string) previewDoc {
		t.Helper()
		r := runCLI(ctx, t, bin, env, append([]string{"preview", "--json"}, args...)...)
		if r.exitCode != wantExit {
			t.Fatalf("preview %v: want exit %d, got %d\nstderr: %s", args, wantExit, r.exitCode, r.stderr)
		}
		var d previewDoc
		if err := json.Unmarshal([]byte(r.stdout), &d); err != nil {
			t.Fatalf("preview --json: stdout is not JSON: %v\nstdout: %s", err, r.stdout)
		}
		if !d.ResultReady {
			t.Fatalf("preview %s: result_ready false", d.ID)
		}
		return d
	}
	listTables := func() map[string]bool {
		t.Helper()
		r := runCLI(ctx, t, bin, env, "storage", "tables")
		if r.exitCode != 0 {
			t.Fatalf("storage tables: exit %d", r.exitCode)
		}
		var v struct {
			Tables []struct {
				Namespace string `json:"namespace"`
				Name      string `json:"name"`
			} `json:"tables"`
		}
		if err := json.Unmarshal([]byte(r.stdout), &v); err != nil {
			t.Fatalf("storage tables: stdout is not JSON: %v", err)
		}
		out := map[string]bool{}
		for _, tb := range v.Tables {
			out[tb.Namespace+"."+tb.Name] = true
		}
		return out
	}
	scratchPrefix := func(id string) string { return "pv-" + id[:8] + "-" + runPrefix }
	hasScratch := func(tables map[string]bool, id string) []string {
		var found []string
		for k := range tables {
			if strings.HasPrefix(k, scratchPrefix(id)) {
				found = append(found, k)
			}
		}
		return found
	}

	// Full capped two-stage preview.
	full := runPreview(0, "-f", docPath, "--workspace", wsA, "--rows", "50", "--timeout", "8m")
	if full.Phase != "Succeeded" {
		t.Fatalf("full preview %s: phase %q, want Succeeded", full.ID, full.Phase)
	}
	var rawScratch, etlScratch string
	for _, o := range full.Outputs {
		switch {
		case o.Bucket == rawBucket && o.Table == "data":
			rawScratch = o.ScratchTable
			if o.Rows != 50 || !o.AtCap {
				t.Errorf("%s.data: rows=%d at_cap=%t, want 50/true", rawBucket, o.Rows, o.AtCap)
			}
			if len(o.Schema) == 0 || len(o.ColumnsHint) == 0 {
				t.Errorf("%s.data: empty schema (%d) or columns_hint (%d)", rawBucket, len(o.Schema), len(o.ColumnsHint))
			}
			if o.Sample == nil || len(o.Sample.Rows) == 0 {
				t.Errorf("%s.data: empty sample", rawBucket)
			}
		case o.Bucket == etlBucket && o.Table == "user_summary":
			etlScratch = o.ScratchTable
		}
	}
	if rawScratch == "" || etlScratch == "" {
		t.Fatalf("full preview outputs missing %s.data or %s.user_summary: %+v", rawBucket, etlBucket, full.Outputs)
	}

	tables := listTables()
	for _, real := range []string{rawBucket + ".data", etlBucket + ".user_summary"} {
		if tables[real] {
			t.Errorf("storage tables lists real table %s; previews must write scratch only", real)
		}
	}
	for _, s := range []string{rawScratch, etlScratch} {
		if !tables[s] {
			t.Errorf("storage tables does not list scratch table %s", s)
		}
	}

	ns := rawScratch[:strings.LastIndex(rawScratch, ".")]
	qr := runCLI(ctx, t, bin, env, "query", "--format", "json", "--sql", fmt.Sprintf(`select count(*) from "%s"."data"`, ns))
	if qr.exitCode != 0 {
		t.Fatalf("query scratch table: exit %d", qr.exitCode)
	}
	var q struct {
		Rows [][]any `json:"rows"`
	}
	if err := json.Unmarshal([]byte(qr.stdout), &q); err != nil || len(q.Rows) != 1 || len(q.Rows[0]) != 1 {
		t.Fatalf("query result: %v, stdout: %s", err, qr.stdout)
	}
	if got := fmt.Sprint(q.Rows[0][0]); got != "50" {
		t.Errorf("count(*) on %s.data = %s, want 50", ns, got)
	}

	// Capped single-stage preview.
	start := time.Now()
	single := runPreview(0, "-f", docPath, "--workspace", wsA, "--rows", "50", "--until-stage", "extract", "--timeout", "5m")
	t.Logf("capped single-stage preview %s wall-clock: %s", single.ID, time.Since(start).Round(100*time.Millisecond))
	if len(single.Stages) != 1 || single.Stages[0].Name != "extract" {
		t.Errorf("--until-stage extract: stages %+v, want exactly [extract]", single.Stages)
	}

	lr := runCLI(ctx, t, bin, env, "preview", "list", "--workspace", wsB, "--json")
	if lr.exitCode != 0 {
		t.Fatalf("preview list --workspace %s: exit %d", wsB, lr.exitCode)
	}
	for _, id := range []string{full.ID, single.ID} {
		if strings.Contains(lr.stdout, id) {
			t.Errorf("preview list --workspace %s contains %s from %s", wsB, id, wsA)
		}
	}

	// A user error surfaces as FailedUser with the component's log tail.
	raw, err := os.ReadFile(docPath)
	if err != nil {
		t.Fatal(err)
	}
	badDoc := strings.Replace(string(raw), `SELECT "userId"`, `SELECT "no_such_column"`, 1)
	if badDoc == string(raw) {
		t.Fatal("bad doc: SQL substitution did not apply")
	}
	badPath := filepath.Join(t.TempDir(), "preview-bad.yaml")
	if err := os.WriteFile(badPath, []byte(badDoc), 0o600); err != nil {
		t.Fatal(err)
	}
	bad := runPreview(1, "-f", badPath, "--workspace", wsA, "--rows", "50", "--timeout", "8m")
	if bad.Phase != "FailedUser" {
		t.Errorf("bad preview %s: phase %q, want FailedUser", bad.ID, bad.Phase)
	}
	hasTail := false
	for _, s := range bad.Stages {
		for _, c := range s.Components {
			if c.LogTail != nil && *c.LogTail != "" {
				hasTail = true
			}
		}
	}
	if !hasTail {
		t.Errorf("bad preview %s: no component log_tail", bad.ID)
	}

	dr := runCLI(ctx, t, bin, env, "preview", "delete", "--workspace", wsA, "--all")
	if dr.exitCode != 0 {
		t.Fatalf("preview delete --workspace %s --all: exit %d", wsA, dr.exitCode)
	}
	tables = listTables()
	for _, id := range []string{full.ID, single.ID, bad.ID} {
		if left := hasScratch(tables, id); len(left) > 0 {
			t.Errorf("after delete --all, preview %s still has scratch tables %v", id, left)
		}
	}

	// The sweeper purges an expired preview.
	ttl := runPreview(0, "-f", docPath, "--workspace", wsTTL, "--rows", "10", "--until-stage", "extract", "--ttl", "60s", "--timeout", "5m")
	if len(hasScratch(listTables(), ttl.ID)) == 0 {
		t.Fatalf("ttl preview %s: no scratch table listed before expiry", ttl.ID)
	}
	deadline := time.Now().Add(3 * time.Minute)
	for {
		left := hasScratch(listTables(), ttl.ID)
		if len(left) == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("ttl preview %s: scratch tables %v not purged within 3m", ttl.ID, left)
		}
		time.Sleep(10 * time.Second)
	}
}
