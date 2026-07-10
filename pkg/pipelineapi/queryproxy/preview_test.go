package queryproxy

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"

	"github.com/datuplet/datuplet/pkg/pipelineapi/auth"
	"github.com/datuplet/datuplet/pkg/pipelineapi/projectgate/projectgatetest"
	"github.com/datuplet/datuplet/pkg/pipelineapi/store"
)

// TestCorePreview_SQLShapeAndGate asserts two things about Core.Preview
// (RFC 025 Task 3.1): it builds the exact server-generated SELECT statement
// from ns/table/MaxRows, and it gates per-principal at capacity 1 — a
// second concurrent preview for the SAME principal bounces with 429 while
// the first is still in flight.
func TestCorePreview_SQLShapeAndGate(t *testing.T) {
	// All cross-goroutine values travel over channels — no unsynchronized
	// shared variables (this test must survive `go test -race`).
	sqlCh := make(chan string, 1)
	block := make(chan struct{})
	worker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body workerRequest
		_ = json.NewDecoder(r.Body).Decode(&body)
		sqlCh <- body.SQL
		<-block
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"schema":[{"name":"id","type":"INTEGER"}],"rows":[[1]],"truncated":true,"stats":{"duration_ms":1}}`))
	}))
	defer worker.Close()

	core, err := NewCore(Config{WorkerURL: worker.URL, Gate: projectgatetest.AllowAll("p", "w")}, testSigner(t))
	if err != nil {
		t.Fatal(err)
	}
	subID := uuid.New()
	sub := subID.String()
	// The minters read the subject from ctx — hence the authed ctx.
	ctxUser := auth.WithCtxUser(context.Background(), &store.User{ID: subID, Email: "t@e.c"})
	lim := PreviewLimits{TimeoutS: 30, MaxRows: 100, MaxBytes: 1 << 20}

	type previewOut struct {
		res  *Result
		qerr *QueryError
	}
	outCh := make(chan previewOut, 1)
	go func() {
		res, qerr := core.Preview(ctxUser, sub, "p/w", "my-ns", "users", lim)
		outCh <- previewOut{res, qerr}
	}()

	gotSQL := <-sqlCh // first preview is in-flight and holds the gate slot

	// Second concurrent preview for the SAME principal must bounce (cap 1).
	if _, qerr := core.Preview(ctxUser, sub, "p/w", "my-ns", "users", lim); qerr == nil || qerr.Status != http.StatusTooManyRequests {
		t.Fatalf("second concurrent preview = %+v, want 429", qerr)
	}
	close(block)

	first := <-outCh // all assertions happen on the test goroutine
	if first.qerr != nil {
		t.Fatalf("Preview: %+v", first.qerr)
	}
	if !first.res.Truncated || len(first.res.Rows) != 1 || first.res.Schema[0].Name != "id" {
		t.Fatalf("decoded result mismatch: %+v", first.res)
	}
	if want := `SELECT * FROM lk."my-ns"."users" LIMIT 100`; gotSQL != want {
		t.Fatalf("sql = %q, want %q", gotSQL, want)
	}
}
