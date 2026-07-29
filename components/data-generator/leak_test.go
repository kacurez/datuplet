package main

import (
	"context"
	"errors"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	sdk "github.com/datuplet/datuplet/sdk/go"
	dgarrow "github.com/datuplet/datuplet/sdk/go/arrow"
)

// fakeChunkWriter is a minimal dgarrow.ChunkWriter for proving
// finishRandomQuietly's effect on the underlying writer without a real
// gateway connection. runRandom takes a concrete *sdk.Client (via
// client.OpenWriter), which itself requires a live gRPC connection, so this
// is the practical seam: dgarrow.Sink's `open` callback only needs a
// dgarrow.ChunkWriter, and that's an exported interface we can fake
// directly. This exercises the real finishRandomQuietly/dgarrow.Sink
// mechanics runRandom's defer relies on; it does not drive runRandom (and
// therefore *sdk.Client) end-to-end.
type fakeChunkWriter struct {
	writeErr   error
	closeErr   error
	writes     int
	closeCalls int
}

func (f *fakeChunkWriter) Write(ctx context.Context, data []byte) error {
	f.writes++
	return f.writeErr
}

func (f *fakeChunkWriter) Close(ctx context.Context) (*sdk.CloseResult, error) {
	f.closeCalls++
	return &sdk.CloseResult{}, f.closeErr
}

func (f *fakeChunkWriter) Bucket() string { return "bucket" }
func (f *fakeChunkWriter) Table() string  { return "table" }

func leakTestSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
	}, nil)
}

func appendLeakTestRow(sink *dgarrow.Sink, id int64) {
	sink.Builder().Field(0).(*array.Int64Builder).Append(id)
}

// TestFinishRandomQuietly_ClosesWriterBeforeExit proves the leak fix for
// the sdk.ExitUserError injection path (generator.go, "user-error injection
// point" branch): before the port, that path called sdk.ExitUserError
// directly, and os.Exit skips every deferred cleanup, so a writer already
// opened by an earlier flushed batch was left open at the gateway. Now
// runRandom calls finishRandomQuietly(sink) immediately before that exit.
// This test proves finishRandomQuietly, called on a sink with no prior
// failure (the normal case for this injection point — it fires on a row
// count, independent of any write outcome), both flushes the pending batch
// and closes the writer exactly once.
func TestFinishRandomQuietly_ClosesWriterBeforeExit(t *testing.T) {
	fw := &fakeChunkWriter{}
	opened := false
	sink := dgarrow.NewSink(context.Background(), func() (dgarrow.ChunkWriter, error) {
		opened = true
		return fw, nil
	}, leakTestSchema(), dgarrow.WithBatchRows(100)) // large batch: rows stay pending

	for i := int64(0); i < 5; i++ {
		appendLeakTestRow(sink, i)
		if err := sink.RowDone(); err != nil {
			t.Fatalf("RowDone: %v", err)
		}
	}

	if opened {
		t.Fatal("writer must not be opened yet: batch (100) not reached, nothing flushed")
	}
	if fw.closeCalls != 0 {
		t.Fatalf("writer must not be closed yet, closeCalls=%d", fw.closeCalls)
	}

	// This is the exact call runRandom makes immediately before
	// sdk.ExitUserError.
	finishRandomQuietly(sink)

	if !opened {
		t.Fatal("finishRandomQuietly must flush the pending partial batch, opening the writer")
	}
	if fw.writes != 1 {
		t.Fatalf("expected exactly one flushed IPC stream (the partial batch), got writes=%d", fw.writes)
	}
	if fw.closeCalls != 1 {
		t.Fatalf("expected the writer to be closed exactly once, got closeCalls=%d", fw.closeCalls)
	}

	// The deferred call in runRandom fires on top of this explicit one on
	// every return path. Finish (and therefore finishRandomQuietly) must be
	// idempotent so that doesn't double-close.
	finishRandomQuietly(sink)
	if fw.closeCalls != 1 {
		t.Fatalf("finishRandomQuietly must be idempotent: closeCalls should stay 1, got %d", fw.closeCalls)
	}
}

// TestFinishRandomQuietly_SafeAfterFailedFlush proves the behavior of
// runRandom's `defer finishRandomQuietly(sink)` for the other leak scenario
// in scope: a sibling table's failure cancels the shared run context
// (main.go:90), and this table's own next flush then fails too — which is
// how every one of runRandom's own non-injection error returns arises,
// since both of its error-return sites are dgarrow.Sink failures.
// dgarrow.Sink.Finish() makes a single best-effort attempt to close the
// writer once a prior failure is sticky (the failure normally comes from a
// failed writer.Write, so the writer was already open, and leaving it
// unclosed would dangle the gateway-side writer session), while still
// returning the original sticky write error to the caller unchanged.
// http-json-extractor's identical finishQuietly/defer adoption of the same
// core shares this property. This test proves: after a failed flush, a
// subsequent finishRandomQuietly closes the writer exactly once, and
// repeated calls stay safe (no double-close).
func TestFinishRandomQuietly_SafeAfterFailedFlush(t *testing.T) {
	fw := &fakeChunkWriter{}
	sink := dgarrow.NewSink(context.Background(), func() (dgarrow.ChunkWriter, error) {
		return fw, nil
	}, leakTestSchema(), dgarrow.WithBatchRows(1)) // flush every row

	appendLeakTestRow(sink, 1)
	if err := sink.RowDone(); err != nil {
		t.Fatalf("first RowDone should succeed, got %v", err)
	}
	if fw.closeCalls != 0 {
		t.Fatalf("writer must not be closed yet, closeCalls=%d", fw.closeCalls)
	}

	// Simulate the shared run context getting cancelled by a sibling
	// table's failure: this table's next flush now fails too.
	fw.writeErr = errors.New("simulated write failure (e.g. context canceled)")
	appendLeakTestRow(sink, 2)
	if err := sink.RowDone(); err == nil {
		t.Fatal("expected RowDone to fail on simulated write error")
	}

	// This is the exact call runRandom's `defer finishRandomQuietly(sink)`
	// makes on this return path.
	finishRandomQuietly(sink)

	if fw.closeCalls != 1 {
		t.Fatalf("dgarrow.Sink makes a best-effort close of the already-opened writer after a sticky failure; got closeCalls=%d, want 1", fw.closeCalls)
	}

	// Still must be safe to call again (e.g. if runRandom's own explicit
	// call site and its defer both run) — exactly one close, not two.
	finishRandomQuietly(sink)
	if fw.closeCalls != 1 {
		t.Fatalf("repeated finishRandomQuietly after failure must not double-close, got closeCalls=%d, want 1", fw.closeCalls)
	}
}
