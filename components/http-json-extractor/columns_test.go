package main

import (
	"context"
	"testing"

	sdk "github.com/datuplet/datuplet/sdk/go"
	dgarrow "github.com/datuplet/datuplet/sdk/go/arrow"
)

func TestFieldColumns(t *testing.T) {
	names, extract := fieldColumns([]FieldMapping{
		{Path: "country.value", Name: "entity"},
		{Path: "iso", Name: "iso3"},
	})
	if len(names) != 2 || names[0] != "entity" || names[1] != "iso3" {
		t.Fatalf("names = %v (declared order required)", names)
	}
	rec := map[string]any{"country": map[string]any{"value": "Africa"}, "iso": "AFE"}
	if v := extract(rec, 0); v != "Africa" {
		t.Fatalf("extract nested = %v", v)
	}
	if v := extract(rec, 1); v != "AFE" {
		t.Fatalf("extract flat = %v", v)
	}
	if v := extract(map[string]any{}, 0); v != nil {
		t.Fatalf("missing path should be nil, got %v", v)
	}
}

// fakeChunkWriter is a minimal dgarrow.ChunkWriter for finishQuietly tests.
type fakeChunkWriter struct {
	closed bool
}

func (f *fakeChunkWriter) Write(_ context.Context, _ []byte) error { return nil }
func (f *fakeChunkWriter) Close(_ context.Context) (*sdk.CloseResult, error) {
	f.closed = true
	return &sdk.CloseResult{TotalRows: 1}, nil
}
func (f *fakeChunkWriter) Bucket() string { return "raw" }
func (f *fakeChunkWriter) Table() string  { return "t" }

// TestFinishQuietly_ClosesAlreadyOpenedWriter covers Finding I1: a sink
// that already flushed a batch (and so opened its writer) must have that
// writer closed by finishQuietly, matching the case where sdk.Exit* runs
// after the sink has written data but before the normal Finish() call.
func TestFinishQuietly_ClosesAlreadyOpenedWriter(t *testing.T) {
	fw := &fakeChunkWriter{}
	sink := dgarrow.NewStringSink(context.Background(), func() (dgarrow.ChunkWriter, error) { return fw, nil },
		dgarrow.WithColumns([]string{"id"}, func(rec map[string]any, _ int) any { return rec["id"] }))
	if err := sink.Add(map[string]any{"id": "1"}); err != nil {
		t.Fatal(err)
	}
	finishQuietly(sink)
	if !fw.closed {
		t.Fatal("finishQuietly must close a writer the sink already opened")
	}
	// Idempotent: a later call (e.g. main's normal-path Finish, or another
	// finishQuietly from a defer) must not panic or double-close incorrectly.
	finishQuietly(sink)
}

// TestFinishQuietly_UntouchedSinkNoWriter covers the fetchStream-failure
// case: finishQuietly on a sink that never received any records must be a
// harmless no-op (no writer ever opened, per StringSink's lazy-open design).
func TestFinishQuietly_UntouchedSinkNoWriter(t *testing.T) {
	opened := false
	sink := dgarrow.NewStringSink(context.Background(), func() (dgarrow.ChunkWriter, error) {
		opened = true
		return &fakeChunkWriter{}, nil
	}, dgarrow.WithColumns([]string{"id"}, func(rec map[string]any, _ int) any { return rec["id"] }))
	finishQuietly(sink)
	if opened {
		t.Fatal("finishQuietly on an untouched sink must not open a writer")
	}
}
