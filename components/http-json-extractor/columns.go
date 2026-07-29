package main

import (
	"context"

	pb "github.com/datuplet/datuplet/pkg/datagateway/proto/v2"
	sdk "github.com/datuplet/datuplet/sdk/go"
	dgarrow "github.com/datuplet/datuplet/sdk/go/arrow"
)

// fieldColumns adapts the config's `fields` projection to the SDK sink's
// explicit-columns form: names in declared order, values resolved via
// getValueRaw dot-paths.
func fieldColumns(fields []FieldMapping) ([]string, func(rec map[string]any, i int) any) {
	names := make([]string, len(fields))
	paths := make([]string, len(fields))
	for i, f := range fields {
		names[i] = f.Name
		paths[i] = f.Path
	}
	return names, func(rec map[string]any, i int) any {
		return getValueRaw(rec, paths[i])
	}
}

// newExtractorSink builds the StringSink both extraction paths write to:
// lazy Arrow-IPC writer open, explicit columns when a fields projection is
// configured, first-batch inference otherwise.
func newExtractorSink(ctx context.Context, client *sdk.Client, outputTable string, fields []FieldMapping) *dgarrow.StringSink {
	var opts []dgarrow.SinkOption
	if len(fields) > 0 {
		names, extract := fieldColumns(fields)
		opts = append(opts, dgarrow.WithColumns(names, extract))
	}
	return dgarrow.NewStringSink(ctx, func() (dgarrow.ChunkWriter, error) {
		return client.OpenWriter(ctx, outputTable, sdk.WithFormat(pb.DataFormat_FORMAT_ARROW_IPC))
	}, opts...)
}

// finishQuietly calls sink.Finish() and discards the result. It exists
// because sdk.ExitUserError/ExitAppError call os.Exit, which skips all of
// main's deferred cleanup — so an already-opened gateway writer (e.g. a
// single-request fetch that streamed past the first batch, flushed once,
// then hit a decode error in the malformed tail) would otherwise be left
// open. Call this immediately before any sdk.Exit* that can run after the
// sink was constructed. Finish is idempotent, so a later real Finish call
// (or another finishQuietly call) is a harmless no-op.
func finishQuietly(sink *dgarrow.StringSink) {
	_, _, _ = sink.Finish()
}
