package datagateway

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

// looksLikeSchemaMismatch gates an extra catalog round-trip on the commit
// failure path, so it must fire for real schema rejections and stay quiet for
// cancellation/shutdown (where the catalog is going away anyway).
//
// isSchemaDeferred distinguishes "table not created yet" (expected) from a
// real bootstrap failure. Getting this wrong either hides a genuine problem or
// logs a scary Warning for the normal first-write path.
func TestSchemaErrorClassifiers(t *testing.T) {
	tests := []struct {
		name string
		fn   func(error) bool
		err  error
		want bool
	}{
		{"mismatch: nil", looksLikeSchemaMismatch, nil, false},
		{"mismatch: cancelled", looksLikeSchemaMismatch, context.Canceled, false},
		{"mismatch: deadline", looksLikeSchemaMismatch, context.DeadlineExceeded, false},
		{"mismatch: wrapped cancellation is still skipped", looksLikeSchemaMismatch, fmt.Errorf("commit: %w", context.Canceled), false},
		{"mismatch: iceberg schema rejection", looksLikeSchemaMismatch, errors.New("cannot add files: schema mismatch for column value"), true},
		{"mismatch: field-level message", looksLikeSchemaMismatch, errors.New("field 'date' has type long, file has utf8"), true},
		{"mismatch: incompatible wording", looksLikeSchemaMismatch, errors.New("incompatible parquet file"), true},
		{"mismatch: type mismatch wording", looksLikeSchemaMismatch, errors.New("type mismatch on column x"), true},
		{"mismatch: capitalisation is ignored", looksLikeSchemaMismatch, errors.New("Schema Mismatch"), true},
		{"mismatch: unrelated failure does not pay for a catalog read", looksLikeSchemaMismatch, errors.New("503 service unavailable"), false},
		{"mismatch: credential failure", looksLikeSchemaMismatch, errors.New("token expired"), false},
		{"deferred: lakekeeper schema-deferred message is expected", isSchemaDeferred,
			errors.New("failed to resolve write path for raw.t: lakekeeper: table raw.t missing and no schema available to create it"), true},
		{"deferred: nil is not a deferred schema", isSchemaDeferred, nil, false},
		{"deferred: unrelated bootstrap failure must NOT be treated as expected", isSchemaDeferred, errors.New("permission denied listing bucket"), false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.fn(tc.err); got != tc.want {
				t.Errorf("got %v, want %v for %v", got, tc.want, tc.err)
			}
		})
	}
}

// The failure-path log helpers run during shutdown too, so a nil catalog and
// a nil writer schema must not panic.
func TestLogHelpers_NilSafeOnShutdown(t *testing.T) {
	logCatalogSchemaOnMismatch(context.Background(), nil, "raw", "t", errors.New("schema mismatch"))
	logWriterSchema(&writerState{writerID: "w1", bucket: "raw", table: "t"}, "component")
}
