package datagateway

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"testing"

	"github.com/datuplet/datuplet/pkg/datagateway/schema"
)

// captureLog redirects the standard logger for the duration of fn and returns
// everything it emitted.
func captureLog(t *testing.T, fn func()) string {
	t.Helper()
	var buf bytes.Buffer
	prevOut := log.Writer()
	prevFlags := log.Flags()
	log.SetOutput(&buf)
	log.SetFlags(0)
	t.Cleanup(func() {
		log.SetOutput(prevOut)
		log.SetFlags(prevFlags)
	})
	fn()
	return buf.String()
}

// looksLikeSchemaMismatch gates an extra catalog round-trip on the commit
// failure path, so it must fire for real schema rejections and stay quiet for
// cancellation/shutdown (where the catalog is going away anyway).
func TestLooksLikeSchemaMismatch(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"cancelled", context.Canceled, false},
		{"deadline", context.DeadlineExceeded, false},
		{"wrapped cancellation is still skipped", fmt.Errorf("commit: %w", context.Canceled), false},
		{"iceberg schema rejection", errors.New("cannot add files: schema mismatch for column value"), true},
		{"field-level message", errors.New("field 'date' has type long, file has utf8"), true},
		{"incompatible wording", errors.New("incompatible parquet file"), true},
		{"type mismatch wording", errors.New("type mismatch on column x"), true},
		{"capitalisation is ignored", errors.New("Schema Mismatch"), true},
		{"unrelated failure does not pay for a catalog read", errors.New("503 service unavailable"), false},
		{"credential failure", errors.New("token expired"), false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := looksLikeSchemaMismatch(tc.err); got != tc.want {
				t.Errorf("looksLikeSchemaMismatch(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

// A nil catalog must not panic — the failure path runs during shutdown too.
func TestLogCatalogSchemaOnMismatch_NilCatalogIsSafe(t *testing.T) {
	logCatalogSchemaOnMismatch(context.Background(), nil, "raw", "t", errors.New("schema mismatch"))
}

// isSchemaDeferred distinguishes "table not created yet" (expected) from a
// real bootstrap failure. Getting this wrong either hides a genuine problem or
// logs a scary Warning for the normal first-write path.
func TestIsSchemaDeferred(t *testing.T) {
	deferred := errors.New("failed to resolve write path for raw.t: lakekeeper: table raw.t missing and no schema available to create it")
	if !isSchemaDeferred(deferred) {
		t.Error("the lakekeeper schema-deferred message should be recognised as expected")
	}
	if isSchemaDeferred(nil) {
		t.Error("nil is not a deferred schema")
	}
	if isSchemaDeferred(errors.New("permission denied listing bucket")) {
		t.Error("an unrelated bootstrap failure must NOT be treated as expected")
	}
}

// logWriterSchema is the line that makes a schema mismatch diagnosable, so it
// must render types and nullability, survive a nil schema, and bound its own
// output on wide tables.
func TestLogWriterSchema(t *testing.T) {
	t.Run("nil schema does not panic", func(t *testing.T) {
		logWriterSchema(&writerState{writerID: "w1", bucket: "raw", table: "t"}, "component")
	})

	t.Run("renders name:type with nullability marker", func(t *testing.T) {
		sch, err := schema.NewSchema([]schema.ColumnDef{
			{Name: "id", Type: schema.TypeInt64, Nullable: false},
			{Name: "label", Type: schema.TypeString, Nullable: true},
		})
		if err != nil {
			t.Fatalf("NewSchema: %v", err)
		}
		line := captureLog(t, func() {
			logWriterSchema(&writerState{writerID: "w1", bucket: "raw", table: "t", schema: sch}, "component")
		})
		for _, want := range []string{"writer=w1", "raw.t", "source=component", "columns=2", "label:", "?"} {
			if !strings.Contains(line, want) {
				t.Errorf("log line missing %q\ngot: %s", want, line)
			}
		}
		// A non-nullable column must NOT get the "?" marker.
		if strings.Contains(line, "id:int64?") {
			t.Errorf("non-nullable column wrongly marked nullable\ngot: %s", line)
		}
	})

	t.Run("wide schema is truncated but reports the true total", func(t *testing.T) {
		cols := make([]schema.ColumnDef, maxLoggedSchemaColumns+7)
		for i := range cols {
			cols[i] = schema.ColumnDef{Name: fmt.Sprintf("c%d", i), Type: schema.TypeString, Nullable: true}
		}
		sch, err := schema.NewSchema(cols)
		if err != nil {
			t.Fatalf("NewSchema: %v", err)
		}
		line := captureLog(t, func() {
			logWriterSchema(&writerState{writerID: "w2", bucket: "raw", table: "wide", schema: sch}, "gateway-inferred")
		})
		if !strings.Contains(line, fmt.Sprintf("columns=%d", len(cols))) {
			t.Errorf("must report the exact total column count\ngot: %s", line)
		}
		if !strings.Contains(line, "+7 more") {
			t.Errorf("must disclose how many columns were omitted\ngot: %s", line)
		}
		if strings.Contains(line, fmt.Sprintf("c%d:", len(cols)-1)) {
			t.Errorf("last column should have been truncated away\ngot: %s", line)
		}
	})
}
