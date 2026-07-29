package main

import (
	"bytes"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// strField builds an all-String Nullable:true field — the sink's contract.
func strField(name string) arrow.Field {
	return arrow.Field{Name: name, Type: arrow.BinaryTypes.String, Nullable: true}
}

// buildIPCStream encodes one self-contained Arrow IPC stream (schema +
// one record batch + EOS) — the shape a real Writer.Write POST body always
// has. rows may be nil/empty to produce a valid, self-contained,
// ZERO-ROW record batch (distinct from buildEmptyIPCStream's zero-BATCH
// stream). Each row must supply exactly len(fields) values (nil = null).
func buildIPCStream(t *testing.T, fields []arrow.Field, rows [][]*string) []byte {
	t.Helper()
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema(fields, nil)
	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()
	for _, row := range rows {
		for i, v := range row {
			sb := b.Field(i).(*array.StringBuilder)
			if v == nil {
				sb.AppendNull()
			} else {
				sb.Append(*v)
			}
		}
	}
	rec := b.NewRecord()
	defer rec.Release()

	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(schema), ipc.WithAllocator(mem))
	if err := w.Write(rec); err != nil {
		t.Fatalf("ipc write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("ipc close: %v", err)
	}
	return buf.Bytes()
}

// buildEmptyIPCStream encodes a schema-only IPC stream with NO record
// batches at all (schema message + EOS, no Write call) — the degenerate
// "zero-row stream" case, distinct from a record batch that itself has zero
// rows (see buildIPCStream with nil rows).
func buildEmptyIPCStream(t *testing.T, fields []arrow.Field) []byte {
	t.Helper()
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema(fields, nil)
	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(schema), ipc.WithAllocator(mem))
	if err := w.Close(); err != nil {
		t.Fatalf("ipc close: %v", err)
	}
	return buf.Bytes()
}

// buildNonUTF8Stream encodes a valid, self-contained IPC stream whose one
// column is Int64 instead of String — violates the all-String contract.
func buildNonUTF8Stream(t *testing.T) []byte {
	t.Helper()
	mem := memory.NewGoAllocator()
	fields := []arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true}}
	schema := arrow.NewSchema(fields, nil)
	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()
	b.Field(0).(*array.Int64Builder).Append(1)
	rec := b.NewRecord()
	defer rec.Release()

	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(schema), ipc.WithAllocator(mem))
	if err := w.Write(rec); err != nil {
		t.Fatalf("ipc write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("ipc close: %v", err)
	}
	return buf.Bytes()
}

func strp(s string) *string { return &s }

// newTestGateway returns a mockGateway with one pre-opened writer "w1", as
// OpenWriter would leave it — ingest() is only ever called for a writer ID
// already present in the map in the real flow.
func newTestGateway() *mockGateway {
	return &mockGateway{writers: map[string]*writerState{"w1": {bucket: "raw", table: "t"}}}
}

func TestIngest_Validation(t *testing.T) {
	tests := []struct {
		name    string
		payload func(t *testing.T) []byte
		wantErr string // substring expected in the error; empty = expect success
	}{
		{
			name: "valid single stream",
			payload: func(t *testing.T) []byte {
				return buildIPCStream(t, []arrow.Field{strField("id"), strField("name")},
					[][]*string{{strp("1"), strp("a")}, {strp("2"), strp("b")}})
			},
		},
		{
			// C1: concatenating two valid IPC streams into one POST body is
			// exactly the defect this whole mock exists to catch (see the
			// gen-big-pipeline write-benchmark regression documented in
			// sdk/go/client.go's OpenWriterToBucket).
			name: "concatenated streams",
			payload: func(t *testing.T) []byte {
				one := buildIPCStream(t, []arrow.Field{strField("id")}, [][]*string{{strp("1")}})
				two := buildIPCStream(t, []arrow.Field{strField("id")}, [][]*string{{strp("2")}})
				return append(append([]byte{}, one...), two...)
			},
			wantErr: "trailing",
		},
		{
			name: "valid stream followed by garbage",
			payload: func(t *testing.T) []byte {
				one := buildIPCStream(t, []arrow.Field{strField("id")}, [][]*string{{strp("1")}})
				return append(append([]byte{}, one...), []byte("not-arrow-garbage")...)
			},
			wantErr: "trailing",
		},
		{
			name: "zero-row stream (no record batches)",
			payload: func(t *testing.T) []byte {
				return buildEmptyIPCStream(t, []arrow.Field{strField("id")})
			},
			wantErr: "zero rows",
		},
		{
			name: "zero-row batch",
			payload: func(t *testing.T) []byte {
				return buildIPCStream(t, []arrow.Field{strField("id")}, nil)
			},
			wantErr: "zero rows",
		},
		{
			name: "non-utf8 column",
			payload: func(t *testing.T) []byte {
				return buildNonUTF8Stream(t)
			},
			wantErr: "utf8",
		},
		{
			name: "non-nullable column",
			payload: func(t *testing.T) []byte {
				fields := []arrow.Field{{Name: "id", Type: arrow.BinaryTypes.String, Nullable: false}}
				return buildIPCStream(t, fields, [][]*string{{strp("1")}})
			},
			wantErr: "nullable",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			g := newTestGateway()
			data := tc.payload(t)
			rows, err := g.ingest("w1", data)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("ingest() unexpected error: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("ingest() = (%d, nil), want error containing %q", rows, tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("ingest() error = %q, want substring %q", err.Error(), tc.wantErr)
			}
		})
	}
}

// TestIngest_SchemaDriftAcrossWrites: a writer's first payload locks the
// schema; a later payload on the SAME writer with a different column set
// must be rejected even though each payload is individually valid
// (all-String, all-Nullable) — this is the no-`fields`-projection case
// (expectCols empty) where only the drift lock, not validateSchema's
// column-name check, can catch the change.
func TestIngest_SchemaDriftAcrossWrites(t *testing.T) {
	g := newTestGateway()

	first := buildIPCStream(t, []arrow.Field{strField("id"), strField("name")},
		[][]*string{{strp("1"), strp("a")}})
	if _, err := g.ingest("w1", first); err != nil {
		t.Fatalf("first ingest: unexpected error: %v", err)
	}

	second := buildIPCStream(t, []arrow.Field{strField("id"), strField("other")},
		[][]*string{{strp("2"), strp("z")}})
	_, err := g.ingest("w1", second)
	if err == nil {
		t.Fatal("second ingest: expected schema-drift error, got nil")
	}
	if !strings.Contains(err.Error(), "drift") {
		t.Fatalf("second ingest: error = %q, want substring %q", err.Error(), "drift")
	}
}
