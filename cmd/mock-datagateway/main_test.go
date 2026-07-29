package main

import (
	"bytes"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"

	pb "github.com/datuplet/datuplet/pkg/datagateway/proto/v2"
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

// buildMixedTypeStream encodes a valid, self-contained IPC stream with one
// column of each type in the sink type vocabulary (dgarrow.ArrowTypeFor:
// Int64, Float64, Boolean, String — the mix an InferringSink can legitimately
// produce across different columns of the same schema), all Nullable:true.
func buildMixedTypeStream(t *testing.T) []byte {
	t.Helper()
	mem := memory.NewGoAllocator()
	fields := []arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "amount", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		{Name: "active", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	}
	schema := arrow.NewSchema(fields, nil)
	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()
	b.Field(0).(*array.Int64Builder).Append(1)
	b.Field(1).(*array.Float64Builder).Append(1.5)
	b.Field(2).(*array.BooleanBuilder).Append(true)
	b.Field(3).(*array.StringBuilder).Append("a")
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

// buildOutOfVocabStream encodes a valid, self-contained IPC stream whose one
// column is Timestamp — a real Arrow type, but OUTSIDE the sink type
// vocabulary (dgarrow.ArrowTypeFor only ever resolves to Int64/Float64/
// Boolean/String) — must still be rejected by validateSchema.
func buildOutOfVocabStream(t *testing.T) []byte {
	t.Helper()
	mem := memory.NewGoAllocator()
	fields := []arrow.Field{{Name: "ts", Type: arrow.FixedWidthTypes.Timestamp_ns, Nullable: true}}
	schema := arrow.NewSchema(fields, nil)
	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()
	b.Field(0).(*array.TimestampBuilder).Append(0)
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

// buildNonNullableTypedStream encodes a valid, self-contained IPC stream
// with one Int64 column that is Nullable:false — an in-vocabulary TYPE but
// still a nullability violation, proving the Nullable:true requirement holds
// across the whole vocabulary, not just utf8.
func buildNonNullableTypedStream(t *testing.T) []byte {
	t.Helper()
	mem := memory.NewGoAllocator()
	fields := []arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false}}
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

// intField/floatField/boolField build Nullable:true fields of their
// respective type — the numeric/boolean counterparts of strField, for
// declared-columns-mapping tests that need a mix of types.
func intField(name string) arrow.Field {
	return arrow.Field{Name: name, Type: arrow.PrimitiveTypes.Int64, Nullable: true}
}
func floatField(name string) arrow.Field {
	return arrow.Field{Name: name, Type: arrow.PrimitiveTypes.Float64, Nullable: true}
}
func boolField(name string) arrow.Field {
	return arrow.Field{Name: name, Type: arrow.FixedWidthTypes.Boolean, Nullable: true}
}

// buildSchemaOnlyStream encodes a valid, self-contained, ONE-ROW IPC stream
// for exactly the given fields, with a trivial placeholder value per column
// (zero/false/empty-string). The declared-columns-mapping tests below only
// care about the SCHEMA (names, types, order) validateAgainstMapping
// checks, never the row values.
func buildSchemaOnlyStream(t *testing.T, fields []arrow.Field) []byte {
	t.Helper()
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema(fields, nil)
	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()
	for i, f := range fields {
		switch f.Type.ID() {
		case arrow.INT64:
			b.Field(i).(*array.Int64Builder).Append(0)
		case arrow.FLOAT64:
			b.Field(i).(*array.Float64Builder).Append(0)
		case arrow.BOOL:
			b.Field(i).(*array.BooleanBuilder).Append(false)
		case arrow.STRING:
			b.Field(i).(*array.StringBuilder).Append("")
		default:
			t.Fatalf("buildSchemaOnlyStream: unsupported field type %s", f.Type)
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

// mappingCols builds a []*pb.ColumnConfig from (name, type) pairs, in order
// — a compact literal for declared-columns-mapping test tables.
func mappingCols(pairs ...[2]string) []*pb.ColumnConfig {
	cols := make([]*pb.ColumnConfig, len(pairs))
	for i, p := range pairs {
		cols[i] = &pb.ColumnConfig{Name: p[0], Type: p[1]}
	}
	return cols
}

// newTestGatewayWithMapping returns a mockGateway with one pre-opened writer
// "w1" against table, and a declared output_columns mapping for that same
// table — the shape GetConfig/OpenWriter would leave it in when the config
// carries output_columns (see main()).
func newTestGatewayWithMapping(table string, mapping []*pb.ColumnConfig) *mockGateway {
	return &mockGateway{
		writers:      map[string]*writerState{"w1": {bucket: "raw", table: table}},
		outputTables: []*pb.TableOutputConfig{{Name: table, Columns: mapping}},
	}
}

// TestIngest_DeclaredColumnsMapping is finding I1's teeth: the mock now
// consults a declared output_columns mapping (when one is configured for
// the writer's table) and rejects any emitted schema that doesn't match it
// EXACTLY — names, types, and order. Before this test existed, a broken
// extractor emitting the wrong schema for a declared table passed the local
// rig silently (the mapping was served via GetConfig but never checked at
// ingest time).
func TestIngest_DeclaredColumnsMapping(t *testing.T) {
	declared := mappingCols([2]string{"id", "int"}, [2]string{"userId", "int"}, [2]string{"title", "string"})

	tests := []struct {
		name    string
		fields  []arrow.Field
		wantErr string // substring expected in the error; empty = expect success
	}{
		{
			name:   "matches declared schema exactly",
			fields: []arrow.Field{intField("id"), intField("userId"), strField("title")},
		},
		{
			name:    "wrong name",
			fields:  []arrow.Field{intField("id"), intField("userId"), strField("headline")},
			wantErr: `column 2 is "headline", want "title"`,
		},
		{
			name:    "wrong type",
			fields:  []arrow.Field{intField("id"), floatField("userId"), strField("title")},
			wantErr: `column "userId" is float64, want int64 (declared type "int")`,
		},
		{
			name:    "wrong order",
			fields:  []arrow.Field{intField("userId"), intField("id"), strField("title")},
			wantErr: `column 0 is "userId", want "id"`,
		},
		{
			name:    "extra column",
			fields:  []arrow.Field{intField("id"), intField("userId"), strField("title"), strField("body")},
			wantErr: "4 columns, want 3",
		},
		{
			name:    "missing column",
			fields:  []arrow.Field{intField("id"), intField("userId")},
			wantErr: "2 columns, want 3",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			g := newTestGatewayWithMapping("posts", declared)
			data := buildSchemaOnlyStream(t, tc.fields)
			_, err := g.ingest("w1", data)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("ingest() unexpected error: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatal("ingest() = nil error, want a declared-columns schema violation")
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("ingest() error = %q, want substring %q", err.Error(), tc.wantErr)
			}
		})
	}
}

// TestIngest_DeclaredColumnsMapping_OverridesExpectCols proves the stated
// precedence: when BOTH a declared output_columns mapping and a `fields`
// projection (g.expectCols) apply to the same table, the mapping governs
// and expectCols is not consulted at all. expectCols here names completely
// different columns than the mapping — if expectCols were still checked,
// this would fail; it must not.
func TestIngest_DeclaredColumnsMapping_OverridesExpectCols(t *testing.T) {
	g := newTestGatewayWithMapping("posts", mappingCols([2]string{"id", "int"}, [2]string{"userId", "int"}, [2]string{"title", "string"}))
	g.expectCols = []string{"totally", "different", "names"}

	data := buildSchemaOnlyStream(t, []arrow.Field{intField("id"), intField("userId"), strField("title")})
	if _, err := g.ingest("w1", data); err != nil {
		t.Fatalf("declared mapping should govern over expectCols, got error: %v", err)
	}
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
			// Relaxed validateSchema (this change): the sink type vocabulary
			// is no longer utf8-only — InferringSink can legitimately emit
			// Int64/Float64/Boolean columns alongside String ones.
			name: "typed schema (int64/float64/bool/utf8 mix) accepted",
			payload: func(t *testing.T) []byte {
				return buildMixedTypeStream(t)
			},
		},
		{
			name: "type outside vocabulary (timestamp) rejected",
			payload: func(t *testing.T) []byte {
				return buildOutOfVocabStream(t)
			},
			wantErr: "want one of utf8/int64/float64/bool",
		},
		{
			name: "non-nullable utf8 column rejected",
			payload: func(t *testing.T) []byte {
				fields := []arrow.Field{{Name: "id", Type: arrow.BinaryTypes.String, Nullable: false}}
				return buildIPCStream(t, fields, [][]*string{{strp("1")}})
			},
			wantErr: "nullable",
		},
		{
			// Nullable:true is required across the WHOLE vocabulary, not
			// just utf8 — an in-vocabulary type is not by itself a pass.
			name: "non-nullable int64 column rejected",
			payload: func(t *testing.T) []byte {
				return buildNonNullableTypedStream(t)
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
