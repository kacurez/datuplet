//go:build duckdb_arrow

package queryengine

import (
	"bytes"
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

const variantFixtureJSON = `{"s":"he said \"hi\"","b":true,"big":9007199254740993,"n":{"arr":["x",false,18446744073709551615],"s":"it's"}}`

func decodeJSONExact(t *testing.T, text string) (any, error) {
	t.Helper()
	dec := json.NewDecoder(bytes.NewReader([]byte(text)))
	dec.UseNumber()
	var v any
	err := dec.Decode(&v)
	return v, err
}

// TestPreviewProjectsVariantAsJSONText runs the projection the storage preview
// generates for a VARIANT column (pkg/pipelineapi/queryproxy/preview.go) and
// checks the value round-trips as real JSON text.
func TestPreviewProjectsVariantAsJSONText(t *testing.T) {
	want, err := decodeJSONExact(t, variantFixtureJSON)
	if err != nil {
		t.Fatalf("fixture: %v", err)
	}
	setup := "CREATE TEMP TABLE t AS SELECT 1 AS id, '" + strings.ReplaceAll(variantFixtureJSON, "'", "''") + "'::JSON::VARIANT AS v; "

	res, err := Run(context.Background(), Request{
		SQL:     setup + `SELECT * REPLACE ("v"::JSON::VARCHAR AS "v") FROM t LIMIT 100`,
		TempDir: t.TempDir(),
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(res.Schema) != 2 || res.Schema[0].Name != "id" || res.Schema[1].Name != "v" || res.Schema[1].Type != "VARCHAR" {
		t.Fatalf("schema = %+v, want [id, v VARCHAR]", res.Schema)
	}
	text, ok := res.Rows[0][1].(string)
	if !ok {
		t.Fatalf("v = %#v, want string", res.Rows[0][1])
	}
	got, err := decodeJSONExact(t, text)
	if err != nil {
		t.Fatalf("v is not JSON text: %v (%q)", err, text)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("v = %#v, want %#v", got, want)
	}

	// Negative control: a direct ::VARCHAR yields DuckDB display text.
	res, err = Run(context.Background(), Request{
		SQL:     setup + `SELECT "v"::VARCHAR AS v FROM t`,
		TempDir: t.TempDir(),
	})
	if err != nil {
		t.Fatalf("Run (control): %v", err)
	}
	display, ok := res.Rows[0][0].(string)
	if !ok {
		t.Fatalf("direct ::VARCHAR = %T %v, want string", res.Rows[0][0], res.Rows[0][0])
	}
	if got, err := decodeJSONExact(t, display); err == nil && reflect.DeepEqual(got, want) {
		t.Fatalf("direct ::VARCHAR unexpectedly produced equal JSON: %q", display)
	}
}

func TestQueryVariantResultClearError(t *testing.T) {
	_, err := Run(context.Background(), Request{
		SQL:     `SELECT 1 AS id, '{"a":1}'::JSON::VARIANT AS payload`,
		TempDir: t.TempDir(),
	})
	if err == nil {
		t.Fatal("Run should reject a VARIANT result column")
	}
	for _, s := range []string{`"payload"`, "VARIANT", `payload::JSON::VARCHAR`} {
		if !strings.Contains(err.Error(), s) {
			t.Errorf("error %q should contain %q", err, s)
		}
	}
}

func TestQueryJSONResultClearError(t *testing.T) {
	_, err := Run(context.Background(), Request{
		SQL:     `SELECT '{"a":1}'::JSON AS doc`,
		TempDir: t.TempDir(),
	})
	if err == nil {
		t.Fatal("Run should reject a JSON result column")
	}
	for _, s := range []string{`"doc"`, "JSON", `doc::VARCHAR`} {
		if !strings.Contains(err.Error(), s) {
			t.Errorf("error %q should contain %q", err, s)
		}
	}
	if strings.Contains(err.Error(), "::JSON::VARCHAR") {
		t.Errorf("JSON error should suggest ::VARCHAR, not ::JSON::VARCHAR: %q", err)
	}
}
