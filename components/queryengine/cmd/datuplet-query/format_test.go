package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/datuplet/datuplet/components/queryengine"
)

func fixedResult() *queryengine.Result {
	return &queryengine.Result{
		Schema: []queryengine.Column{
			{Name: "id", Type: "BIGINT"},
			{Name: "name", Type: "VARCHAR"},
		},
		Rows: [][]any{
			{int64(1), "alice"},
			{int64(2), nil},
			{int64(30), "bob, jr"},
		},
		Truncated: false,
	}
}

func TestRenderTable(t *testing.T) {
	var buf bytes.Buffer
	if err := render(&buf, fixedResult(), "table"); err != nil {
		t.Fatalf("render table: %v", err)
	}
	out := buf.String()
	// Header column names present.
	if !strings.Contains(out, "id") || !strings.Contains(out, "name") {
		t.Errorf("table missing header: %q", out)
	}
	// Null rendered sensibly (not the literal Go <nil>).
	if !strings.Contains(out, "NULL") {
		t.Errorf("table should render nil as NULL: %q", out)
	}
	if strings.Contains(out, "<nil>") {
		t.Errorf("table should not contain <nil>: %q", out)
	}
	// Values present.
	if !strings.Contains(out, "alice") || !strings.Contains(out, "bob, jr") {
		t.Errorf("table missing values: %q", out)
	}
}

func TestRenderCSV(t *testing.T) {
	var buf bytes.Buffer
	if err := render(&buf, fixedResult(), "csv"); err != nil {
		t.Fatalf("render csv: %v", err)
	}
	out := buf.String()
	lines := strings.Split(strings.TrimRight(out, "\r\n"), "\n")
	if len(lines) != 4 { // header + 3 rows
		t.Fatalf("csv want 4 lines, got %d: %q", len(lines), out)
	}
	if strings.TrimRight(lines[0], "\r") != "id,name" {
		t.Errorf("csv header = %q", lines[0])
	}
	// Null → empty field.
	if strings.TrimRight(lines[2], "\r") != "2," {
		t.Errorf("csv null row = %q, want '2,'", lines[2])
	}
	// Embedded comma → quoted per RFC4180.
	if !strings.Contains(lines[3], `"bob, jr"`) {
		t.Errorf("csv should quote embedded comma: %q", lines[3])
	}
}

func TestRenderJSON(t *testing.T) {
	var buf bytes.Buffer
	if err := render(&buf, fixedResult(), "json"); err != nil {
		t.Fatalf("render json: %v", err)
	}
	var back queryengine.Result
	if err := json.Unmarshal(buf.Bytes(), &back); err != nil {
		t.Fatalf("json output not valid Result JSON: %v\n%s", err, buf.String())
	}
	if len(back.Rows) != 3 || len(back.Schema) != 2 {
		t.Errorf("json round-trip lost data: %+v", back)
	}
}

func TestExitCodeFor(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want int
	}{
		{"success", nil, exitSuccess},
		{"timeout", queryengine.ErrTimeout, exitAppFailure},
		{"wrapped timeout", wrap(queryengine.ErrTimeout), exitAppFailure},
		{"result too large is user error", queryengine.ErrResultTooLarge, exitUserFailure},
		{"generic sql/fga error is user error", errString("Binder Error: no such column"), exitUserFailure},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := exitCodeFor(tc.err); got != tc.want {
				t.Errorf("exitCodeFor(%v) = %d, want %d", tc.err, got, tc.want)
			}
		})
	}
}

type errString string

func (e errString) Error() string { return string(e) }

func wrap(err error) error { return wrapped{err} }

type wrapped struct{ e error }

func (w wrapped) Error() string { return "context: " + w.e.Error() }
func (w wrapped) Unwrap() error { return w.e }
