package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

const testProjectID = "11111111-1111-1111-1111-111111111111"

func TestResolveQuerySQL(t *testing.T) {
	cases := []struct {
		name      string
		sqlFlag   string
		fileBody  *string // nil = no -f; non-nil = -f with this content
		stdin     string
		want      string
		wantErr   bool
		errSubstr string
	}{
		{name: "sql flag wins", sqlFlag: "SELECT 1", fileBody: ptr("SELECT 2"), stdin: "SELECT 3", want: "SELECT 1"},
		{name: "file when no sql", fileBody: ptr("SELECT 2\n"), stdin: "SELECT 3", want: "SELECT 2\n"},
		{name: "stdin when no sql no file", stdin: "SELECT 3\n", want: "SELECT 3\n"},
		{name: "empty everything errors", wantErr: true, errSubstr: "no SQL"},
		{name: "whitespace-only sql flag falls through to file", sqlFlag: "   ", fileBody: ptr("SELECT 2"), want: "SELECT 2"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var filePath string
			if tc.fileBody != nil {
				filePath = writeTempFile(t, *tc.fileBody)
			}
			got, err := resolveQuerySQL(tc.sqlFlag, filePath, strings.NewReader(tc.stdin))
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error")
				}
				if tc.errSubstr != "" && !strings.Contains(err.Error(), tc.errSubstr) {
					t.Fatalf("error %q does not contain %q", err.Error(), tc.errSubstr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Fatalf("resolveQuerySQL = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestServerQueryRoute(t *testing.T) {
	// Stub pipeline-api: assert the POST shape, return a queryengine.Result body.
	var gotPath, gotAuth, gotSQL string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotAuth = r.Header.Get("Authorization")
		var body struct {
			SQL string `json:"sql"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		gotSQL = body.SQL
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"schema":[{"name":"n","type":"BIGINT"}],"rows":[[42]],"truncated":false,"stats":{"duration_ms":5}}`)
	}))
	defer srv.Close()

	var out bytes.Buffer
	err := serverQuery(srv.URL, "api-token-xyz", testProjectID, "SELECT count(*) FROM t", "json", &out)
	if err != nil {
		t.Fatalf("serverQuery: %v", err)
	}
	wantPath := "/api/v1/projects/" + testProjectID + "/query"
	if gotPath != wantPath {
		t.Fatalf("path = %q, want %q", gotPath, wantPath)
	}
	if gotAuth != "Bearer api-token-xyz" {
		t.Fatalf("auth = %q, want Bearer api-token-xyz", gotAuth)
	}
	if gotSQL != "SELECT count(*) FROM t" {
		t.Fatalf("forwarded sql = %q", gotSQL)
	}
	if !strings.Contains(out.String(), "42") {
		t.Fatalf("output missing row value: %q", out.String())
	}
}

func TestServerQuery_ErrorStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = io.WriteString(w, `{"error":"Binder Error: no such column","kind":"sql_error"}`)
	}))
	defer srv.Close()

	var out bytes.Buffer
	err := serverQuery(srv.URL, "tok", testProjectID, "SELECT bad", "json", &out)
	if err == nil {
		t.Fatalf("expected error on HTTP 400")
	}
	// Require the server's actual error MESSAGE to be surfaced verbatim — not
	// merely the `kind`. An implementation that dropped the message (echoing
	// only kind=sql_error) would be a real regression this must catch.
	if !strings.Contains(err.Error(), "Binder Error: no such column") {
		t.Fatalf("error should surface the server's message verbatim, got: %v", err)
	}
}

func TestLocalQueryMessage(t *testing.T) {
	var out bytes.Buffer
	err := localQueryNotAvailable(&out)
	if err == nil {
		t.Fatalf("--local must error: root datuplet is duckdb-free")
	}
	msg := err.Error()
	if !strings.Contains(msg, "datuplet-query") {
		t.Fatalf("message must name the datuplet-query binary, got: %q", msg)
	}
}

func ptr(s string) *string { return &s }

func writeTempFile(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	p := dir + "/q.sql"
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		t.Fatalf("write temp file: %v", err)
	}
	return p
}
