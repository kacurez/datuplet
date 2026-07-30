package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestParseNsTable(t *testing.T) {
	cases := []struct {
		in      string
		ns, tbl string
		wantErr bool
	}{
		{"raw.products", "raw", "products", false},
		{"my_ns.my_table", "my_ns", "my_table", false},
		{"noseparator", "", "", true},
		{"too.many.dots", "", "", true},
		{".missingNs", "", "", true},
		{"missingTbl.", "", "", true},
	}
	for _, tc := range cases {
		ns, tbl, err := parseNsTable(tc.in)
		if tc.wantErr {
			if err == nil {
				t.Errorf("parseNsTable(%q) expected error", tc.in)
			}
			continue
		}
		if err != nil {
			t.Errorf("parseNsTable(%q) unexpected error: %v", tc.in, err)
		}
		if ns != tc.ns || tbl != tc.tbl {
			t.Errorf("parseNsTable(%q) = (%q,%q), want (%q,%q)", tc.in, ns, tbl, tc.ns, tc.tbl)
		}
	}
}

// confirmTableDeletion is the CLI's only guard against destroying the wrong
// table, so it must accept ONLY an exact retype of the reference.
func TestConfirmTableDeletion(t *testing.T) {
	const ref = "raw.gbif_occurrences_sk"
	tests := []struct {
		name    string
		confirm string
		wantErr bool
	}{
		{"exact match proceeds", ref, false},
		{"empty is refused", "", true},
		{"different table is refused", "raw.other_table", true},
		{"table name alone is not enough", "gbif_occurrences_sk", true},
		{"namespace alone is not enough", "raw", true},
		{"case difference is refused", "RAW.GBIF_OCCURRENCES_SK", true},
		{"trailing space is refused", ref + " ", true},
		{"boolean-style yes is refused", "yes", true},
		{"boolean-style true is refused", "true", true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := confirmTableDeletion(ref, tc.confirm)
			if tc.wantErr && err == nil {
				t.Fatalf("confirmTableDeletion(%q, %q) = nil, want an error", ref, tc.confirm)
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("confirmTableDeletion(%q, %q) = %v, want nil", ref, tc.confirm, err)
			}
			// The refusal must tell the operator exactly what to type.
			if tc.wantErr && !strings.Contains(err.Error(), ref) {
				t.Errorf("refusal should name the required confirmation value; got: %v", err)
			}
		})
	}
}

// An unconfirmed delete must fail before ANY network call or credential load.
// The remote here is a closed port: if the guard ran late, this would surface
// as a connection error instead of the confirmation refusal.
func TestRunStorageDelete_UnconfirmedNeverReachesNetwork(t *testing.T) {
	err := runStorageDelete("http://127.0.0.1:1", "", "", "raw.t", "")
	if err == nil {
		t.Fatal("runStorageDelete with no --confirm = nil, want a refusal")
	}
	if !strings.Contains(err.Error(), "refusing to delete") {
		t.Fatalf("want the confirmation refusal (proving the guard runs before any I/O), got: %v", err)
	}
}

// A malformed reference must be rejected before the confirmation comparison,
// so "--confirm garbage" matching a garbage ref still cannot delete anything.
func TestRunStorageDelete_RejectsMalformedRef(t *testing.T) {
	err := runStorageDelete("http://127.0.0.1:1", "", "", "no-dot", "no-dot")
	if err == nil {
		t.Fatal("runStorageDelete with a malformed ref = nil, want an error")
	}
	if !strings.Contains(err.Error(), "invalid <namespace>.<table>") {
		t.Fatalf("want the ref-parse error, got: %v", err)
	}
}

// storageDELETE must use the DELETE verb, hit the /api/v1/storage-prefixed
// path, carry the bearer token, and surface a non-2xx as an error.
func TestStorageDELETE_RequestShapeAndErrors(t *testing.T) {
	var gotMethod, gotPath, gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod, gotPath, gotAuth = r.Method, r.URL.Path, r.Header.Get("Authorization")
		if r.URL.Path == "/api/v1/storage/projects/p/tables/raw/boom" {
			http.Error(w, `{"error":"nope"}`, http.StatusForbidden)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"deleted":true}`))
	}))
	defer srv.Close()

	body, err := storageDELETE(context.Background(), srv.URL, "/projects/p/tables/raw/t", "tok")
	if err != nil {
		t.Fatalf("storageDELETE: %v", err)
	}
	if gotMethod != http.MethodDelete {
		t.Errorf("method = %s, want DELETE", gotMethod)
	}
	if gotPath != "/api/v1/storage/projects/p/tables/raw/t" {
		t.Errorf("path = %s", gotPath)
	}
	if gotAuth != "Bearer tok" {
		t.Errorf("auth = %q, want %q", gotAuth, "Bearer tok")
	}
	if !strings.Contains(string(body), `"deleted":true`) {
		t.Errorf("body = %s", body)
	}

	if _, err := storageDELETE(context.Background(), srv.URL, "/projects/p/tables/raw/boom", "tok"); err == nil {
		t.Fatal("a 403 response must surface as an error, not a silent success")
	}
}
