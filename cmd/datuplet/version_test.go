package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func versionServer(t *testing.T, version string) *httptest.Server {
	t.Helper()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/version" {
			http.NotFound(w, r)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]string{"version": version, "commit": "abc"})
	}))
	t.Cleanup(ts.Close)
	return ts
}

func TestVersionJSONWithoutRemote(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	t.Setenv("DATUPLET_REMOTE", "")
	var out, errb bytes.Buffer
	if code := runVersion([]string{"--json"}, &out, &errb); code != 0 {
		t.Fatalf("exit = %d", code)
	}
	var got map[string]any
	if err := json.Unmarshal(out.Bytes(), &got); err != nil {
		t.Fatalf("bad json %q: %v", out.String(), err)
	}
	if _, ok := got["server"]; ok {
		t.Errorf("server present without remote: %v", got)
	}
	if got["minor_mismatch"] != false {
		t.Errorf("minor_mismatch = %v", got["minor_mismatch"])
	}
	if got["client"].(map[string]any)["version"] == "" {
		t.Errorf("client.version empty")
	}
	if errb.Len() != 0 {
		t.Errorf("stderr = %q", errb.String())
	}
}

func TestVersionJSONMinorMismatchFlag(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	ts := versionServer(t, "0.99.0")
	prev := clientVersion
	clientVersion = func() (string, string, string) { return "0.13.3", "c", "d" }
	defer func() { clientVersion = prev }()
	var out, errb bytes.Buffer
	if code := runVersion([]string{"--json", "--remote", ts.URL}, &out, &errb); code != 0 {
		t.Fatalf("exit = %d", code)
	}
	var got struct {
		Server        struct{ Version string } `json:"server"`
		MinorMismatch bool                     `json:"minor_mismatch"`
	}
	if err := json.Unmarshal(out.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.Server.Version != "0.99.0" || !got.MinorMismatch {
		t.Errorf("got %+v", got)
	}
	if errb.Len() != 0 {
		t.Errorf("stderr must be empty under --json, got %q", errb.String())
	}
}

func TestVersionHumanWarns(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	t.Setenv("DATUPLET_REMOTE", "")
	ts := versionServer(t, "0.99.0")
	t.Setenv("DATUPLET_REMOTE", ts.URL)
	// Force a parseable client version so the mismatch is deterministic.
	prev := clientVersion
	clientVersion = func() (string, string, string) { return "0.13.3", "c", "d" }
	defer func() { clientVersion = prev }()

	var out, errb bytes.Buffer
	if code := runVersion(nil, &out, &errb); code != 0 {
		t.Fatalf("exit = %d", code)
	}
	if !strings.Contains(errb.String(), "minor versions differ") {
		t.Errorf("stderr = %q", errb.String())
	}
	if !strings.Contains(out.String(), "0.99.0") || !strings.Contains(out.String(), "0.13.3") {
		t.Errorf("stdout = %q", out.String())
	}
}
