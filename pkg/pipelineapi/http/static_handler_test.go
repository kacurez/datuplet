package http_test

import (
	"io"
	stdhttp "net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	apihttp "github.com/datuplet/datuplet/pkg/pipelineapi/http"
)

// testUIDir writes a fake ui/product layout into a temp dir so the
// static handler can be exercised without the real checked-in assets.
func testUIDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	must := func(p string, body string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(dir, p), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	must("index.html", "<!doctype html><title>datuplet-test</title>")
	must("app.js", "console.log('hi');")
	return dir
}

func TestUIStatic_ServesIndexAtRoot(t *testing.T) {
	dir := testUIDir(t)
	srv := apihttp.NewServer(nil).WithStaticDir(dir)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	resp, err := stdhttp.Get(ts.URL + "/ui/")
	if err != nil {
		t.Fatalf("GET /ui/: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "datuplet-test") {
		t.Errorf("body = %q, want to contain datuplet-test", body)
	}
}

func TestUIStatic_ServesRealFile(t *testing.T) {
	dir := testUIDir(t)
	srv := apihttp.NewServer(nil).WithStaticDir(dir)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	resp, err := stdhttp.Get(ts.URL + "/ui/app.js")
	if err != nil {
		t.Fatalf("GET /ui/app.js: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "console.log") {
		t.Errorf("body = %q, want app.js contents", body)
	}
	// Must receive a JS MIME type, not HTML — otherwise the SPA
	// fallback accidentally served index.html in place of the asset.
	ct := resp.Header.Get("Content-Type")
	if !strings.HasPrefix(ct, "text/javascript") && !strings.HasPrefix(ct, "application/javascript") {
		t.Errorf("Content-Type = %q, want a JS type", ct)
	}
}

func TestUIStatic_SPAFallbackForDeepLinks(t *testing.T) {
	dir := testUIDir(t)
	srv := apihttp.NewServer(nil).WithStaticDir(dir)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	// /ui/pipelines/foo doesn't exist in the test dir — must fall back
	// to index.html so the client-side router can handle it.
	resp, err := stdhttp.Get(ts.URL + "/ui/pipelines/foo")
	if err != nil {
		t.Fatalf("GET /ui/pipelines/foo: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "datuplet-test") {
		t.Errorf("body = %q, want SPA fallback to index.html", body)
	}
}

func TestUIStatic_RejectsTraversal(t *testing.T) {
	dir := testUIDir(t)
	// Put a fake sensitive file outside the static dir.
	parent := filepath.Dir(dir)
	outside := filepath.Join(parent, "secret.txt")
	_ = os.WriteFile(outside, []byte("sensitive"), 0o644)
	defer os.Remove(outside)

	srv := apihttp.NewServer(nil).WithStaticDir(dir)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	// Raw request — bypass net/url cleanup by using a Client that doesn't
	// normalize. We care that the server's own cleanup rejects traversal.
	req, _ := stdhttp.NewRequest("GET", ts.URL+"/ui/../secret.txt", nil)
	resp, err := stdhttp.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if strings.Contains(string(body), "sensitive") {
		t.Errorf("path traversal leaked %q", body)
	}
}

func TestUIStatic_SiblingDirSharingPrefixIsNotReachable(t *testing.T) {
	// Build two sibling dirs under the same parent:
	//   <tmp>/product   (configured as the static root)
	//   <tmp>/product2  (sibling whose absolute path starts with the
	//                    same prefix string as the root)
	parent := t.TempDir()
	root := filepath.Join(parent, "product")
	sibling := filepath.Join(parent, "product2")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(sibling, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "index.html"), []byte("<!doctype html><title>ok</title>"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sibling, "leak.txt"), []byte("SIBLING LEAK"), 0o644); err != nil {
		t.Fatal(err)
	}

	srv := apihttp.NewServer(nil).WithStaticDir(root)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	// A plain HasPrefix check would let "../product2/leak.txt" resolve
	// to the sibling; the directory-boundary guard must refuse it.
	req, _ := stdhttp.NewRequest("GET", ts.URL+"/ui/../product2/leak.txt", nil)
	resp, err := stdhttp.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if strings.Contains(string(body), "SIBLING LEAK") {
		t.Errorf("sibling leaked via shared-prefix escape: %q", body)
	}
}

func TestUIStatic_DisabledWhenUnset(t *testing.T) {
	srv := apihttp.NewServer(nil)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	resp, err := stdhttp.Get(ts.URL + "/ui/")
	if err != nil {
		t.Fatalf("GET /ui/: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 404 {
		t.Errorf("status = %d, want 404 when staticDir unset", resp.StatusCode)
	}
}
