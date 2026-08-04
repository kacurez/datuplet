package appworker

// shell_test.go — RFC 028 Part 4 (V0): the shell skeleton's Go-testable
// surface — the embedded static asset routes, the same-origin/CSP-compliance
// check on the rendered shell HTML, and source-level guards on shell.js for
// the §6.4 security requirements a headless Go test CAN verify without a
// browser (dynamic DOM behavior is V4's manual checklist, not here).

import (
	"io"
	"net/http"
	"regexp"
	"strings"
	"testing"

	"github.com/datuplet/datuplet/ui/appshell"
)

// shellAssetPaths enumerates every file V0 mounts at shellAssetPrefix — the
// fixture both the serving test and the byte-equality assertion are driven
// from. Deliberately excludes index.html (never served verbatim; see
// appshell.IndexHTML's doc comment).
var shellAssetPaths = []string{
	"shell.js",
	"theme.css",
	"vegaspec.schema.json",
	"vendor/vega.min.js",
	"vendor/vega-lite.min.js",
	"vendor/vega-embed.min.js",
	"vendor/purify.min.js",
	"vendor/marked.min.js",
}

// TestShellAssets_ServedAtReservedPath proves every embedded shell asset is
// reachable at /apps/-/shell/<path> with byte-identical content, and that
// serving it does NOT go through handleApp's render pipeline. "-" is a
// reserved pid sentinel (never a real lakekeeper project id) that lets this
// literal route out-specificity the /apps/{pid}/{name}/{path...} wildcard
// (Go 1.22 ServeMux: a literal segment beats a wildcard at the same
// position) — this test's resolveCalls assertion is what proves that
// precedence actually holds at runtime, not just in theory.
func TestShellAssets_ServedAtReservedPath(t *testing.T) {
	h := newServerHarness(t)
	before := h.api.resolveCalls

	for _, p := range shellAssetPaths {
		t.Run(p, func(t *testing.T) {
			want, err := appshell.Assets.ReadFile(p)
			if err != nil {
				t.Fatalf("fixture missing from appshell.Assets: %v", err)
			}
			resp, err := h.client().Get(h.ts.URL + shellAssetPrefix + p)
			if err != nil {
				t.Fatalf("GET: %v", err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("status = %d, want 200", resp.StatusCode)
			}
			got, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatalf("read body: %v", err)
			}
			if string(got) != string(want) {
				t.Errorf("served bytes for %q differ from appshell.Assets", p)
			}
		})
	}

	if h.api.resolveCalls != before {
		t.Errorf("resolveCalls = %d, want %d — shell assets must never go through handleApp/resolveApp",
			h.api.resolveCalls, before)
	}
}

// TestShellAssets_DirectoryRequestsAreNotFound guards against the embedded
// FS (which has no index.html of its own — see appshell.IndexHTML) falling
// through to net/http's automatic directory-listing behavior.
func TestShellAssets_DirectoryRequestsAreNotFound(t *testing.T) {
	h := newServerHarness(t)
	for _, p := range []string{shellAssetPrefix, shellAssetPrefix + "vendor/"} {
		resp, err := h.client().Get(h.ts.URL + p)
		if err != nil {
			t.Fatalf("GET %s: %v", p, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusNotFound {
			t.Errorf("GET %s status = %d, want 404 (no directory listing)", p, resp.StatusCode)
		}
	}
}

// srcOrHrefAttr matches src="..." / href="..." attribute values anywhere in
// an HTML document — used to prove the rendered shell references only
// same-origin assets (spec §6.4 CSP: script-src/style-src 'self').
var srcOrHrefAttr = regexp.MustCompile(`(?:src|href)="([^"]*)"`)

// TestShell_ReferencesOnlySameOriginAssets is the CSP-compliance guard the
// brief calls for: every src=/href= in the rendered shell HTML must be a
// same-origin, root-relative path under the reserved shell prefix — no CDN,
// no external fonts/scripts, no protocol-relative or absolute URL.
func TestShell_ReferencesOnlySameOriginAssets(t *testing.T) {
	h := newServerHarness(t)
	body := readBody(t, h.bearerGet(h.url(""), "")) // navigation -> shell HTML

	matches := srcOrHrefAttr.FindAllStringSubmatch(body, -1)
	if len(matches) == 0 {
		t.Fatal("no src=/href= attributes found in the shell response — test fixture is broken")
	}
	for _, m := range matches {
		ref := m[1]
		if !strings.HasPrefix(ref, shellAssetPrefix) {
			t.Errorf("shell references a non-same-origin (or non-reserved-prefix) asset %q", ref)
		}
	}
}

// innerHTMLUsage matches actual innerHTML ASSIGNMENT or insertAdjacentHTML
// CALL syntax — not the bare word, which this file's own doc comments use
// (correctly) when explaining the rule those comments enforce. A prose
// mention like "never innerHTML" must not trip this check; `el.innerHTML =`
// or `insertAdjacentHTML(` must.
var innerHTMLUsage = regexp.MustCompile(`\.innerHTML\s*=|insertAdjacentHTML\s*\(`)

// TestShellJS_NeverUsesInnerHTML enforces spec §6.4 at the source level:
// every text field the shell renders — the title in V0, block content in
// V1 — MUST go through textContent, never innerHTML/insertAdjacentHTML,
// because innerHTML on app-controlled text is the XSS hole the whole
// "untrusted code, trusted output" boundary exists to close.
func TestShellJS_NeverUsesInnerHTML(t *testing.T) {
	src := readShellAsset(t, "shell.js")
	if loc := innerHTMLUsage.FindString(src); loc != "" {
		t.Errorf("shell.js must never use innerHTML/insertAdjacentHTML (spec §6.4) — use textContent instead; found %q", loc)
	}
	if !strings.Contains(src, "textContent") {
		t.Error("shell.js does not appear to render anything via textContent")
	}
}

// TestShellJS_VegaEmbedNetworkLoadingDisabled proves shell.js's vega-embed
// entry point matches the brief's required lockdown (content, not
// formatting): actions disabled and the loader rejecting every load — even
// though the vegaspec subset already forbids data.url, this is
// defense-in-depth (spec §6.4).
func TestShellJS_VegaEmbedNetworkLoadingDisabled(t *testing.T) {
	src := readShellAsset(t, "shell.js")
	got := stripWhitespace(src)
	want := stripWhitespace(`actions:false,loader:{load:()=>Promise.reject(new Error("loading disabled"))}`)
	if !strings.Contains(got, want) {
		t.Errorf("shell.js does not initialize vega-embed with network loading disabled:\n%s", src)
	}
}

// TestShellJS_HasRenderersRegistryAndUnknownPlaceholder proves V0 built the
// extensibility seam V1 needs (a RENDERERS registry keyed by block type) and
// the required fallback for any type not yet registered.
func TestShellJS_HasRenderersRegistryAndUnknownPlaceholder(t *testing.T) {
	src := readShellAsset(t, "shell.js")
	for _, want := range []string{"RENDERERS", "unknown block"} {
		if !strings.Contains(src, want) {
			t.Errorf("shell.js missing %q", want)
		}
	}
}

// readShellAsset reads a file out of the embedded shell asset FS, failing
// the test rather than returning an error: every caller treats a missing
// fixture as a broken test/build, not a case to handle.
func readShellAsset(t *testing.T, path string) string {
	t.Helper()
	b, err := appshell.Assets.ReadFile(path)
	if err != nil {
		t.Fatalf("appshell.Assets.ReadFile(%q): %v", path, err)
	}
	return string(b)
}

// stripWhitespace removes every whitespace rune, for a formatting-insensitive
// (but content-exact) substring comparison against hand-written JS.
func stripWhitespace(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for _, r := range s {
		switch r {
		case ' ', '\t', '\n', '\r':
			continue
		default:
			b.WriteRune(r)
		}
	}
	return b.String()
}
