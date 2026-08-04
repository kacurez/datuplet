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
	"blocks/markdown.js",
	"blocks/metric.js",
	"blocks/table.js",
	"blocks/chart.js",
	"theme.css",
	"vegaspec.schema.json",
	"vendor/vega.min.js",
	"vendor/vega-lite.min.js",
	"vendor/vega-embed.min.js",
	"vendor/purify.min.js",
	"vendor/marked.min.js",
}

// shellRendererJS is every platform-owned JS module that reaches a viewer's
// browser (the boot loader + the four V1 block renderers). The §6.4 source-level
// security guards below run across ALL of them, not just shell.js.
var shellRendererJS = []string{
	"shell.js",
	"blocks/markdown.js",
	"blocks/metric.js",
	"blocks/table.js",
	"blocks/chart.js",
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

// ===========================================================================
// RFC 028 Part 4 (V1): block-renderer source-level security guards.
//
// The §6.4 boundary — "untrusted code, trusted output" — is enforced in the
// browser, so its live behavior (a <script> actually stripped, a chart actually
// mounting) is V4's manual browser checklist. What a Go test CAN assert is what
// the server emits and what is statically true of the shipped JS: no renderer
// assigns innerHTML, the markdown config is the fixed marked→DOMPurify allowlist,
// lazy imports are same-origin, and the chart goes through the locked-down
// chokepoint with the platform theme + client-side subset validation.
// ===========================================================================

// TestShellJS_RegistersAllBlockRenderers proves shell.js wires each V1 renderer
// into RENDERERS by block type — the seam boot()/renderBlocks() dispatch on.
func TestShellJS_RegistersAllBlockRenderers(t *testing.T) {
	got := stripWhitespace(readShellAsset(t, "shell.js"))
	for _, want := range []string{
		`import{renderMarkdown}from"./blocks/markdown.js"`,
		`import{renderMetric}from"./blocks/metric.js"`,
		`import{renderTable}from"./blocks/table.js"`,
		`import{renderChart}from"./blocks/chart.js"`,
		`markdown:renderMarkdown`,
		`metric:renderMetric`,
		`table:renderTable`,
		`chart:renderChart`,
	} {
		if !strings.Contains(got, want) {
			t.Errorf("shell.js does not register a renderer as expected: missing %q", want)
		}
	}
}

// TestShellRenderers_NeverUseInnerHTML extends the V0 shell.js-only guard to
// EVERY shipped renderer module (spec §6.4). This is the whole-shell invariant:
// nothing assigns innerHTML or calls insertAdjacentHTML anywhere — markdown
// enters the DOM only as a DOMPurify-returned sanitized fragment
// (RETURN_DOM_FRAGMENT), everything else via textContent.
func TestShellRenderers_NeverUseInnerHTML(t *testing.T) {
	for _, path := range shellRendererJS {
		src := readShellAsset(t, path)
		if loc := innerHTMLUsage.FindString(src); loc != "" {
			t.Errorf("%s must never use innerHTML/insertAdjacentHTML (spec §6.4) — found %q", path, loc)
		}
		if !strings.Contains(src, "textContent") {
			t.Errorf("%s does not appear to render anything via textContent", path)
		}
	}
}

// dynamicImportSpecifier matches the string argument of a dynamic import()
// call, e.g. import("/apps/-/shell/vendor/vega.min.js").
var dynamicImportSpecifier = regexp.MustCompile(`import\(\s*["']([^"']+)["']\s*\)`)

// TestShellRenderers_LazyImportsAreSameOrigin proves the maintainer's lazy-load
// design keeps V0's same-origin invariant: every dynamic import() in the block
// renderers targets a same-origin /apps/-/shell/vendor/ path — never a CDN or
// any external origin — so the shell CSP (script-src 'self') still holds.
func TestShellRenderers_LazyImportsAreSameOrigin(t *testing.T) {
	total := 0
	for _, path := range []string{"blocks/markdown.js", "blocks/chart.js"} {
		src := readShellAsset(t, path)
		matches := dynamicImportSpecifier.FindAllStringSubmatch(src, -1)
		if len(matches) == 0 {
			t.Errorf("%s has no dynamic import() — the lazy-load design is not in place", path)
		}
		for _, m := range matches {
			total++
			if !strings.HasPrefix(m[1], shellAssetPrefix+"vendor/") {
				t.Errorf("%s dynamic-imports a non-same-origin/-reserved specifier %q (must be under %svendor/)",
					path, m[1], shellAssetPrefix)
			}
		}
	}
	// markdown (marked + purify) + chart (vega trio) = 5 lazy imports; a
	// vacuous pass (zero found) would defeat the point.
	if total < 5 {
		t.Errorf("found only %d lazy vendor imports across the renderers, want >= 5", total)
	}
}

// TestIndexHTML_DoesNotEagerLoadVendorLibs proves the shell page no longer
// eager-loads any vendored library (RFC 028 V1 maintainer decision): the only
// <script src> is the boot module; the vega trio and marked/DOMPurify are
// lazy-imported by the renderers instead.
func TestIndexHTML_DoesNotEagerLoadVendorLibs(t *testing.T) {
	idx := string(appshell.IndexHTML)
	scriptSrc := regexp.MustCompile(`<script[^>]*\bsrc="([^"]+)"`)
	matches := scriptSrc.FindAllStringSubmatch(idx, -1)
	if len(matches) == 0 {
		t.Fatal("index.html has no <script src> at all — expected the boot module")
	}
	for _, m := range matches {
		if strings.Contains(m[1], "/vendor/") {
			t.Errorf("index.html eager-loads a vendored library %q — V1 lazy-imports them instead", m[1])
		}
	}
}

// TestMarkdownRenderer_MarkedThenDOMPurifyFixedAllowlist verifies the markdown
// block is parsed by marked then sanitized by DOMPurify with the FIXED §6.4
// allowlist: no raw HTML pass-through (script/iframe never allow-listed), no
// `style` attribute, link schemes limited to http/https/mailto, and every link
// stamped rel="noopener nofollow".
func TestMarkdownRenderer_MarkedThenDOMPurifyFixedAllowlist(t *testing.T) {
	src := readShellAsset(t, "blocks/markdown.js")
	stripped := stripWhitespace(src)
	for _, want := range []string{"marked.parse(", "DOMPurify.sanitize(", "ALLOWED_TAGS", "ALLOWED_ATTR", "RETURN_DOM_FRAGMENT"} {
		if !strings.Contains(src, want) {
			t.Errorf("markdown.js missing %q — sanitizer pipeline incomplete", want)
		}
	}
	if !strings.Contains(src, `"noopener nofollow"`) {
		t.Error("markdown.js does not stamp links with rel=\"noopener nofollow\" (spec §6.4)")
	}
	// Scheme allowlist http/https/mailto (the ALLOWED_URI_REGEXP).
	if !strings.Contains(stripped, "https?|mailto") {
		t.Error("markdown.js does not restrict link schemes to http/https/mailto (spec §6.4)")
	}
	// No `style` attribute (explicit forbid, belt-and-braces atop the allowlist).
	if !strings.Contains(stripped, `FORBID_ATTR:["style"]`) {
		t.Error("markdown.js does not forbid the style attribute (spec §6.4)")
	}
	// The ALLOWED_TAGS / ALLOWED_ATTR allowlists must NOT admit dangerous
	// tags/attrs — their absence IS the "no raw HTML pass-through" guarantee.
	allowedTags := arrayLiteral(t, src, "ALLOWED_TAGS")
	for _, tag := range []string{"script", "iframe", "style", "object", "svg", "form", "input"} {
		if strings.Contains(allowedTags, `"`+tag+`"`) {
			t.Errorf("markdown.js allow-lists <%s> in ALLOWED_TAGS — must not (spec §6.4)", tag)
		}
	}
	allowedAttr := arrayLiteral(t, src, "ALLOWED_ATTR")
	for _, attr := range []string{"style", "onerror", "onload", "src", "srcset"} {
		if strings.Contains(allowedAttr, `"`+attr+`"`) {
			t.Errorf("markdown.js allow-lists the %q attribute — must not (spec §6.4)", attr)
		}
	}
}

// arrayLiteral extracts the `[...]` body of a `const NAME = [ … ]` array in JS
// source, failing the test if it is absent.
func arrayLiteral(t *testing.T, src, name string) string {
	t.Helper()
	m := regexp.MustCompile(name + `\s*=\s*\[([^\]]*)\]`).FindStringSubmatch(src)
	if m == nil {
		t.Fatalf("no %s array literal found", name)
	}
	return m[1]
}

// TestMetricRenderer_FormatsWithIntl verifies metric values are formatted with
// Intl.NumberFormat per the format vocabulary and rendered via textContent.
func TestMetricRenderer_FormatsWithIntl(t *testing.T) {
	src := readShellAsset(t, "blocks/metric.js")
	stripped := stripWhitespace(src)
	if !strings.Contains(src, "Intl.NumberFormat") {
		t.Error("metric.js does not format values with Intl.NumberFormat")
	}
	if !strings.Contains(stripped, `style:"currency"`) {
		t.Error("metric.js does not handle the currency format")
	}
	if !strings.Contains(src, "textContent") || innerHTMLUsage.MatchString(src) {
		t.Error("metric.js must render values/labels via textContent, never innerHTML")
	}
}

// TestTableRenderer_SortSearchNumeric verifies the table is sortable and
// searchable, right-aligns numeric columns with tabular figures, renders cells
// via textContent, and handles BOTH W1 row shapes (plain array + {cells}).
func TestTableRenderer_SortSearchNumeric(t *testing.T) {
	src := readShellAsset(t, "blocks/table.js")
	stripped := stripWhitespace(src)
	for _, want := range []string{
		`type="search"`,            // search/filter box
		`addEventListener("input"`, // filter on input
		`addEventListener("click"`, // sortable headers
		"dtp-num",                  // numeric right-align + tabular-nums class
		"cells",                    // object-row form {cells, modal?}
	} {
		if !strings.Contains(stripped, stripWhitespace(want)) {
			t.Errorf("table.js missing %q", want)
		}
	}
	if !strings.Contains(src, "textContent") {
		t.Error("table.js must render headers/cells via textContent")
	}
}

// TestChartRenderer_ChokepointThemeAndValidation verifies the chart renderer
// (a) routes through shell.js's mountVegaLiteChart rather than touching
// vega-embed directly, (b) supplies the platform THEME_CONFIG (ui/product
// palette + theme.css CSS vars), and (c) validates the spec client-side against
// the vendored subset schema before embedding (spec §6.4 defense-in-depth).
func TestChartRenderer_ChokepointThemeAndValidation(t *testing.T) {
	src := readShellAsset(t, "blocks/chart.js")
	stripped := stripWhitespace(src)

	// (a) chokepoint only — imports mountVegaLiteChart, and never names vegaEmbed.
	if !strings.Contains(stripped, `import{mountVegaLiteChart}from"../shell.js"`) {
		t.Error("chart.js does not import mountVegaLiteChart from shell.js")
	}
	if strings.Contains(src, "vegaEmbed") {
		t.Error("chart.js references vega-embed directly — it must go through shell.js's mountVegaLiteChart chokepoint")
	}
	// (b) platform theme applied at the chokepoint with the ui/product palette
	// and theme.css CSS variables.
	if !strings.Contains(stripped, "mountVegaLiteChart(mount,spec,buildThemeConfig())") {
		t.Error("chart.js does not embed with the platform theme config (buildThemeConfig)")
	}
	if !strings.Contains(src, "#3ecf8e") {
		t.Error("chart.js theme does not use the ui/product palette (accent #3ecf8e)")
	}
	if !strings.Contains(src, "--dtp-fg") {
		t.Error("chart.js theme does not read the shell's theme.css CSS variables")
	}
	// (c) client-side subset validation against the vendored schema.
	for _, want := range []string{"vegaspec.schema.json", "validateAgainstSchema"} {
		if !strings.Contains(src, want) {
			t.Errorf("chart.js missing client-side subset validation: %q", want)
		}
	}
}

// serverAllBlockTypesBundle renders an OutputDoc exercising all four V1 block
// types, with an HTML-hostile title. Used by TestShell_EmbedsAllV1BlockTypes.
const serverAllBlockTypesBundle = `var __dtp_app = { render: () => ({
	outputDoc: 1,
	title: "Sales <script>evil</script>",
	blocks: [
		{ id: "md", type: "markdown", text: "# hi\n[x](https://example.com)" },
		{ id: "kpi", type: "metric", items: [{ label: "Revenue", value: 1234.5, format: "currency:EUR" }] },
		{ id: "tbl", type: "table", columns: ["Product", "Revenue"], rows: [["A", 10], ["B", 20]] },
		{ id: "cht", type: "chart", library: "vega-lite",
		  spec: { mark: "bar", data: { values: [{ a: 1 }] }, encoding: { x: { field: "a", type: "quantitative" } } } }
	]
}) };`

// TestShell_EmbedsAllV1BlockTypes is the server-emits golden: a doc with all
// four V1 block types round-trips through the shell's JSON island intact, and an
// HTML-hostile title is neutralized (never appears as live markup) — the title
// is a text field (spec §6.4), escaped server-side before the browser renders
// it via textContent.
func TestShell_EmbedsAllV1BlockTypes(t *testing.T) {
	h := newServerHarness(t)
	h.api.bundle = []byte(serverAllBlockTypesBundle)

	body := readBody(t, h.bearerGet(h.url(""), "")) // navigation → shell HTML

	// The hostile title never appears as a live <script> element anywhere.
	if strings.Contains(body, "<script>evil") {
		t.Fatalf("hostile doc title broke out as live markup:\n%s", body)
	}

	doc := scriptDoc(t, body) // truncated/broken JSON would fail here
	if doc["title"] != "Sales <script>evil</script>" {
		t.Errorf("title did not round-trip through the island: %v", doc["title"])
	}
	blocks, _ := doc["blocks"].([]any)
	if len(blocks) != 4 {
		t.Fatalf("embedded doc has %d blocks, want 4", len(blocks))
	}
	wantType := map[string]string{"md": "markdown", "kpi": "metric", "tbl": "table", "cht": "chart"}
	for _, raw := range blocks {
		b, _ := raw.(map[string]any)
		id, _ := b["id"].(string)
		if want, ok := wantType[id]; !ok || b["type"] != want {
			t.Errorf("block %q has type %v, want %v", id, b["type"], want)
		}
	}
}
