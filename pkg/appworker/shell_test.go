package appworker

// shell_test.go — RFC 028 Part 4 (V0): the shell skeleton's Go-testable
// surface — the embedded static asset routes, the same-origin/CSP-compliance
// check on the rendered shell HTML, and source-level guards on shell.js for
// the §6.4 security requirements a headless Go test CAN verify without a
// browser (dynamic DOM behavior is V4's manual checklist, not here).

import (
	"io"
	"net/http"
	"reflect"
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
	"interact.js",
	"csv.js",
	"blocks/markdown.js",
	"blocks/metric.js",
	"blocks/table.js",
	"blocks/chart.js",
	"blocks/filter.js",
	"blocks/tabs.js",
	"theme.css",
	"vegaspec.schema.json",
	"vendor/vega.min.js",
	"vendor/vega-lite.min.js",
	"vendor/vega-embed.min.js",
	"vendor/purify.min.js",
	"vendor/marked.min.js",
}

// shellRendererJS is every platform-owned JS module that reaches a viewer's
// browser (the boot loader, the interactivity hub, and the six block
// renderers). The §6.4 source-level security guards below run across ALL of
// them, not just shell.js.
var shellRendererJS = []string{
	"shell.js",
	"interact.js",
	"blocks/markdown.js",
	"blocks/metric.js",
	"blocks/table.js",
	"blocks/chart.js",
	"blocks/filter.js",
	"blocks/tabs.js",
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

// TestShellJS_RegistersAllBlockRenderers proves shell.js wires every renderer
// of the v1 vocabulary into RENDERERS by block type — the seam
// boot()/renderBlocks() dispatch on. V2 adds filter + tabs to the four V1
// data-block renderers, so all six block types are now registered.
func TestShellJS_RegistersAllBlockRenderers(t *testing.T) {
	got := stripWhitespace(readShellAsset(t, "shell.js"))
	for _, want := range []string{
		`import{renderMarkdown}from"./blocks/markdown.js"`,
		`import{renderMetric}from"./blocks/metric.js"`,
		`import{renderTable}from"./blocks/table.js"`,
		`import{renderChart}from"./blocks/chart.js"`,
		`import{renderFilter}from"./blocks/filter.js"`,
		`import{renderTabs}from"./blocks/tabs.js"`,
		`markdown:renderMarkdown`,
		`metric:renderMetric`,
		`table:renderTable`,
		`chart:renderChart`,
		`filter:renderFilter`,
		`tabs:renderTabs`,
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

// ===========================================================================
// RFC 028 Part 4 (V2): interactivity source-level guards.
//
// The live behavior — a filter change re-rendering, a modal opening, the tab
// hidden pausing auto-refresh — is V4's manual browser checklist (no headless
// harness; no build step). What a Go test CAN assert is what is statically true
// of the shipped JS: the re-render fetch carries the exact CSRF/Accept/body
// contract, every interaction fetch is same-origin, params are string-only, the
// scheduler honors visibility + Retry-After and never drops below the 15s clamp,
// onClick binds to the vega view, and modal content renders through the SAME
// renderer registry (no second, unsanitized path).
// ===========================================================================

// TestInteract_RerenderPostContract proves the re-render fetch is the exact
// POST the server's response matrix (§4.2) + CSRF check (§5.3) require: method
// POST, Accept: application/json, Content-Type: application/json, the shell's
// X-Datuplet-App-Render: 1 header, and a body that is the current param state.
func TestInteract_RerenderPostContract(t *testing.T) {
	got := stripWhitespace(readShellAsset(t, "interact.js"))
	for _, want := range []string{
		`method:"POST"`,
		`Accept:"application/json"`,
		`"Content-Type":"application/json"`,
		`"X-Datuplet-App-Render":"1"`,
		`body:JSON.stringify(getParams())`,
	} {
		if !strings.Contains(got, want) {
			t.Errorf("interact.js re-render fetch is missing the required contract token %q", want)
		}
	}
	// The single-block partial fetch (lazy modals) rides the query string as
	// `?block=<id>` (matches the §4.2 matrix), not a body key.
	if !strings.Contains(got, `"?block="`) {
		t.Error("interact.js does not select a single block via ?block= for partial (modal) fetches")
	}
}

// TestInteract_FetchesAreSameOrigin proves no interaction module reaches a new
// origin (spec §6.4 CSP connect-src 'self'): no scheme/authority URL appears in
// any of them, and interact.js — the only one that fetches — builds its request
// URL from location.pathname (same-origin, path-relative).
func TestInteract_FetchesAreSameOrigin(t *testing.T) {
	for _, path := range []string{"interact.js", "blocks/filter.js", "blocks/tabs.js"} {
		src := readShellAsset(t, path)
		if strings.Contains(src, "://") {
			t.Errorf("%s contains a scheme/authority URL (\"://\") — every interaction fetch must be same-origin/path-relative", path)
		}
	}
	interact := readShellAsset(t, "interact.js")
	if !strings.Contains(interact, "fetch(") {
		t.Fatal("interact.js has no fetch() — the re-render path is missing")
	}
	if !strings.Contains(interact, "location.pathname") {
		t.Error("interact.js does not build its fetch URL from location.pathname (same-origin)")
	}
}

// TestInteract_ParamsAreStringOnly proves the re-render body can only ever carry
// string values (§6.5 "no type coercion — apps parse their own numbers"; W6
// REJECTS non-string body values): params come from URLSearchParams (whose
// values are strings) and the body is JSON.stringify of exactly that map.
func TestInteract_ParamsAreStringOnly(t *testing.T) {
	got := stripWhitespace(readShellAsset(t, "interact.js"))
	if !strings.Contains(got, "newURLSearchParams(location.search)") {
		t.Error("interact.js getParams does not read params from URLSearchParams (string values by construction)")
	}
	if !strings.Contains(got, `body:JSON.stringify(getParams())`) {
		t.Error("interact.js re-render body is not JSON.stringify(getParams()) — string-only guarantee lost")
	}
	// The reserved platform names are stripped from ctx.params on the way out.
	if !strings.Contains(readShellAsset(t, "interact.js"), "RESERVED_PARAMS") {
		t.Error("interact.js does not strip reserved param names (token/block) from getParams")
	}
}

// TestInteract_AutoRefreshVisibilityRetryAfterFloor proves the refreshInterval
// scheduler (spec §6.3): ±10% jitter, pause while the tab is hidden, exponential
// backoff on 429/503 honoring Retry-After, and — the load-bearing invariant — a
// 15s floor it can never drop below (the server clamps; this is the client-side
// guarantee a bundle can never drive sub-15s re-rendering).
func TestInteract_AutoRefreshVisibilityRetryAfterFloor(t *testing.T) {
	src := readShellAsset(t, "interact.js")
	got := stripWhitespace(src)

	// 15s floor: the constant, and the clamp that applies it as a lower bound.
	if !strings.Contains(got, "MIN_REFRESH_SECONDS=15") {
		t.Error("interact.js does not define the 15s minimum refresh floor")
	}
	if !strings.Contains(got, "Math.max(MIN_REFRESH_SECONDS,raw)") {
		t.Error("interact.js does not clamp the refresh interval up to the 15s floor")
	}
	// Visibility pause.
	if !strings.Contains(src, "visibilitychange") || !strings.Contains(got, `visibilityState==="hidden"`) {
		t.Error("interact.js does not pause auto-refresh while the tab is hidden (spec §6.3)")
	}
	// Backoff on 429/503 honoring Retry-After.
	for _, want := range []string{"429", "503", `"Retry-After"`, "parseRetryAfter"} {
		if !strings.Contains(src, want) {
			t.Errorf("interact.js backoff is missing %q (429/503 + Retry-After handling)", want)
		}
	}
	// ±10% jitter.
	if !strings.Contains(got, "REFRESH_JITTER=0.1") {
		t.Error("interact.js does not apply ±10% jitter to the refresh cadence")
	}
	// The backoff never falls below the base interval (which is >= the floor).
	if !strings.Contains(got, "Math.max(base,seconds)") {
		t.Error("interact.js backoff does not keep the delay at or above the base interval")
	}
}

// TestInteract_ChartOnClickBindsVegaView proves the onClick cross-filter binding
// (spec §6.3 onClick:{param}): chart.js hands the resolved vega view + the
// declared param to interact.js's bindChartOnClick, which listens on the view
// and sets the param (triggering a re-render) — config, not code.
func TestInteract_ChartOnClickBindsVegaView(t *testing.T) {
	chart := stripWhitespace(readShellAsset(t, "blocks/chart.js"))
	if !strings.Contains(chart, `import{bindChartOnClick,registerVegaView}from"../interact.js"`) {
		t.Error("chart.js does not import bindChartOnClick from interact.js")
	}
	if !strings.Contains(chart, "bindChartOnClick(result.view,param)") {
		t.Error("chart.js does not bind onClick to the resolved vega view (result.view)")
	}
	interact := stripWhitespace(readShellAsset(t, "interact.js"))
	if !strings.Contains(interact, `view.addEventListener("click"`) {
		t.Error("interact.js bindChartOnClick does not add a click listener to the vega view")
	}
	if !strings.Contains(interact, "setParam(param,String(value))") {
		t.Error("interact.js bindChartOnClick does not set the named param (as a string) from the clicked datum")
	}
}

// TestInteract_ModalsRenderThroughRegistry proves both modal forms and that
// modal content — inline OR lazily fetched via block=<id> — is rendered through
// the SAME renderer registry the page uses (deps.renderBlock, injected from
// shell.js), so partial content is sanitized identically (no second path).
func TestInteract_ModalsRenderThroughRegistry(t *testing.T) {
	got := stripWhitespace(readShellAsset(t, "interact.js"))
	// Inline form renders already-present blocks; lazy form fetches block=<param>.
	if !strings.Contains(got, "fetchRender({block:param})") {
		t.Error("interact.js lazy modal does not fetch its content via the block=<id> partial render")
	}
	if !strings.Contains(got, "deps.renderBlock(block)") {
		t.Error("interact.js modals do not render blocks through the shared renderBlock registry")
	}
	// Modal state deep-links via a URL param (setParams with the lazy param).
	if !strings.Contains(readShellAsset(t, "interact.js"), "restoreModalDeepLink") {
		t.Error("interact.js does not restore a deep-linked modal from the URL param")
	}
}

// TestShellJS_RerenderPrimitivesWiredToInteract proves shell.js exposes the two
// mount primitives interact.js needs and injects them via initInteract, and that
// renderBlock (the single dispatch point) wires modal triggers — so a re-render
// or a modal reuses the exact same registry-driven, sanitized render path.
func TestShellJS_RerenderPrimitivesWiredToInteract(t *testing.T) {
	got := stripWhitespace(readShellAsset(t, "shell.js"))
	for _, want := range []string{
		`import{initInteract,onBooted,attachModalTrigger,finalizeVegaViewsWithin}from"./interact.js"`,
		`initInteract({applyDoc,renderBlock})`,
		`exportfunctionapplyDoc(doc)`,
		`exportfunctionrenderBlock(block)`,
		`attachModalTrigger(node,block.modal)`,
		`onBooted(doc)`,
	} {
		if !strings.Contains(got, want) {
			t.Errorf("shell.js is missing the interact wiring token %q", want)
		}
	}
}

// ===========================================================================
// RFC 028 Part 4 (V2 fix round): the four Codex-gate Major findings. Live
// behavior (race ordering, finalize-on-swap, modal namespace in the live DOM)
// is V4's browser checklist; these are the statically-checkable guards.
// ===========================================================================

// TestInteract_RendersAreSequenced proves the fix for Finding 1: every full
// re-render takes a monotonic token + AbortController, and a response is dropped
// (superseded) if a newer render started or its fetch was aborted — so a slow
// earlier response can never overwrite a newer one. The abort of a superseded
// render is treated as "superseded", never an error state.
func TestInteract_RendersAreSequenced(t *testing.T) {
	src := readShellAsset(t, "interact.js")
	got := stripWhitespace(src)
	for _, want := range []string{
		"letrenderSeq=0",                             // the monotonic token
		"functionbeginRender()",                      // taken per full render
		"newAbortController()",                       // supersede-abort of the prior fetch
		"if(seq!==renderSeq)return{superseded:true}", // drop a stale/late response
		"fetchRender({signal})",                      // the POST re-render carries the abort signal
	} {
		if !strings.Contains(got, want) {
			t.Errorf("interact.js render-sequencing is missing %q", want)
		}
	}
	// An abort must NOT surface as an error state (it is treated as superseded).
	if !strings.Contains(src, "isAbortError") {
		t.Error("interact.js does not distinguish an aborted (superseded) fetch from a real error")
	}
	// The GET ctx.path navigation (loadPath) is sequenced too — the signal is
	// threaded into its fetch options alongside the GET method.
	if !strings.Contains(got, `method:"GET",credentials:"same-origin",signal,`) {
		t.Error("interact.js loadPath (ctx.path nav) does not pass the abort signal — nav is not sequenced")
	}
}

// TestInteract_ModalStateReservedNamespace proves the fix for Finding 2: modal
// deep-link state uses a reserved `__dtp_modal` key (its own namespace), never a
// top-level param named after the block — so it cannot clobber an app filter of
// the same name — and the shell + server agree on the reserved literal.
func TestInteract_ModalStateReservedNamespace(t *testing.T) {
	src := readShellAsset(t, "interact.js")
	got := stripWhitespace(src)
	if !strings.Contains(got, `constMODAL_PARAM="__dtp_modal"`) {
		t.Error("interact.js does not define the reserved modal key MODAL_PARAM=\"__dtp_modal\"")
	}
	if !strings.Contains(got, `RESERVED_PARAMS=["token","block",MODAL_PARAM]`) {
		t.Error("interact.js does not include MODAL_PARAM in the reserved (stripped-from-ctx.params) set")
	}
	// Open/close write the RESERVED key, not the app param name.
	if !strings.Contains(got, `setParams({[MODAL_PARAM]:param},{refresh:false})`) {
		t.Error("openLazyModal does not deep-link via the reserved MODAL_PARAM key")
	}
	if !strings.Contains(got, `setParams({[MODAL_PARAM]:null},{refresh:false})`) {
		t.Error("closeActiveModal does not clear the reserved MODAL_PARAM key")
	}
	// The old, colliding pattern (a top-level param named after the block) is gone.
	if strings.Contains(got, `setParams({[param]:"1"}`) || strings.Contains(got, `setParams({[param]:null}`) {
		t.Error("interact.js still writes a modal param under the app's own param namespace (collision not fixed)")
	}
	// The server strips the SAME literal (same package const), so the reserved
	// key never reaches ctx.params. The behavioral proof is
	// TestReadParams_StripsModalStateKey; here we pin the literal agreement.
	if modalStateParam != "__dtp_modal" {
		t.Errorf("server modalStateParam = %q, must match the shell's MODAL_PARAM __dtp_modal", modalStateParam)
	}
}

// TestInteract_VegaViewsFinalizedBeforeSwap proves the fix for Finding 3: charts
// register their embed result, and the shell finalizes live views (calling
// result.finalize() and removing the click listener) BEFORE clearing the DOM on
// a re-render, and before a modal closes — so a view never leaks across a swap.
func TestInteract_VegaViewsFinalizedBeforeSwap(t *testing.T) {
	interact := readShellAsset(t, "interact.js")
	gi := stripWhitespace(interact)
	for _, want := range []string{
		"exportfunctionregisterVegaView(",
		"exportfunctionfinalizeVegaViewsWithin(",
		"entry.result.finalize()",                     // the finalize call
		`removeEventListener("click",entry.listener)`, // listener teardown
	} {
		if !strings.Contains(gi, want) {
			t.Errorf("interact.js vega finalize is missing %q", want)
		}
	}
	// chart.js registers each mounted view (with its result, listener, mount).
	chart := stripWhitespace(readShellAsset(t, "blocks/chart.js"))
	if !strings.Contains(chart, `import{bindChartOnClick,registerVegaView}from"../interact.js"`) {
		t.Error("chart.js does not import registerVegaView")
	}
	if !strings.Contains(chart, "registerVegaView({result,listener,mount})") {
		t.Error("chart.js does not register the mounted vega view for finalization")
	}
	// shell.js applyDoc finalizes BEFORE it clears the root (ordering matters —
	// finalize after clear would already have leaked the view).
	shell := readShellAsset(t, "shell.js")
	idxFinalize := strings.Index(shell, "finalizeVegaViewsWithin(root)")
	idxClear := strings.Index(shell, `root.textContent = ""`)
	if idxFinalize < 0 || idxClear < 0 {
		t.Fatalf("shell.js applyDoc is missing the finalize (%d) or clear (%d)", idxFinalize, idxClear)
	}
	if idxFinalize > idxClear {
		t.Error("shell.js applyDoc clears the root BEFORE finalizing vega views — the view leaks")
	}
}

// TestInteract_RetryAfterIsCapped proves the fix for Finding 4: the honored
// Retry-After delay is capped to MAX_REFRESH_SECONDS before scheduling, so a
// huge/hostile value can never overflow setTimeout's ~2^31 ms limit and wrap to
// fire immediately. The cap is applied on the honored path (backoffSeconds) and
// again as a final guard in scheduleNextRefresh.
func TestInteract_RetryAfterIsCapped(t *testing.T) {
	got := stripWhitespace(readShellAsset(t, "interact.js"))
	// The cap reassignment appears on the honored path and the scheduler guard.
	if strings.Count(got, "seconds=Math.min(MAX_REFRESH_SECONDS,seconds)") < 2 {
		t.Error("interact.js does not cap the honored Retry-After delay to MAX_REFRESH_SECONDS (setTimeout overflow risk)")
	}
	// backoffSeconds still honors Retry-After (waits at least that long, up to the cap).
	if !strings.Contains(got, "Math.max(seconds,retryAfter)") {
		t.Error("interact.js no longer honors Retry-After on the backoff path")
	}
}

// ===========================================================================
// RFC 028 Part 4 (V3): export + polish.
//
// CSV export is client-side only (spec §6.3 "Client-side, no round trip …
// CSV export") — csv.js's buildCSV is a pure JS function with no server-side
// counterpart, and there is no JS runtime vendored into this module tree to
// execute it directly (adding one just to exercise a ~30-line escaper would
// be a disproportionate new dependency, and V0-V2 already established the
// source-level-inspection convention this file uses throughout). Two things
// close that gap together:
//   - TestCSVGolden_OWASPEscapingAndRFC4180Quoting is a Go PORT of csv.js's
//     exact rule set (OWASP formula-injection escape, then RFC 4180
//     quoting), checked against a documented input->output fixture — the
//     "golden fixture" the task brief calls for, made genuinely runnable
//     without a JS engine.
//   - TestCSVModule_TriggerSetAndQuotingMatchGolden pins the SHIPPED csv.js
//     to that exact same rule set at the source level (the trigger-character
//     array, the quoting condition, the doubling of embedded quotes), so the
//     Go port and the real file can never silently drift apart.
//
// The rest of V3 (print CSS, skeleton, empty/error states, theme reuse) is
// visual and lands in V4's manual browser checklist; what follows are the
// source-level guards a headless Go test CAN make — see task-V3-report.md
// for the full checklist.
// ===========================================================================

// csvFormulaTriggers is a Go PORT of csv.js's FORMULA_TRIGGER_CHARS — the
// exact OWASP CSV-injection trigger set spec §6.3 names, no more, no less.
var csvFormulaTriggers = []byte{'=', '+', '-', '@', '\t', '\r'}

// csvCellGolden is a Go PORT of csv.js's csvCell: OWASP formula-escape (a
// leading trigger character gets a "'" prefix), then RFC 4180 quoting (wrap
// in double quotes, doubling any embedded double quote, if the — possibly
// already-escaped — value contains a comma, double quote, or newline).
func csvCellGolden(value string) string {
	if len(value) > 0 {
		for _, c := range csvFormulaTriggers {
			if value[0] == c {
				value = "'" + value
				break
			}
		}
	}
	if strings.ContainsAny(value, ",\"\r\n") {
		value = `"` + strings.ReplaceAll(value, `"`, `""`) + `"`
	}
	return value
}

// buildCSVGolden is a Go PORT of csv.js's buildCSV: a header row (columns)
// plus one row per entry in rows, CRLF-joined, CRLF-terminated.
func buildCSVGolden(columns []string, rows [][]string) string {
	csvRow := func(cells []string) string {
		out := make([]string, len(cells))
		for i, c := range cells {
			out[i] = csvCellGolden(c)
		}
		return strings.Join(out, ",")
	}
	lines := []string{csvRow(columns)}
	for _, row := range rows {
		lines = append(lines, csvRow(row))
	}
	return strings.Join(lines, "\r\n") + "\r\n"
}

// TestCSVGolden_OWASPEscapingAndRFC4180Quoting is the golden-fixture check
// the task brief calls for: a documented table of cell inputs -> expected
// CSV-encoded output, run through the Go port above.
func TestCSVGolden_OWASPEscapingAndRFC4180Quoting(t *testing.T) {
	cases := []struct {
		name  string
		input string
		want  string
	}{
		{"plain text is untouched", "Widget", "Widget"},
		{"empty string is never prefixed (no leading char to trigger on)", "", ""},
		{"formula equals", "=SUM(A1:A10)", "'=SUM(A1:A10)"},
		{"formula plus", "+1234", "'+1234"},
		{"formula minus (also an ordinary negative number)", "-42", "'-42"},
		{"formula at-sign", "@mention", "'@mention"},
		{"formula tab (tab alone does not also trigger RFC 4180 quoting)", "\tTabbed", "'\tTabbed"},
		{"formula CR (CR ALSO triggers RFC 4180 quoting, unlike tab, so this is BOTH escaped AND quoted)", "\rCarriage", "\"'\rCarriage\""},
		{"trigger char NOT at the start is untouched", "A=B", "A=B"},
		{"embedded comma is quoted", "Acme, Inc.", `"Acme, Inc."`},
		{"embedded double quote is doubled and quoted", `He said "hi"`, `"He said ""hi"""`},
		{"embedded newline is quoted", "Line1\nLine2", "\"Line1\nLine2\""},
		{"formula-escaped AND comma: escape first, then quote the whole field", "=A1,A2", `"'=A1,A2"`},
		{"formula-escaped AND quote-containing: apostrophe survives inside the quoting", `="x"`, `"'=""x"""`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := csvCellGolden(c.input)
			if got != c.want {
				t.Errorf("csvCellGolden(%q) = %q, want %q", c.input, got, c.want)
			}
		})
	}

	// buildCSV-level integration: header + rows, CRLF-joined and -terminated,
	// header cells escaped exactly like data cells (a column named "=Note" is
	// just as untrusted as a cell).
	got := buildCSVGolden(
		[]string{"Name", "=Note"},
		[][]string{
			{"Widget", "ok"},
			{"=EVIL()", "danger, zone"},
		},
	)
	want := "Name,'=Note\r\nWidget,ok\r\n'=EVIL(),\"danger, zone\"\r\n"
	if got != want {
		t.Errorf("buildCSVGolden(...) =\n%q\nwant\n%q", got, want)
	}
}

// ===========================================================================
// RFC 028 Part 4 (V3 fix round): Codex Major finding on commit 74b0740.
//
// blocks/table.js's CSV download handler used to map over the WHOLE row
// array (`visibleRows().map((row) => row.map(cellText))`), unbounded by
// `columns` — but the render loop (table.js's renderBody) draws exactly
// `columns.length` cells per row (`for (let i = 0; i < columns.length; i++)
// ... cellText(row[i])`). An over-wide row (more cells than columns) leaked
// un-rendered trailing cells into the export; a short row (fewer cells than
// columns) produced a CSV whose data fields didn't line up with the header
// row. The fix projects each row through the SAME `columns` list and the
// SAME index-based `cellText(row[i])` accessor the renderer uses.
//
// projectRowGolden below is a Go PORT of the FIXED projection; it is checked
// against the two cases the fix addresses, and contrasted against
// projectRowBuggyGolden (a Go port of the PRE-fix shape) to demonstrate, in
// Go, why the old shape was wrong for both. The shipped table.js is pinned to
// the fixed shape at the source level by
// TestTableRenderer_CSVExportProjectsThroughRenderedColumns below (source-
// level, since there is no JS runtime here — see the V3 CSV section above for
// why that is this file's established way of closing this gap). Mutation-
// tested: reverting table.js to the pre-fix line FAILs that test (see
// task-V3-report.md's fix-round section); reverted byte-identically.
// ===========================================================================

// projectRowGolden is a Go PORT of the FIXED table.js download-handler
// projection: `columns.map((_c, i) => cellText(row[i]))`. `row` here is
// already-stringified cell text (as cellText would produce); a "missing"
// cell is modeled by `row` simply being shorter than `columns` (in JS,
// `row[i]` for an out-of-range `i` is `undefined`, and `cellText(undefined)
// === ""`).
func projectRowGolden(columns []string, row []string) []string {
	out := make([]string, len(columns))
	for i := range columns {
		if i < len(row) {
			out[i] = row[i]
		} else {
			out[i] = "" // cellText(undefined) === ""
		}
	}
	return out
}

// projectRowBuggyGolden is a Go PORT of the PRE-FIX table.js download
// handler (commit 74b0740): `row.map(cellText)` — the whole row, unbounded by
// `columns`. Kept ONLY so TestCSVGolden_RowProjectionMatchesRenderedColumns
// can demonstrate this shape is wrong for both fixture cases below; nothing
// else in this file should ever call it.
func projectRowBuggyGolden(row []string) []string {
	return row
}

// TestCSVGolden_RowProjectionMatchesRenderedColumns is the golden-fixture
// extension for the Codex Major finding: an over-wide row's extra cells are
// dropped, and a short row is padded blank to the header's field count — the
// CSV a viewer downloads must have exactly the fields the table rendered, no
// more, no less.
func TestCSVGolden_RowProjectionMatchesRenderedColumns(t *testing.T) {
	t.Run("over-wide row: extra cells beyond columns are dropped, not exported", func(t *testing.T) {
		columns := []string{"A", "B"}
		row := []string{"1", "2", "3", "4"} // 4 cells, only 2 columns — table renders just A, B
		want := []string{"1", "2"}

		got := projectRowGolden(columns, row)
		if !reflect.DeepEqual(got, want) {
			t.Errorf("projectRowGolden(%v, %v) = %v, want %v", columns, row, got, want)
		}
		// Demonstrate the bug this fixes: the pre-fix shape leaks the two
		// un-rendered trailing cells straight into the export.
		if buggy := projectRowBuggyGolden(row); reflect.DeepEqual(buggy, want) {
			t.Fatalf("fixture is broken: the PRE-FIX projection %v must NOT already equal the fixed expectation %v", buggy, want)
		}
	})

	t.Run("short row: missing cells are padded blank to match the header width", func(t *testing.T) {
		columns := []string{"A", "B", "C"}
		row := []string{"1"} // 1 cell, 3 columns — table renders A="1", B="", C=""
		want := []string{"1", "", ""}

		got := projectRowGolden(columns, row)
		if !reflect.DeepEqual(got, want) {
			t.Errorf("projectRowGolden(%v, %v) = %v, want %v", columns, row, got, want)
		}
		// Demonstrate the bug this fixes: the pre-fix shape produces a row with
		// FEWER fields than the header, desyncing every column after it.
		if buggy := projectRowBuggyGolden(row); len(buggy) == len(want) {
			t.Fatalf("fixture is broken: the PRE-FIX projection %v must NOT already have the fixed field count %d", buggy, len(want))
		}
	})

	// End-to-end through the full pipeline (project -> escape -> quote): an
	// over-wide row whose extra cell would ALSO have been formula-escaped had
	// it leaked through must not appear in the final CSV text at all.
	got := buildCSVGolden(
		[]string{"Name", "Note"},
		[][]string{projectRowGolden([]string{"Name", "Note"}, []string{"Widget", "=EVIL()", "=ALSO_EVIL()"})},
	)
	want := "Name,Note\r\nWidget,'=EVIL()\r\n"
	if got != want {
		t.Errorf("end-to-end projected+escaped CSV =\n%q\nwant\n%q (the dropped extra cell must not appear anywhere)", got, want)
	}
}

// TestCSVModule_TriggerSetAndQuotingMatchGolden pins the SHIPPED csv.js to
// the exact same rule set TestCSVGolden_* exercises in Go, at the source
// level: the trigger-character array (all six, nothing extra), the RFC 4180
// quoting condition (comma/quote/CR/LF), and the embedded-quote-doubling
// replace. Mutation-tested: with FORMULA_TRIGGER_CHARS temporarily emptied to
// `[]` in csv.js, this test FAILs (see task-V3-report.md); reverted
// byte-identically.
func TestCSVModule_TriggerSetAndQuotingMatchGolden(t *testing.T) {
	src := readShellAsset(t, "csv.js")

	triggers := arrayLiteral(t, src, "FORMULA_TRIGGER_CHARS")
	wantTriggers := stripWhitespace(`"=", "+", "-", "@", "\t", "\r"`)
	if stripWhitespace(triggers) != wantTriggers {
		t.Errorf("csv.js FORMULA_TRIGGER_CHARS = %q, want exactly %q", stripWhitespace(triggers), wantTriggers)
	}

	stripped := stripWhitespace(src)
	for _, want := range []string{
		`NEEDS_QUOTING=/[",\r\n]/`,        // the RFC 4180 quoting condition (comma/quote/CR/LF)
		`text.replace(/"/g,'""')`,         // doubling an embedded quote
		`escapeFormula(text)`,             // csvCell calls the formula-escape step
		"quoteField(escapeFormula(text))", // escape BEFORE quote — order matters (see module header)
	} {
		if !strings.Contains(stripped, want) {
			t.Errorf("csv.js is missing %q — does not match the golden rule set", want)
		}
	}
}

// TestCSVModule_PureNoDOMNoInnerHTML proves csv.js is the pure, side-effect-
// free module its header comment claims: no innerHTML/insertAdjacentHTML
// (trivially true if it never touches the DOM at all, which this also
// checks), no `document.`/`window.` access, and no fetch/import. This is
// stricter than — and deliberately separate from — the generic
// TestShellRenderers_NeverUseInnerHTML sweep: that sweep also requires
// `textContent` to be present (proving a renderer actually sanitizes
// something), which csv.js correctly has none of, being pure string logic
// rather than a DOM renderer.
func TestCSVModule_PureNoDOMNoInnerHTML(t *testing.T) {
	src := readShellAsset(t, "csv.js")
	if loc := innerHTMLUsage.FindString(src); loc != "" {
		t.Errorf("csv.js must never use innerHTML/insertAdjacentHTML — found %q", loc)
	}
	for _, forbidden := range []string{"document.", "window.", "fetch(", "import("} {
		if strings.Contains(src, forbidden) {
			t.Errorf("csv.js contains %q — it must stay a pure, DOM-free string-building module", forbidden)
		}
	}
}

// TestTableRenderer_CSVDownloadIsBlobNotInnerHTML proves the "Download CSV"
// affordance is wired the way spec §6.4 requires: table.js imports csv.js's
// pure buildCSV, exposes a "Download CSV" control, and hands the result to a
// same-origin Blob + object URL — never a network call, never innerHTML.
func TestTableRenderer_CSVDownloadIsBlobNotInnerHTML(t *testing.T) {
	src := readShellAsset(t, "blocks/table.js")
	stripped := stripWhitespace(src)

	if !strings.Contains(stripped, `import{buildCSV}from"../csv.js"`) {
		t.Error("table.js does not import buildCSV from csv.js")
	}
	if !strings.Contains(src, "Download CSV") {
		t.Error("table.js has no \"Download CSV\" affordance")
	}
	for _, want := range []string{"new Blob(", "URL.createObjectURL(", ".download ="} {
		if !strings.Contains(src, want) {
			t.Errorf("table.js CSV download does not use %q — spec §6.4 requires a Blob + object URL download", want)
		}
	}
	if strings.Contains(src, "://") {
		t.Error("table.js CSV download reaches an external origin — it must be same-origin/local (Blob) only")
	}
	if loc := innerHTMLUsage.FindString(src); loc != "" {
		t.Errorf("table.js must never use innerHTML/insertAdjacentHTML — found %q", loc)
	}
}

// TestTableRenderer_CSVExportProjectsThroughRenderedColumns proves the fix
// for the Codex Major finding on commit 74b0740: the CSV download handler
// projects each row through the SAME `columns` list and index-based
// `cellText(row[i])` accessor the render loop (table.js's renderBody, `for
// (let i = 0; i < columns.length; i++) ... cellText(row[i])`) uses — never
// the whole row array unbounded. Without this, an over-wide row leaks
// un-rendered trailing cells into the export, and a short row desyncs the
// CSV's field count from its header. See TestCSVGolden_RowProjectionMatchesRenderedColumns
// for the Go-side proof that the fixed projection produces the right shape
// for both cases. Mutation-tested: reverting to the pre-fix
// `visibleRows().map((row) => row.map(cellText))` FAILs this test (see
// task-V3-report.md's fix-round section); reverted byte-identically.
func TestTableRenderer_CSVExportProjectsThroughRenderedColumns(t *testing.T) {
	got := stripWhitespace(readShellAsset(t, "blocks/table.js"))

	want := stripWhitespace(`visibleRows().map((row) => columns.map((_c, i) => cellText(row[i])))`)
	if !strings.Contains(got, want) {
		t.Errorf("table.js's CSV download does not project each row through columns (missing %q) — it may leak un-rendered cells or desync the header/row field count", want)
	}

	// The pre-fix shape (the whole row, unbounded by columns) must be gone.
	preFix := stripWhitespace(`visibleRows().map((row) => row.map(cellText))`)
	if strings.Contains(got, preFix) {
		t.Error("table.js's CSV download still maps the whole row unbounded by columns — the Codex Major finding on commit 74b0740 is not fixed")
	}
}

// TestShellJS_EmptyDocAndDataUseSharedEmptyState proves the spec-brief empty
// states (a doc with zero blocks, a table with no rows, a chart whose inline
// dataset is present but empty) all route through the SAME shared
// renderEmptyState placeholder (shell.js) rather than each block inventing
// its own — table.js and chart.js both import it from shell.js.
func TestShellJS_EmptyDocAndDataUseSharedEmptyState(t *testing.T) {
	shell := stripWhitespace(readShellAsset(t, "shell.js"))
	for _, want := range []string{
		"exportfunctionrenderEmptyState(message)",
		"blocks.length===0",
		"renderEmptyState(",
	} {
		if !strings.Contains(shell, want) {
			t.Errorf("shell.js missing %q — zero-block empty state not wired", want)
		}
	}

	table := stripWhitespace(readShellAsset(t, "blocks/table.js"))
	if !strings.Contains(table, `import{renderEmptyState}from"../shell.js"`) {
		t.Error("table.js does not import the shared renderEmptyState from shell.js")
	}
	if !strings.Contains(table, "rows.length===0") {
		t.Error("table.js does not detect the zero-rows (no data) case")
	}

	chart := stripWhitespace(readShellAsset(t, "blocks/chart.js"))
	if !strings.Contains(chart, `import{renderEmptyState}from"../shell.js"`) {
		t.Error("chart.js does not import the shared renderEmptyState from shell.js")
	}
	if !strings.Contains(chart, "isEmptyChartData(spec)") {
		t.Error("chart.js does not detect an empty (but schema-valid) inline dataset")
	}
}

// TestErrorCard_ChartMarkdownModalReuseSharedClass proves every V1/V2 render-
// failure text ("this … could not be displayed") is marked with the shared
// dtp-error-card class (spec brief: "reuse V1/V2's error states, make them
// look intentional"), and that theme.css defines it using --dtp-danger — a
// hue reused verbatim from ui/product/style.css's --status-fail-bg, not an
// invented color.
func TestErrorCard_ChartMarkdownModalReuseSharedClass(t *testing.T) {
	for _, path := range []string{"blocks/chart.js", "blocks/markdown.js", "interact.js"} {
		src := readShellAsset(t, path)
		if !strings.Contains(src, `classList.add("dtp-error-card")`) {
			t.Errorf("%s does not apply the shared dtp-error-card class on its failure path", path)
		}
	}
	theme := readShellAsset(t, "theme.css")
	if !strings.Contains(theme, ".dtp-error-card") {
		t.Error("theme.css does not define .dtp-error-card")
	}
	if !strings.Contains(theme, "--dtp-danger") {
		t.Error("theme.css does not define --dtp-danger for the error card")
	}
	if !strings.Contains(theme, "#ef4444") {
		t.Error("theme.css's error color does not reuse ui/product's --status-fail-bg hue (#ef4444)")
	}
}

// TestShellSkeleton_FirstPaintBeforeMount proves index.html ships a real
// skeleton placeholder (not a bare "Loading…" line) marked aria-busy before
// shell.js's boot() ever runs, and that applyDoc clears that attribute once
// real content is mounted (so the accessibility state does not go stale).
func TestShellSkeleton_FirstPaintBeforeMount(t *testing.T) {
	idx := string(appshell.IndexHTML)
	for _, want := range []string{`id="dtp-root"`, `aria-busy="true"`, "dtp-skeleton"} {
		if !strings.Contains(idx, want) {
			t.Errorf("index.html missing %q — no first-paint skeleton before shell.js mounts", want)
		}
	}
	shell := stripWhitespace(readShellAsset(t, "shell.js"))
	if !strings.Contains(shell, `removeAttribute("aria-busy")`) {
		t.Error("shell.js's applyDoc does not clear aria-busy once the real doc is mounted")
	}
}

// TestShellJS_ChartRendererUsesSVGForPrintTheming proves shell.js's
// mountVegaLiteChart chokepoint requests the SVG renderer (vega-embed's
// default is canvas): a canvas is an opaque bitmap baked at mount time from
// whatever theme was active on screen, and no CSS rule (including theme.css's
// print override) can repaint its pixels — SVG text is real DOM the print
// stylesheet can recolor. This is load-bearing for "charts … legible on
// paper", not a rendering-quality preference.
func TestShellJS_ChartRendererUsesSVGForPrintTheming(t *testing.T) {
	got := stripWhitespace(readShellAsset(t, "shell.js"))
	if !strings.Contains(got, `renderer:"svg"`) {
		t.Error("shell.js's mountVegaLiteChart does not force the SVG renderer — the print stylesheet cannot recolor a canvas")
	}
	// Still routed through the SAME lockdown call (renderer sits alongside the
	// existing actions/loader lockdown, not a second vegaEmbed call site).
	wantSameCall := stripWhitespace(`actions:false,loader:{load:()=>Promise.reject(new Error("loading disabled"))},renderer:"svg"`)
	if !strings.Contains(got, wantSameCall) {
		t.Error("shell.js's renderer:\"svg\" is not part of the single locked-down vegaEmbed call")
	}
}

// dtpBlockClassSelector matches a bare, unqualified CSS class selector (e.g.
// ".dtp-tab-bar {" or ".dtp-tab-bar,") — used to find where a given rule is
// declared, not merely mentioned in a comment.
func dtpBlockClassSelector(class string) *regexp.Regexp {
	return regexp.MustCompile(regexp.QuoteMeta(class) + `\s*[,{]`)
}

// TestPrintCSS_HidesChromeForcesLightPageBreak proves theme.css's @media
// print block (spec brief: hide chrome/filters/interactive controls,
// page-break between blocks, force light) exists, hides the specific
// interactive-chrome classes named in the V3 report, keeps a block from being
// sliced across a page, forces the light palette, and — the cascade-ordering
// invariant — is declared AFTER the prefers-color-scheme:dark block so it
// wins when a viewer prints while their OS is in dark mode.
func TestPrintCSS_HidesChromeForcesLightPageBreak(t *testing.T) {
	theme := readShellAsset(t, "theme.css")

	darkIdx := strings.Index(theme, "@media (prefers-color-scheme: dark)")
	// "@media print {" (the actual rule, brace included) rather than "@media
	// print" alone — the file's own header comment mentions the stylesheet by
	// name too, and that mention must not be mistaken for the rule itself.
	printIdx := strings.Index(theme, "@media print {")
	if darkIdx < 0 || printIdx < 0 {
		t.Fatalf("theme.css is missing the dark-mode (%d) or print (%d) media block", darkIdx, printIdx)
	}
	if printIdx < darkIdx {
		t.Error("theme.css's @media print block must come AFTER @media (prefers-color-scheme: dark) to win the cascade tiebreak when printing in dark mode")
	}

	printBlock := theme[printIdx:]
	for _, class := range []string{
		".dtp-table-toolbar", ".dtp-modal-trigger", ".dtp-tab-bar",
		".dtp-block-filter", ".dtp-modal-backdrop", ".dtp-refresh-overlay", ".dtp-refresh-error",
	} {
		if !dtpBlockClassSelector(class).MatchString(printBlock) {
			t.Errorf("theme.css's @media print block does not hide %q (interactive chrome)", class)
		}
	}
	if !strings.Contains(printBlock, "display: none !important") {
		t.Error("theme.css's @media print block does not force-hide interactive chrome")
	}
	if !strings.Contains(printBlock, "break-inside: avoid") {
		t.Error("theme.css's @media print block does not keep a block from splitting across a page")
	}
	if !strings.Contains(printBlock, "--dtp-bg: #ffffff") {
		t.Error("theme.css's @media print block does not force the light background")
	}
	if !strings.Contains(printBlock, ".dtp-chart-mount svg text") {
		t.Error("theme.css's @media print block does not recolor chart text for paper")
	}
}

// ===========================================================================
// RFC 028 Part 4 (V4 gate fix): extend the Vega finalize discipline to the two
// paths V2/V3 did not wire it into — a tab switch, and a chart whose mount
// detaches before its async embed resolves. Live "view count stays flat" is a
// V4 browser-checklist item; these are the statically-checkable guards.
// ===========================================================================

// TestTabsRenderer_FinalizesViewsBeforeClearingPanel proves Finding 1: a tab
// switch finalizes the outgoing panel's Vega views BEFORE dropping their DOM,
// exactly as applyDoc does before clearing root — otherwise switching away from
// a chart tab leaks the view/dataflow/timers/click-listener.
func TestTabsRenderer_FinalizesViewsBeforeClearingPanel(t *testing.T) {
	src := readShellAsset(t, "blocks/tabs.js")
	if !strings.Contains(stripWhitespace(src), `import{finalizeVegaViewsWithin}from"../interact.js"`) {
		t.Error("tabs.js does not import finalizeVegaViewsWithin from interact.js")
	}
	idxFinalize := strings.Index(src, "finalizeVegaViewsWithin(panel)")
	idxClear := strings.Index(src, `panel.textContent = ""`)
	if idxFinalize < 0 || idxClear < 0 {
		t.Fatalf("tabs.js show() is missing the finalize (%d) or the panel clear (%d)", idxFinalize, idxClear)
	}
	if idxFinalize > idxClear {
		t.Error("tabs.js show() clears the panel BEFORE finalizing its vega views — a tab switch leaks the outgoing chart")
	}
}

// TestChartRenderer_GuardsDetachedMountRace proves Finding 2: a chart whose
// mount detaches before its async import+embed resolves is finalized rather
// than left live and unregistered-for-cleanup. chart.js skips embedding into an
// already-detached node (isConnected pre-check), and interact.js's
// registerVegaView finalizes-and-drops a view whose mount detached during the
// embed (an orphan finalizeVegaViewsWithin(root) would never reach).
func TestChartRenderer_GuardsDetachedMountRace(t *testing.T) {
	chart := stripWhitespace(readShellAsset(t, "blocks/chart.js"))
	if !strings.Contains(chart, "if(!mount.isConnected)returnundefined") {
		t.Error("chart.js does not skip embedding into a detached mount (isConnected pre-check)")
	}
	interact := stripWhitespace(readShellAsset(t, "interact.js"))
	if !strings.Contains(interact, "entry.mount.isConnected===false") {
		t.Error("interact.js registerVegaView does not detect an already-detached mount")
	}
	// The detached branch must FINALIZE (not merely skip) so nothing leaks.
	if !strings.Contains(interact, "if(!entry.mount||entry.mount.isConnected===false){finalizeVegaEntry(entry);") {
		t.Error("interact.js registerVegaView does not finalize a detached-mount view (it would leak)")
	}
}
