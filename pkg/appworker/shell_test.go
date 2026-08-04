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
	"interact.js",
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
