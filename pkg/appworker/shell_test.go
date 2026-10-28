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
// X-Datuplet-App-Render: 1 header, and a body that is the current param state
// (string-valued by construction).
func TestInteract_RerenderPostContract(t *testing.T) {
	got := stripWhitespace(readShellAsset(t, "interact.js"))
	for _, want := range []string{
		`method:"POST"`,
		`Accept:"application/json"`,
		`"Content-Type":"application/json"`,
		`"X-Datuplet-App-Render":"1"`,
		`body:JSON.stringify(getParams())`,
		// Params are string-only (§6.5 "no type coercion"; W6 REJECTS non-string
		// body values): they come from URLSearchParams, whose values are strings.
		"newURLSearchParams(location.search)",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("interact.js re-render fetch is missing the required contract token %q", want)
		}
	}
	// The reserved platform names are stripped from ctx.params on the way out.
	if !strings.Contains(readShellAsset(t, "interact.js"), "RESERVED_PARAMS") {
		t.Error("interact.js does not strip reserved param names (token/block) from getParams")
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

// ===========================================================================
// RFC 028 Part 4 (V2 fix round): the four Codex-gate Major findings. Live
// behavior (race ordering, finalize-on-swap, modal namespace in the live DOM)
// is V4's browser checklist; these are the statically-checkable guards.
// ===========================================================================

// TestShellJS_V2FixRoundGuards pins the load-bearing tokens of the V2/V4
// Codex-gate fixes at the source level (live behavior — race ordering,
// finalize-on-swap, modal namespace in the live DOM — is V4's browser
// checklist). Each row is one finding; `tokens` are whitespace-stripped
// substrings that must be present in the named asset, `forbidden` ones must
// be absent, and `extra` carries the checks a substring cannot express.
func TestShellJS_V2FixRoundGuards(t *testing.T) {
	cases := []struct {
		name      string
		tokens    map[string][]string // asset -> whitespace-stripped tokens that must be present
		forbidden map[string][]string // asset -> whitespace-stripped tokens that must be absent
		extra     func(t *testing.T)
	}{
		{
			// Finding 1: every full re-render takes a monotonic token +
			// AbortController, and a stale/aborted response is dropped, so a slow
			// earlier response can never overwrite a newer one. An abort is
			// "superseded", never an error state; the ctx.path nav (loadPath) is
			// sequenced too.
			name: "renders are sequenced",
			tokens: map[string][]string{"interact.js": {
				"letrenderSeq=0",
				"newAbortController()",
				"if(seq!==renderSeq)return{superseded:true}",
				"fetchRender({signal})",
				"isAbortError",
				`method:"GET",credentials:"same-origin",signal,`,
			}},
		},
		{
			// Finding 2: modal deep-link state uses the reserved __dtp_modal key,
			// never a top-level param named after the block (which would clobber
			// an app filter of the same name), and the shell + server agree on
			// the literal.
			name: "modal state uses the reserved namespace",
			tokens: map[string][]string{"interact.js": {
				`constMODAL_PARAM="__dtp_modal"`,
				`RESERVED_PARAMS=["token","block",MODAL_PARAM]`,
				`setParams({[MODAL_PARAM]:param},{refresh:false})`,
				`setParams({[MODAL_PARAM]:null},{refresh:false})`,
			}},
			forbidden: map[string][]string{"interact.js": {
				`setParams({[param]:"1"}`,
				`setParams({[param]:null}`,
			}},
			extra: func(t *testing.T) {
				// The server strips the SAME literal, so the reserved key never
				// reaches ctx.params (behavior: TestReadParams_StripsModalStateKey).
				if modalStateParam != "__dtp_modal" {
					t.Errorf("server modalStateParam = %q, must match the shell's MODAL_PARAM __dtp_modal", modalStateParam)
				}
			},
		},
		{
			// Finding 3: charts register their embed result and the shell
			// finalizes live views BEFORE clearing the DOM on a re-render, so a
			// view never leaks across a swap (finalize after clear would already
			// have leaked it).
			name: "vega views are finalized before the swap",
			tokens: map[string][]string{
				"interact.js": {
					"exportfunctionregisterVegaView(",
					"exportfunctionfinalizeVegaViewsWithin(",
					"entry.result.finalize()",
					`removeEventListener("click",entry.listener)`,
				},
				"blocks/chart.js": {"registerVegaView({result,listener,mount})"},
			},
			extra: func(t *testing.T) {
				shell := readShellAsset(t, "shell.js")
				idxFinalize := strings.Index(shell, "finalizeVegaViewsWithin(root)")
				idxClear := strings.Index(shell, `root.textContent = ""`)
				if idxFinalize < 0 || idxClear < 0 {
					t.Fatalf("shell.js applyDoc is missing the finalize (%d) or clear (%d)", idxFinalize, idxClear)
				}
				if idxFinalize > idxClear {
					t.Error("shell.js applyDoc clears the root BEFORE finalizing vega views — the view leaks")
				}
			},
		},
		{
			// Finding 4: the honored Retry-After is capped to MAX_REFRESH_SECONDS
			// so a hostile value cannot overflow setTimeout's ~2^31 ms limit and
			// wrap to fire immediately — on the honored path (backoffSeconds) and
			// again as a final guard in scheduleNextRefresh.
			name: "Retry-After is capped",
			tokens: map[string][]string{"interact.js": {
				"Math.max(seconds,retryAfter)", // still honors Retry-After, up to the cap
			}},
			extra: func(t *testing.T) {
				got := stripWhitespace(readShellAsset(t, "interact.js"))
				if strings.Count(got, "seconds=Math.min(MAX_REFRESH_SECONDS,seconds)") < 2 {
					t.Error("interact.js does not cap the honored Retry-After delay to MAX_REFRESH_SECONDS (setTimeout overflow risk)")
				}
			},
		},
		{
			// V4 Finding 1: a tab switch finalizes the outgoing panel's Vega
			// views BEFORE dropping their DOM, like applyDoc does for root.
			name: "tab switch finalizes before clearing the panel",
			tokens: map[string][]string{"blocks/tabs.js": {
				`import{finalizeVegaViewsWithin}from"../interact.js"`,
			}},
			extra: func(t *testing.T) {
				src := readShellAsset(t, "blocks/tabs.js")
				idxFinalize := strings.Index(src, "finalizeVegaViewsWithin(panel)")
				idxClear := strings.Index(src, `panel.textContent = ""`)
				if idxFinalize < 0 || idxClear < 0 {
					t.Fatalf("tabs.js show() is missing the finalize (%d) or the panel clear (%d)", idxFinalize, idxClear)
				}
				if idxFinalize > idxClear {
					t.Error("tabs.js show() clears the panel BEFORE finalizing its vega views — a tab switch leaks the outgoing chart")
				}
			},
		},
		{
			// V4 Finding 2: a chart whose mount detaches before its async
			// import+embed resolves is finalized (not merely skipped), so
			// nothing leaks.
			name: "detached chart mount is finalized",
			tokens: map[string][]string{
				"blocks/chart.js": {"if(!mount.isConnected)returnundefined"},
				"interact.js": {
					"entry.mount.isConnected===false",
					"if(!entry.mount||entry.mount.isConnected===false){finalizeVegaEntry(entry);",
				},
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for asset, tokens := range tc.tokens {
				got := stripWhitespace(readShellAsset(t, asset))
				for _, want := range tokens {
					if !strings.Contains(got, want) {
						t.Errorf("%s is missing %q", asset, want)
					}
				}
			}
			for asset, tokens := range tc.forbidden {
				got := stripWhitespace(readShellAsset(t, asset))
				for _, banned := range tokens {
					if strings.Contains(got, banned) {
						t.Errorf("%s still contains %q", asset, banned)
					}
				}
			}
			if tc.extra != nil {
				tc.extra(t)
			}
		})
	}
}

// ===========================================================================
// RFC 028 Part 4 (V3): client-side CSV export.
//
// CSV export is client-side only (spec §6.3 "Client-side, no round trip …
// CSV export") — csv.js's buildCSV is a pure JS function with no server-side
// counterpart, and there is no JS runtime vendored into this module tree to
// execute it. So the SHIPPED csv.js and table.js are pinned at the source
// level: the trigger-character array, the quoting condition and the
// projection of each row through the rendered columns.
// ===========================================================================

// TestCSVModule_TriggerSetAndQuotingMatchGolden pins the SHIPPED csv.js to the
// OWASP formula-injection escape then RFC 4180 quoting rule set, at the source
// level: the trigger-character array (all six, nothing extra), the RFC 4180
// quoting condition (comma/quote/CR/LF), and the embedded-quote-doubling
// replace.
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
// CSV's field count from its header. Reverting to the pre-fix
// `visibleRows().map((row) => row.map(cellText))` FAILs this test.
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
