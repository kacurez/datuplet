// ui/appshell/blocks/markdown.js — RFC 028 Part 4 (V1) markdown block renderer.
//
// Security (spec §6.4): a markdown block's `text` is UNTRUSTED, app-authored
// input. It is parsed by marked, then sanitized by DOMPurify with a FIXED
// allowlist config — no raw HTML pass-through, no `style` attributes, links
// restricted to http:/https:/mailto: and stamped rel="noopener nofollow" — and
// the sanitized result enters the DOM ONLY as a DOMPurify-returned DOM fragment
// (RETURN_DOM_FRAGMENT), so this file never assigns innerHTML. The DOMPurify
// allowlist is what enforces "no raw HTML pass-through": any tag/attr not below
// is dropped, whether it arrived via markdown syntax or literal HTML in source.
//
// Lazy loading (spec §6.4 CSP `script-src 'self'` + RFC 028 V1 maintainer
// decision): marked and DOMPurify are dynamic-import()ed on the FIRST markdown
// block, from the SAME-ORIGIN /apps/-/shell/vendor/ path only — never a CDN.
// The import() promise is cached so later markdown blocks reuse the one load.

// DOMPurify allowlist — the FIXED config spec §6.4 mandates. Authored here as
// platform policy; never derived from or influenced by app input. `script`,
// `iframe`, `style`, `object`, `svg`, event handlers, etc. are absent, so they
// are dropped: that absence IS the "no raw HTML pass-through" guarantee.
const ALLOWED_TAGS = [
  "a", "p", "br", "hr",
  "h1", "h2", "h3", "h4", "h5", "h6",
  "strong", "em", "b", "i", "del", "s", "sup", "sub",
  "code", "pre", "blockquote",
  "ul", "ol", "li",
  "table", "thead", "tbody", "tfoot", "tr", "th", "td",
];
// href/title only. `style` is NOT here (and is forbidden explicitly below,
// belt-and-braces); no event handlers, no `src`, no `class`/`id`.
const ALLOWED_ATTR = ["href", "title"];
// Link scheme allowlist: http/https/mailto ONLY (spec §6.4). Anything else
// (javascript:, data:, tel:, vbscript:, …) is dropped from href.
const ALLOWED_URI_REGEXP = /^(?:https?|mailto):/i;

let libsPromise = null;

// loadLibs dynamic-imports marked + DOMPurify from the same-origin vendor path
// on first use and installs the link-hardening hook exactly once. The literal
// specifiers keep the same-origin invariant lexically obvious (and Go-testable).
function loadLibs() {
  if (libsPromise) return libsPromise;
  libsPromise = Promise.all([
    import("/apps/-/shell/vendor/marked.min.js"),
    import("/apps/-/shell/vendor/purify.min.js"),
  ]).then(() => {
    const marked = window.marked;
    const DOMPurify = window.DOMPurify;
    installLinkHardeningHook(DOMPurify);
    return { marked, DOMPurify };
  });
  return libsPromise;
}

let hookInstalled = false;

// installLinkHardeningHook stamps every surviving <a href> with
// rel="noopener nofollow" (spec §6.4) and target="_blank" (so `noopener` is
// meaningful and links do not navigate the shell away). It runs in
// afterSanitizeAttributes, so the attributes it sets are not re-filtered by the
// ALLOWED_ATTR allowlist. Registered once — DOMPurify hooks are global to the
// singleton, and every sanitize call in the shell wants the identical hardening.
function installLinkHardeningHook(DOMPurify) {
  if (hookInstalled || !DOMPurify || typeof DOMPurify.addHook !== "function") return;
  DOMPurify.addHook("afterSanitizeAttributes", (node) => {
    if (node.tagName === "A" && node.hasAttribute("href")) {
      node.setAttribute("rel", "noopener nofollow");
      node.setAttribute("target", "_blank");
    }
  });
  hookInstalled = true;
}

// renderMarkdown returns the block container synchronously (with a brief loading
// state) and fills in the sanitized fragment once marked + DOMPurify resolve.
export function renderMarkdown(block) {
  const el = document.createElement("div");
  el.className = "dtp-block dtp-block-markdown";

  const body = document.createElement("div");
  body.className = "dtp-markdown-body";
  body.textContent = "…"; // loading state until marked/DOMPurify resolve
  el.appendChild(body);

  const text = block && typeof block.text === "string" ? block.text : "";

  loadLibs()
    .then(({ marked, DOMPurify }) => {
      const dirty = marked.parse(text);
      // RETURN_DOM_FRAGMENT: DOMPurify hands back an already-sanitized DOM
      // fragment we append — so no innerHTML is ever assigned in the shell.
      const fragment = DOMPurify.sanitize(dirty, {
        ALLOWED_TAGS,
        ALLOWED_ATTR,
        ALLOWED_URI_REGEXP,
        FORBID_ATTR: ["style"],
        ALLOW_DATA_ATTR: false,
        RETURN_DOM_FRAGMENT: true,
      });
      body.textContent = "";
      body.appendChild(fragment);
    })
    .catch(() => {
      // Fail visible-but-inert: never inject unsanitized text as markup. The
      // shared dtp-error-card class (RFC 028 V3) gives this a distinct,
      // intentional look rather than plain muted paragraph text.
      body.textContent = "This content could not be displayed.";
      body.classList.add("dtp-error-card");
    });

  return el;
}
