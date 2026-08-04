// Package appshell embeds RFC 028 Part 4's trusted viewer shell: the static
// assets app-worker serves to render an OutputDoc in the browser (spec §6.3,
// §6.4). ui/appshell/ is vanilla ES modules with no build step (mirrors
// ui/product/) — everything under Assets is exactly what ships to a viewer's
// browser.
//
// Two embeds, not one: IndexHTML is a SERVER-SIDE TEMPLATE (its title/doc
// placeholders are substituted by pkg/appworker's writeShell before the
// response is sent) and must never be served verbatim as a static file,
// whereas everything in Assets is served byte-for-byte at /apps/-/shell/*.
//
// go:embed patterns cannot climb out of the directory containing the source
// file (no ".." path elements), so this package — physically inside
// ui/appshell/, mirroring how pkg/appengine/embed/embed.go sits next to
// engine.wasm — is what lets pkg/appworker (a sibling directory two levels
// up) pull these files into the compiled binary at all.
package appshell

import "embed"

// IndexHTML is the shell page HTML template. Never served directly — see
// pkg/appworker.writeShell, which substitutes the title/doc placeholders.
//
//go:embed index.html
var IndexHTML []byte

// Assets is the servable static subtree mounted at /apps/-/shell/ by
// app-worker: the boot module, the base stylesheet, the vendored
// third-party libraries (see vendor/VERSIONS — NOT yet the genuine upstream
// builds, see that file's STATUS section), and the client-side
// defense-in-depth copy of the restricted Vega-Lite subset schema, kept
// byte-identical to pkg/appengine/vegaspec/schema.json by `make
// sync-appshell-schema` and TestVegaSchemaInSyncWithShell.
//
// index.html is deliberately NOT in this set (see IndexHTML above).
//
//go:embed shell.js theme.css vegaspec.schema.json
//go:embed vendor/vega.min.js vendor/vega-lite.min.js vendor/vega-embed.min.js
//go:embed vendor/purify.min.js vendor/marked.min.js
var Assets embed.FS
