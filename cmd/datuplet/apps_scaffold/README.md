# Datuplet user app

Scaffolded by `datuplet apps init`. Full design: RFC 028 —
`docs/superpowers/specs/2026-07-22-rfc-028-user-apps-wasm-workers-design.md`
(§5.5 covers this CLI workflow end to end; §6 covers the runtime contract
below; Appendix A is the full worked example this scaffold is trimmed from).

## Files

- `app.js` — your app. Export an async `render(ctx)` that returns an
  OutputDoc (a JSON-serializable description of blocks — markdown, metric,
  table, chart, filter, tabs). Query the project's warehouse via the global
  `datuplet.query(sql, params, opts)`. This is a plain ES module; you never
  run it directly.
- `datuplet.d.ts` — TypeScript declarations for `ctx`, `datuplet.query`, and
  every OutputDoc block type. Reference it from your editor for
  autocomplete/hover docs. It is documentation only: never bundled, never
  uploaded, never read by the engine.
- `esbuild.mjs` / `package.json` — the build step. The engine evaluates a
  bundled IIFE, not `app.js` directly (see the comment at the top of
  `esbuild.mjs` for why).

## Workflow

```sh
npm install
npm run build                                        # app.js -> bundle.js

datuplet apps put <name> --bundle bundle.js           # -> draft, prints {app_id, version_hash}
datuplet apps render <name> --channel draft --param days=7 --json   # smoke-test: OutputDoc or a structured error
datuplet apps promote <name> --version <hash>         # draft -> production, once you're happy
datuplet apps token create <name>                     # prints a one-time viewer token to share
```

Repeat edit -> build -> put -> render for each iteration; nothing here needs
a browser (`apps render` runs the same route, same engine, same limits as a
real viewer request — only the response framing differs). `--project <pid>`
is required on the first command and resolves like every other `datuplet`
subcommand: `--project` flag, then `$DATUPLET_PROJECT`, then your logged-in
cluster's default project.

`datuplet apps get <name>` / `list` / `delete` manage metadata, channels,
and versions; `datuplet apps logs <name> --request-id <id>` fetches one
render's captured `console.log` output and error/stack, keyed by the same
request id a failed `render` prints.

## Rules the engine enforces

- Bundle size: <=5 MB raw, checked locally before upload and again
  server-side.
- Render wall clock: 10 s default, 30 s hard cap.
- Queries per render: 10 default, 25 hard cap.
- OutputDoc: <=64 blocks, <=2 MiB serialized.
- `ctx.params` values are viewer-controlled strings: always pass them
  through `datuplet.query`'s `params` bind argument — never splice them into
  a SQL string. `app.js` models the safe pattern.
