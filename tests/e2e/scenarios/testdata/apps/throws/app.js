// tests/e2e/scenarios/testdata/apps/throws/app.js
//
// PRE-BUNDLED fixture bundle for the RFC 028 D3 agent-loop e2e's
// FAILURE-CASE subtest. Same discipline as ../sales/app.js (task-D0-report.md
// §D5 / plan lines 1543-1545: esbuild is NOT run in CI) — this file IS the
// committed IIFE artifact the engine evaluates directly, uploaded verbatim.
//
// SOURCE / SHAPE. Hand-written directly to the IIFE shape
//   esbuild <src> --bundle --format=iife --global-name=__dtp_app
// produces for a single-file app with no imports (identical wrapping to
// ../sales/app.js and to the `datuplet apps init` scaffold's app.js after a
// real build): a top-level `var __dtp_app = (() => { ...; return { render };
// })();`, which the engine's prelude.js reaches at
// globalThis.__dtp_app.render (pkg/appengine/engine.go, pkg/appengine/prelude.js).
//
// PURPOSE. render() unconditionally throws. Verified (by reading, not
// assumed) against the real failure path:
//   - pkg/appengine/engine_test.go's own throw fixture
//     (`throw new Error("boom")`) proves a guest throw yields
//     RenderError{Kind:"render_error", Msg: <contains "boom">}.
//   - pkg/appengine/prelude.js chains `Promise.resolve().then(() =>
//     __dtp_app.render(ctx)).catch(...)`, so an ASYNC throw (a rejected
//     promise, as used here) is caught identically to a sync throw — the
//     caught error's `.message` becomes `gr.Error` verbatim (no "Error: "
//     prefix, no wrapping).
//   - pkg/appworker/server.go's failRender default branch maps any
//     non-{bad_request,timeout,rate_limited,capacity,unavailable} engine kind
//     (i.e. "render_error") to HTTP 500 + the §8 envelope
//     {error:"the app failed to render", kind:"render_error", request_id}.
//   - pkg/appworker/render.go's appendRenderLog sets the async render-log
//     record's outcome = rerr.Kind ("render_error") and
//     error = renderErrorText(rerr) (rerr.Msg [+ "\n\nstack:\n"+stack]) — so
//     the THROW_MARKER string below reaches the author log's `error` field,
//     which is how the agent-loop test proves the fetched author_log is the
//     MATCHING record for this render, not a stale/unrelated one.
//
// No datuplet.query() call and no ctx dependency — this fails identically on
// every render regardless of channel/params/warehouse state, so (unlike
// ../sales/app.js) it needs NO NS/TABLE/VERSION token substitution.
var __dtp_app = (() => {
  "use strict";
  async function render(ctx) {
    throw new Error(
      "D3-AGENTLOOP-THROW-MARKER: intentional render failure fixture (tests/e2e/scenarios/testdata/apps/throws/app.js)",
    );
  }
  return { render: render };
})();
