// tests/e2e/scenarios/testdata/apps/sales/app.js
//
// PRE-BUNDLED fixture bundle for the RFC 028 user-apps viewer + security e2e
// (Task D2). esbuild is NOT run in CI (task-D0-report.md §D5 / plan lines
// 1543-1545), so THIS FILE IS THE COMMITTED IIFE ARTIFACT the engine evaluates
// directly — it is not authored-ES-module source and is uploaded verbatim
// (after the token substitution described below) as the app bundle.
//
// SOURCE. Hand-derived from the CLI scaffold template that `datuplet apps
// init` emits — cmd/datuplet/apps_scaffold/app.js — which is itself trimmed
// from spec Appendix A ("Sales overview"). The scaffold's ES-module
//   export async function render(ctx) { ... }
// is wrapped here as the IIFE that
//   esbuild app.js --bundle --format=iife --global-name=__dtp_app --outfile=bundle.js
// produces for a single-file app with no imports: a top-level
//   var __dtp_app = (() => { ...; return { render }; })();
// which, eval'd as a script by the engine, becomes globalThis.__dtp_app. The
// engine concatenates pkg/appengine/prelude.js + this bundle and reaches the
// entry at globalThis.__dtp_app.render(ctx) (pkg/appengine/engine.go Render,
// pkg/appengine/prelude.js __dtp_run). Regenerate by running that esbuild
// command over the scaffold module if the ABI ever changes.
//
// TOKEN SUBSTITUTION (author/operator config, NOT viewer input). Three string
// tokens are replaced by the e2e harness (scenarios_apps_test.go) before
// upload. They are the seeded warehouse-table identifier and the app version
// marker — supplied by the test acting as the app's author, never by a viewer:
//   __DTP_E2E_NS__      -> the seeded Iceberg namespace  ("<runPrefix>-api")
//   __DTP_E2E_TABLE__   -> the seeded table              ("data")
//   __DTP_APP_VERSION__ -> "v1" or "v2" (distinct bytes => distinct content
//                          hash => a genuine second immutable version, which
//                          drives the promote-propagation assertion)
// This is placeholder substitution on a committed bundle — directly analogous
// to the e2e's RenderPipeline TemplateVars on committed pipeline YAML — NOT a
// build step. No esbuild, npm, or node runs at test time.
//
// INJECTION-SAFETY — the property the D2 scenario asserts. The ONLY
// viewer-controlled input, ctx.params.country, is passed EXCLUSIVELY through
// the datuplet.query() `params` bind argument as $country; it is never spliced
// into a SQL string. The query STRUCTURE (whether the optional WHERE clause is
// present) is author-chosen exactly as spec Appendix A / §6.1 model. So a
// viewer opening `?country=' UNION SELECT ...` binds that text as a literal
// VARCHAR — it matches zero rows and cannot alter the query — while the
// unfiltered count in the same render is unaffected, proving no rows were
// injected. The two table/namespace tokens above are substituted at author
// time (not per request), so they are never a viewer-reachable splice.
var __dtp_app = (() => {
  "use strict";
  const NS = "__DTP_E2E_NS__";
  const TBL = "__DTP_E2E_TABLE__";
  const VERSION = "__DTP_APP_VERSION__";

  async function render(ctx) {
    const from = `"${NS}"."${TBL}"`;
    const country = ctx.params.country ?? "ALL";

    // Unfiltered total. Proves the render reached the REAL warehouse table
    // (the D2 scenario asserts this metric == the seeded row count) and is a
    // control for the injection subtest: it is NOT filtered by $country, so
    // it stays constant no matter what the viewer passes.
    const total = await datuplet.query(`SELECT count(*) AS cnt FROM ${from}`);
    const cnt = total.rows[0][0];

    // Filtered sample. country === "ALL" => no clause and NO $country bind
    // (spec §6.1: every placeholder needs a key AND every key a placeholder —
    // an unreferenced bind key is rejected). Otherwise the viewer value is
    // bound as $country: a literal, never SQL. `title` is a real column of the
    // seeded table (schema userId/id/title/body), so a garbage country binds
    // cleanly and simply matches nothing — no binder error, no error leak.
    const clause = country === "ALL" ? "" : "WHERE title = $country";
    const bind = country === "ALL" ? {} : { country };
    const sample = await datuplet.query(
      `SELECT id, title FROM ${from} ${clause} ORDER BY id LIMIT 5`,
      bind,
    );

    return {
      outputDoc: 1,
      title: `Sales overview ${VERSION}`,
      blocks: [
        { id: "kpis", type: "metric", items: [{ label: "Rows", value: cnt }] },
        {
          id: "sample",
          type: "table",
          title: "Sample rows",
          columns: ["id", "title"],
          rows: sample.rows,
        },
        {
          id: "footer",
          type: "markdown",
          text: `_channel ${VERSION}; country=${country}; ${cnt} rows scanned_`,
        },
      ],
    };
  }

  return { render: render };
})();
