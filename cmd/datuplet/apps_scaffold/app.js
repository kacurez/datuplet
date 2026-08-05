/// <reference path="./datuplet.d.ts" />
// Datuplet user-app entry point (RFC 028).
//
// Authors write a plain ES module exporting `render(ctx)` — same as any
// other JS module, and typed by datuplet.d.ts alongside this file. The
// Datuplet engine never imports this file directly: `npm run build`
// (esbuild.mjs) bundles it into a single self-contained IIFE
// (`--bundle --format=iife --global-name=__dtp_app`), and the engine
// evaluates THAT bundle, calling `globalThis.__dtp_app.render(ctx)`. You
// still author and reason about this file as an ES module; the IIFE step is
// a build-time-only delivery detail (spec §6.2).
//
// Trimmed from the RFC 028 spec's worked example (spec Appendix A, in
// docs/superpowers/specs/2026-07-22-rfc-028-user-apps-wasm-workers-design.md)
// down to a single `datuplet.query` call — see that appendix for the full
// version with a daily-revenue chart and a top-products table.
export async function render(ctx) {
  const days = Number(ctx.params.days ?? 30);

  // SAFETY: ctx.params values are viewer-controlled. Always pass them
  // through the `params` bind argument below — NEVER splice them into the
  // SQL string yourself. `$days` is bound server-side as a prepared-
  // statement parameter (spec §6.1); it is never parsed as SQL.
  const kpi = await datuplet.query(
    `SELECT count(*) AS orders, sum(amount) AS revenue
       FROM sales.orders
      WHERE order_date >= current_date - $days`,
    { days },
  );
  const [orders, revenue] = kpi.rows[0];

  return {
    outputDoc: 1,
    title: "Sales overview",
    blocks: [
      {
        id: "filters",
        type: "filter",
        fields: [
          {
            name: "days",
            label: "Window",
            kind: "select",
            value: days,
            options: [
              { value: 7, label: "Last 7 days" },
              { value: 30, label: "Last 30 days" },
              { value: 90, label: "Last 90 days" },
            ],
          },
        ],
      },
      {
        id: "kpis",
        type: "metric",
        items: [
          { label: "Revenue", value: revenue, format: "currency:EUR" },
          { label: "Orders", value: orders },
        ],
      },
      {
        id: "footer",
        type: "markdown",
        text: `_${(kpi.stats.rows_scanned ?? 0).toLocaleString()} rows scanned · rendered server-side at ${new Date(ctx.now).toISOString().slice(0, 16)}Z_`,
      },
    ],
  };
}
