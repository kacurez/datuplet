// Ambient type declarations for the Datuplet user-app guest runtime
// (RFC 028 spec §5.5, §6.1, §6.2, §6.3). Reference this file from your
// editor/tsconfig ("include": ["*.d.ts", "app.js"], or a
// `/// <reference path="./datuplet.d.ts" />` comment — already present at
// the top of app.js) so `ctx`, the global `datuplet` object, and the
// OutputDoc block types are typed while you write your app.
//
// This is documentation only: the QuickJS engine never reads this file, tsc
// is never invoked by the build (esbuild.mjs bundles plain JS), and nothing
// here is uploaded. It exists so a human or an agent can code against a
// precise, machine-readable contract instead of re-deriving these shapes
// from the spec prose.

/**
 * The argument your exported `render` function receives:
 *
 *   export async function render(ctx: RenderContext): Promise<OutputDoc> { ... }
 */
export interface RenderContext {
  /**
   * Flat string -> string map (spec §6.5): URL query params merged with
   * (and overridden by) the JSON body of a re-render POST. No arrays, no
   * nesting, no type coercion — parse your own numbers/booleans, as app.js
   * does with `Number(ctx.params.days ?? 30)`. Reserved keys (`token`,
   * `block`) are stripped before delivery.
   */
  params: Record<string, string>;
  /**
   * The sub-path after /apps/{pid}/{name}, normalized (no `..`, no encoded
   * separators), <=256 chars. Empty string at the app's root path.
   */
  path: string;
  /** Server render time, milliseconds since the Unix epoch. */
  now: number;
}

/** One column of a query result. */
export interface QueryColumn {
  name: string;
  type: string;
}

/** Best-effort execution stats attached to every query result. */
export interface QueryStats {
  duration_ms: number;
  rows_scanned?: number;
  bytes_scanned?: number;
}

/** The resolved value of a `datuplet.query()` call. */
export interface QueryResult {
  /** Column order matches each row's cell order. */
  schema: QueryColumn[];
  /** Row-major result data. */
  rows: unknown[][];
  /** True when the result was cut off by `maxRows` (opts) or the server cap. */
  truncated: boolean;
  stats: QueryStats;
}

/**
 * Scalar bind values only (spec §6.1) — arrays/structs are not supported.
 * Integral numbers with `|n| > 2^53-1` are rejected; pass those as strings
 * with an explicit SQL `CAST` instead.
 */
export type QueryParamValue = string | number | boolean | null;

export interface QueryOptions {
  /** Row cap for this query (<= the render's configured max). */
  maxRows?: number;
}

declare global {
  const datuplet: {
    /**
     * Run a read-only SQL query against the project's warehouse.
     *
     * `sql` uses named placeholders matching
     * `$[A-Za-z_][A-Za-z0-9_]{0,63}` (case-sensitive, may repeat). Every
     * placeholder in `sql` must have a matching key in `params`, and every
     * `params` key must be referenced by at least one placeholder —
     * unreferenced keys are rejected as a typo defence. Values are bound as
     * prepared-statement parameters; they are NEVER parsed as SQL.
     *
     * SAFETY: never interpolate a `ctx.params` value into the `sql` string
     * yourself. Build the query structure (which columns, which optional
     * clauses) in your own code, then pass viewer-controlled values through
     * `params` — see app.js's `render()` for the pattern: the SQL always
     * references `$days` and only `$days`; the bind object supplies the
     * actual number.
     *
     * Rejects (as a rejected Promise, `err.kind` set): missing/unknown/
     * unreferenced/mistyped params ("bad_request"); bind/prepare failures
     * ("sql_error").
     */
    query(
      sql: string,
      params?: Record<string, QueryParamValue>,
      opts?: QueryOptions,
    ): Promise<QueryResult>;
  };
}

// ---------------------------------------------------------------------
// OutputDoc v1 — the return type of `render()` (spec §6.3).
// Mirrors pkg/appengine/outputdoc/schema.json field-for-field; keep in
// sync by hand if that schema changes.
// ---------------------------------------------------------------------

/** The value `render()` must return (directly, or via its resolved Promise). */
export interface OutputDoc {
  outputDoc: 1;
  title: string;
  /** <=64 blocks total (including nested tabs/modal blocks); doc <=2 MiB serialized. */
  blocks: Block[];
  /** Auto-refresh interval in seconds; clamped server-side to [15, 3600]. */
  refreshInterval?: number;
}

export type Block =
  | MarkdownBlock
  | MetricBlock
  | TableBlock
  | ChartBlock
  | FilterBlock
  | TabsBlock;

/** Declarative cross-filtering: sets `param` and re-renders on click. */
export interface OnClick {
  param: string;
}

/** Inline modal content, or a lazy modal fetched as a partial render. */
export type Modal = { title: string; blocks: Block[] } | { param: string };

interface BlockBase {
  /** Unique within the doc — the `?block=<id>` partial-render key. */
  id: string;
  modal?: Modal;
}

export interface MarkdownBlock extends BlockBase {
  type: "markdown";
  /** Rendered via marked, then DOMPurify-sanitized; raw HTML is stripped. */
  text: string;
}

export interface MetricItem {
  label: string;
  value: unknown;
  /** Free-form, interpreted client-side, e.g. "currency:EUR". */
  format?: string;
}

export interface MetricBlock extends BlockBase {
  type: "metric";
  items: MetricItem[];
}

/** A table row: either a plain cell array, or an object carrying a modal. */
export type TableRow = unknown[] | { cells: unknown[]; modal?: Modal };

export interface TableBlock extends BlockBase {
  type: "table";
  title?: string;
  columns: string[];
  rows: TableRow[];
}

export interface ChartBlock extends BlockBase {
  type: "chart";
  library: "vega-lite";
  title?: string;
  /**
   * A restricted Vega-Lite subset, validated server-side against
   * pkg/appengine/vegaspec/schema.json (spec §6.4): inline `data.values`
   * only (no `data.url`/named datasets/generators); single-view only (no
   * `layer`/`facet`/`concat`/`repeat`/`resolve`); no `config`/`usermeta`;
   * mark types exclude `image`; no `href` anywhere (mark or encoding); no
   * `lookup` transform. Left as `unknown` here — see the schema for the
   * exact allowed key sets per section.
   */
  spec: unknown;
  onClick?: OnClick;
}

export type FilterOption =
  | string
  | number
  | boolean
  | { value: string | number | boolean; label: string };

export interface FilterField {
  name: string;
  label: string;
  /** Free-form widget kind (e.g. "select", "text") — interpreted client-side. */
  kind: string;
  value?: unknown;
  options?: FilterOption[];
}

export interface FilterBlock extends BlockBase {
  type: "filter";
  fields: FilterField[];
}

export interface TabsBlock extends BlockBase {
  type: "tabs";
  /** All tabs' blocks are delivered together; the shell switches client-side. */
  tabs: { label: string; blocks: Block[] }[];
}
