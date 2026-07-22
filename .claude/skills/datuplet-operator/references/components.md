# Built-in components

`datuplet components list --json` is the source of truth for what's actually
deployed on the cluster you're operating — always run it; the set below is the
usual built-in catalog, not a guarantee. For any component you'll use, run
`datuplet components get <name> --schema` and conform your `config` to that
schema (required fields, types, and `x-datuplet-secret` markers).

## The `io` capability

Each component declares an input/output capability. It tells you where a
component can sit in a pipeline:

- `inputs: none` — a **source**; it produces tables but reads none (put it in an
  early stage). `inputs: required` — a **transform/sink**; it must read tables an
  earlier stage produced. `inputs: optional` — either.
- `outputs: required` — writes tables (address them in `outputs`).
  `outputs: none` — writes nothing to storage (e.g. a debug sink).

## Catalog

| Component | io (inputs/outputs) | What it's for | Key config (verify with `--schema`) |
|---|---|---|---|
| `http-json-extractor` | none / required | Fetch JSON from an HTTP endpoint into a table; single-request or paginated. **The workhorse for ingesting an external API.** | `url` (required); `table_name`; pagination options |
| `data-generator` | none / required | Generate random or literal rows inline from the pipeline itself — no external source. **Use for testing/bootstrapping a pipeline.** | `tables` (required): the tables + rows/columns to synthesize |
| `sql-transform` | required / required | Run user SQL in an embedded DuckDB engine over the input tables and write result tables. Credentials-clean. **The workhorse for transform/aggregate/join.** | `sql` (required): `CREATE TABLE <out> AS SELECT …` |
| `finnhub-extractor` | none / required | Fetch market data (quotes, news, financials, earnings, …) from the Finnhub API. | `mode` (required); `apiKey` (secret → `$[finnhub_key]`); `symbols`, `category`, `lookback_days`, `limit` |
| `stdout-writer` | required / none | Read input tables and print to stdout. **Debugging only — no storage output.** | (minimal) |
| `pandas-transform` | required / required | Apply a sequence of pandas ops (filter/select/sort/rename/drop/fillna). May not be enabled on every deployment — check `components list`. | operation list |

## Choosing components for a goal

- **"Get data from an API/source into Datuplet"** → `http-json-extractor` (or
  `finnhub-extractor` for market data) in the first stage, writing to a `raw`
  bucket.
- **"Aggregate / summarize / reshape"** → `sql-transform` in a later stage,
  reading the raw table(s), writing a summary table. Express the logic as
  `CREATE TABLE <name> AS SELECT …`.
- **"Combine two datasets"** → extract both (parallel components in the extract
  stage), then one `sql-transform` with both as `inputs.tables` doing a JOIN.
- **"Just test the plumbing without a real source"** → `data-generator`.
- **"See the rows while debugging"** → `stdout-writer` (its output shows in the
  component's run logs, not storage).

## SQL notes for `sql-transform`

- Reference each input table by its `table` name (the DuckDB table is named
  after the input's `table`).
- The output table name in `CREATE TABLE <name> AS …` should match the `name`
  in that component's `outputs.tables[]`.
- JSON/extractor columns are often camelCase and need quoting in SQL:
  `"userId"`, `"createdAt"`.

See `references/scenarios.md` for these components wired into complete,
validated pipelines.
