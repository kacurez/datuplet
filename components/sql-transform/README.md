# sql-transform

A Datuplet component that runs user-supplied SQL inside an embedded DuckDB engine, reading inputs and writing outputs through the DataGateway sidecar. RFC 010 v3 — DG-mediated I/O. The component holds zero S3 / lakekeeper credentials; DG owns the data plane.

## Quick start

```yaml
- name: transform
  components:
    - name: sql-transform
      image: datuplet/sql-transform:latest
      inputs:
        tables:
          - bucket: raw
            table: orders
          - bucket: raw
            table: customers
      outputs:
        tables:
          - name: order_summary
            bucket: curated
            writeMode: FULL_LOAD
      config:
        sql: |
          CREATE TABLE order_summary AS
          SELECT
            c.country,
            SUM(o.total) AS total_revenue,
            COUNT(*) AS order_count
          FROM orders o
          JOIN customers c ON o.customer_id = c.id
          GROUP BY c.country;
```

The SQL string contains zero environment-specific values: no S3 URLs, no bucket prefixes, no run IDs. Inputs and outputs are referenced by their declared logical name only.

## How inputs and outputs map to SQL

The component is a thin shell around DuckDB. Before your SQL runs, the component pre-registers each declared input as a **DuckDB view**. After your SQL completes, the component reads back each declared output by **selecting from a DuckDB table or view your SQL created** and streams the rows to DG.

**Inputs → DuckDB views.** Each declared input is registered as a DuckDB view. `<view_name>` is `logicalName` if set, otherwise the physical `table`. Inputs stream from DataGateway directly into DuckDB via the Arrow extension's `arrow_scan` registration — no parquet files are written to the component's local disk. Internally:

1. Component opens an `OpenReader(FORMAT_ARROW_IPC)` on the input.
2. SDK wraps the gRPC stream as an `array.RecordReader`.
3. Component calls `Arrow.RegisterView(reader, viewName)` and runs the user SQL.

Your SQL then uses `<view_name>` directly:

```yaml
inputs:
  tables:
    - bucket: raw
      table: orders                # → DuckDB view  orders
    - bucket: raw
      table: products
      logicalName: prod            # → DuckDB view  prod   (NOT products)
config:
  sql: |
    -- Note: FROM uses the logicalName/table name, NEVER the bucket.
    -- "FROM raw.orders" or "FROM raw.products" would fail to resolve.
    SELECT o.*, prod.name AS product_name
    FROM orders o
    JOIN prod ON o.product_id = prod.id;
```

No parquet files are written to the component's local disk; the input streams from DG into DuckDB via the Arrow IPC stream.

**Outputs → DuckDB CREATE TABLE.** For each `outputs.tables[]` entry, the component runs (roughly):

```sql
COPY <sdk_name> TO '<staged>.parquet' (FORMAT PARQUET, COMPRESSION SNAPPY);
```

`<sdk_name>` is `logicalName` if set, otherwise `name`. You're responsible for **creating a DuckDB table or view by that exact name** somewhere in your SQL:

```yaml
outputs:
  tables:
    - name: order_details          # → component does  COPY order_details TO ...
      bucket: curated
      writeMode: APPEND
    - name: revenue_by_country     # → component does  COPY revenue_by_country TO ...
      bucket: curated
      logicalName: revenue         # → component does  COPY revenue TO ...        (logical override)
      writeMode: FULL_LOAD
config:
  sql: |
    -- Output #1: must produce a DuckDB table/view named `order_details`.
    CREATE TABLE order_details AS
    SELECT o.id AS order_id, c.name AS customer_name, o.total
    FROM orders o
    JOIN customers c ON o.customer_id = c.id;

    -- Output #2: logicalName is `revenue`, so create `revenue` (NOT revenue_by_country).
    CREATE TABLE revenue AS
    SELECT c.country, SUM(o.total) AS total_revenue
    FROM orders o
    JOIN customers c ON o.customer_id = c.id
    GROUP BY c.country;
```

Iceberg-side: each `outputs.tables[]` entry's `(bucket, name)` is the **physical** target (lakekeeper namespace + table). `logicalName` only changes what the user writes inside the SQL string.

**What if I forget to create an output table?** The `COPY <sdk_name> TO ...` fails with a DuckDB error and the component exits with `FailedUser`.

**What if I create extra DuckDB tables that aren't declared as outputs?** They're discarded when the component exits — DuckDB is in-memory (with optional spill to `/tmp/duckdb-spill`). Use them freely as intermediates.

**What if an input has zero rows?** The view is registered as a 0-row view shaped to the input schema; SQL that joins/aggregates against it still resolves columns and naturally produces empty output.

## Inputs

`inputs.tables[]` declares the iceberg tables the SQL can read. Each entry maps to a DuckDB view registered before the SQL runs.

| Field | Required | Description |
|---|---|---|
| `bucket` | yes | Iceberg namespace (e.g. `raw`). |
| `table` | yes | Iceberg table name. |
| `logicalName` | no | Override the SQL identifier the view is registered under. Defaults to `table`. Use this when two inputs share the same physical name across buckets, or when SQL readability matters more than matching the warehouse layout. |
| `sinceDays` | no | Numeric; reads only rows whose `timestampColumn` falls within the last N days. Soft-skipped in lakekeeper mode today (DG logs a warning + falls back to a full snapshot read); the field is parsed and forwarded so the wiring is in place once iceberg-go column-predicate filtering ships. |
| `timestampColumn` | no | Column name to filter against when `sinceDays` is set. Defaults to `created`. |
| `since` | no | Duration string form of `sinceDays` (e.g. `"30m"`, `"12h"`, `"2d"`). Same caveat. |
| `sinceSnapshot` | no | Iceberg snapshot ID for incremental snapshot-history reads. Same caveat. |

### Logical names

```yaml
inputs:
  tables:
    - bucket: raw
      table: products
      logicalName: prod         # SQL: FROM prod  (not  FROM products)
    - bucket: raw
      table: orders             # SQL: FROM orders  (logicalName defaults to table)
```

## Outputs

`outputs.tables[]` declares the iceberg target tables the SQL produces. The user must `CREATE TABLE <sdk-name>` (or `CREATE OR REPLACE TABLE`) inside the SQL for each declared output; the component reads that DuckDB table after the SQL completes and streams it to DG.

| Field | Required | Description |
|---|---|---|
| `name` | yes | Iceberg target table (and the default SDK identifier the user writes `CREATE TABLE` against). |
| `bucket` | yes | Iceberg namespace. |
| `writeMode` | no | `APPEND` (default) or `FULL_LOAD`. `FULL_LOAD` replaces the entire table; concurrent runs follow last-writer-wins-with-409-retry. |
| `logicalName` | no | Override the SDK identifier so the user's `CREATE TABLE <logicalName>` doesn't have to match the physical table name. |
| `partitionFields` | no | Iceberg partition spec (Hive-style; passed through to lakekeeper at table-create time). |

### Logical names on outputs

```yaml
outputs:
  tables:
    - name: order_details         # iceberg table = curated.order_details
      bucket: curated
      logicalName: details        # user writes  CREATE TABLE details AS …
      writeMode: FULL_LOAD
```

## Empty-input semantics

When a declared input table has zero rows for the current run (first incremental window, upstream extractor produced no rows, etc.), DG's streaming reader emits a 0-row Arrow record batch shaped to the input schema. DuckDB's `arrow_scan` registers it as a view that resolves columns but yields no rows. Downstream `JOIN` / aggregation SQL still resolves columns and naturally produces empty output.

## Architecture (for the curious)

The component is intentionally creds-clean. Per-step:

1. **Inputs.** For each declared input, DG opens the input via `OpenReader(FORMAT_ARROW_IPC)` and row-group-streams parquet→arrow IPC. The SDK's `sdk/go/arrow.NewReader` wraps the gRPC stream as an `array.RecordReader`. The component calls `arrow.RegisterView(reader, viewName)` to register the input as a DuckDB view via the arrow_scan extension. **No parquet files touch the component's local disk.**
2. **SQL.** The user's SQL string runs verbatim against the view set. DuckDB executes it in-process; intermediate `CREATE TABLE …` statements stay in DuckDB local memory or spill to `/tmp/duckdb-spill` (configurable via `config.temp_directory`).
3. **Outputs.** For each declared output, the component runs `COPY <sdk-name> TO '<staged>.parquet' (FORMAT PARQUET, COMPRESSION SNAPPY)`, reads the staged parquet, and streams it to DG via `OpenWriterToBucket` + `WriteChunk` + `Close`. DG re-emits the parquet to the iceberg target prefix using lakekeeper-vended STS creds, and writes the per-target `files.json` as an audit breadcrumb.
4. **Commit.** Inline in the DG sidecar, outside the component. CloseWriter dispatches the writer's parquet paths to the DG's in-process commit pool, which calls `icebergjob.CommitTableFiles`: it opens an iceberg transaction, calls `txn.AddFiles` (APPEND) or `txn.ReplaceDataFiles` (FULL_LOAD), and commits via lakekeeper. There is no separate commit Job. 409 conflicts retry via `catalogwriter.RetryOnConflict`.

The component process never holds production S3 credentials; DG's `pkg/catalogwriter/VendedCreds` handles per-table STS scoping + auto-refresh at 50%-elapsed TTL.

## DuckDB tuning

```yaml
config:
  sql: |
    …
  threads: 4                  # DuckDB SET threads=N (default 4)
  temp_directory: /tmp/spill  # DuckDB spill-to-disk dir (default /tmp/duckdb-spill)
```

`threads` and `temp_directory` are POC-level escape hatches — most users won't set them. K8s components have bounded ephemeral storage; very large intermediates can OOM the pod. Size pod resource requests accordingly.

## Failure semantics

- **Bad SQL / missing column** → component exits 1 (`FailedUser`); operator marks the run failed; no commit happens.
- **DG / lakekeeper unavailable, transient network failure** → component exits ≥20 (`FailedApplication`); same teardown.
- **Identifier rejected** (logical or physical name doesn't match `^[A-Za-z_][A-Za-z0-9_]*$`) → `FailedUser` at component startup.
- **Run cancelled** → DG's gateway sidecar's annotation watcher fires; in-flight writers close cleanly; partial parquet at target prefixes are orphans (sweepable later).

## Limitations (POC)

- No UPSERT. `txn.AddDeletes` plumbing is sketched in RFC 010 §"Tier 3 deferrals" but not shipped.
- Column-predicate `sinceDays` is wired through CRD + DG config but not yet honoured by the lakekeeper read path; DG soft-skips and reads the full snapshot. Tracked as a follow-up.
- Schema evolution: an output `CREATE TABLE` whose schema diverges from the existing iceberg target's schema fails at `txn.AddFiles` time as `FailedUser`. No auto-evolve.
- Partitioned writes: DG's writer doesn't currently re-partition output rows by the target's partition spec. Lands with the partitioned-writes follow-up.
- Memory: streaming inputs no longer require ephemeral storage for input data; only DuckDB's spill (configurable via `temp_directory`) touches the pod's ephemeral disk. POC posture; document via pod resource requests.
