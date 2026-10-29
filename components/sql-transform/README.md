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

## DuckDB engine

The component embeds DuckDB **2.0.0** (`duckdb-go/v2 v2.20000.0`). Engine
resources are set from `config` (all optional): `memory` (DuckDB
`memory_limit`, default `1GB`), `max_temp_size` (`max_temp_directory_size`,
default `6GB`, a hard cap on spill), `threads` (default `2`), `temp_directory`
(default `/tmp/duckdb-spill`). No other DuckDB settings are exposed and no
compatibility flags are set, so the 2.0 dialect applies as documented in the
[migration guide](https://duckdb.org/docs/2.0/operations_manual/migration.html):
`~`/`!~` match partially; `!` binds tighter; `x -> …` lambdas are disabled
(`lambda x : …`); integer `//` and `%` by zero raise; an untyped `NULL`
column keeps type `NULL`; `X'…'`/`B'…'` literals are BLOB/BITSTRING;
`MERGE INTO` raises on a doubly matched target row. ICU collations and
time-zone functions work without `LOAD icu`; the extension is statically
linked into the engine.

## How inputs and outputs map to SQL

The component is a thin shell around DuckDB. Before your SQL runs, the component pre-registers each declared input as a **DuckDB view**. After your SQL completes, the component reads back each declared output by **selecting from a DuckDB table or view your SQL created** and streams the rows to DG.

**Inputs → DuckDB views.** Each declared input is registered as a DuckDB view. `<view_name>` is `logicalName` if set, otherwise the physical `table`. Inputs are fetched from DataGateway as parquet and staged on the component's ephemeral disk; only one HTTP read buffer is in memory at a time. Internally:

1. Component opens an `OpenReader(FORMAT_PARQUET)` on the input and takes the gateway's HTTP read endpoint from the reply (gRPC's 64 MiB message cap rules it out for large source files).
2. It `GET`s the stream chunk by chunk, streaming each response body to its own parquet file under the work dir (`io.Copy`, ~32 KiB resident), until the gateway sets `X-Datuplet-Is-Last`.
3. It runs `CREATE VIEW <view_name> AS SELECT * FROM read_parquet([<chunk files>])` and then the user SQL; DuckDB scans the staged files lazily, row group by row group.

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

Each input's parquet chunks are staged on the component's local ephemeral disk and read lazily through `read_parquet`; nothing beyond one HTTP read buffer is resident in memory.

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

**What if an input has zero rows?** The view is a zero-row placeholder that does not carry the input's columns; naming one of them fails at bind time. See [Empty-input semantics](#empty-input-semantics).

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

When a declared input has no data files for the current run (first incremental window, upstream extractor produced nothing), the gateway closes the stream with no chunks and the component registers a zero-row placeholder view (`CREATE VIEW <view_name> AS SELECT * FROM (SELECT 1) WHERE 1=0`). The placeholder does **not** carry the input's columns: `SELECT count(*) FROM <view_name>` works and yields 0, but SQL that names a column of the empty input fails at bind time with a column-not-found error — the same outcome as referencing a missing column on a real table. If a pipeline can legitimately see an empty input, make the producing stage always emit its schema; a `UNION ALL` with a typed empty `SELECT` does not help, because the first branch still fails to bind the missing column.

## Architecture (for the curious)

The component is intentionally creds-clean. Per-step:

1. **Inputs.** For each declared input the component opens `OpenReader(FORMAT_PARQUET)`, drains the gateway's HTTP read endpoint chunk by chunk into local parquet files, and registers `CREATE VIEW … AS SELECT * FROM read_parquet([...])`. **Parquet bytes are staged on the component's ephemeral disk; nothing but one HTTP read buffer is resident.**
2. **SQL.** The user's SQL string runs verbatim against the view set. DuckDB executes it in-process; intermediate `CREATE TABLE …` statements stay in DuckDB local memory or spill to `/tmp/duckdb-spill` (configurable via `config.temp_directory`).
3. **Outputs.** For each declared output, the component runs `COPY <sdk-name> TO '<staged>.parquet' (FORMAT PARQUET, COMPRESSION SNAPPY)`, reads the whole staged file into memory (`os.ReadFile`) and sends it to DG in a single `WriteChunk` over the gateway's HTTP endpoint (`OpenWriterToBucket` + `WriteChunk` + `Close`). **The upload needs RAM proportional to the compressed output size, outside DuckDB's `memory_limit`.** DG re-emits the parquet to the iceberg target prefix using lakekeeper-vended STS creds, and writes the per-target `files.json` as an audit breadcrumb.
4. **Commit.** Inline in the DG sidecar, outside the component. CloseWriter dispatches the writer's parquet paths to the DG's in-process commit pool, which calls `icebergjob.CommitTableFiles`: it opens an iceberg transaction, calls `txn.AddFiles` (APPEND) or `txn.ReplaceDataFiles` (FULL_LOAD), and commits via lakekeeper. There is no separate commit Job. 409 conflicts retry via `catalogwriter.RetryOnConflict`.

The component process never holds production S3 credentials; DG's `pkg/catalogwriter/VendedCreds` handles per-table STS scoping + auto-refresh at 50%-elapsed TTL.

## DuckDB tuning

```yaml
config:
  sql: |
    …
  threads: 4                  # DuckDB SET threads=N (default 2)
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
- Memory: input parquet chunks are staged on the component's ephemeral disk (not held in RAM), and DuckDB's spill (configurable via `temp_directory`) shares that disk; only one HTTP read buffer is resident for input data. Each output, however, is read back whole (`os.ReadFile`) before its single `WriteChunk`, so budget RAM for the largest compressed output on top of `memory`. POC posture; size pod resource requests and ephemeral storage accordingly.
