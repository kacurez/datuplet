# Ad-hoc SQL query (RFC 022)

**Experimental.** The query surface is opt-in and disabled by default in the
Helm charts. APIs and limits may change between 0.x releases.

---

Datuplet provides two ways to run read-only ad-hoc SQL against the warehouse:

| Surface | How it runs | Who enables it |
|---|---|---|
| **Server query service** (`POST /api/v1/query`) | DuckDB inside a query-worker Pod; pipeline-api proxies | `queryWorker.enabled: true` in values |
| **BYO-local CLI** (`datuplet-query`) | DuckDB on the user's laptop | `allowClientSideQuery: true` + separate binary |

Both surfaces use lakekeeper for table metadata and lakekeeper-vended credentials
for object storage. FGA governs what each caller can see — you can only read
tables your FGA tuples allow.

The browser query console (`/ui/query`) is the interactive front-end for the
server query service.

---

## Read-only enforcement

The engine does not structurally block writes. DuckDB's iceberg extension can
write. Read-only is enforced by the FGA grant: a viewer receives read-only
lakekeeper-vended STS credentials, so any catalog-qualified write (INSERT /
UPDATE / DELETE / MERGE) fails at the catalog side when lakekeeper rejects the
staging PUT or commit. Do not rely on engine-level write blocking.

---

## Server query service

### Enabling

```yaml
# charts/datuplet-app/values.yaml
queryWorker:
  enabled: true
  query:
    warehouse: "<lakekeeperProjectID>/<warehouseName>"  # required; set after lakekeeper-bootstrap
```

The warehouse value is only known after `pipeline-api admin lakekeeper-bootstrap`
runs. Pipeline-api soft-degrades to 404 on `POST /api/v1/query` when the
warehouse is empty or `DATUPLET_QUERY_WORKER_URL` is unset.

### POST /api/v1/query

Runs a SQL statement against the warehouse and returns results as JSON.

**Authentication:** bearer token (`Authorization: Bearer <api-token>`) or
session cookie.

**Request body:**

```json
{
  "sql": "SELECT * FROM my_namespace.my_table LIMIT 10",
  "timeout_s": 60,
  "max_rows": 1000,
  "max_bytes": 1048576
}
```

| Field | Type | Required | Notes |
|---|---|---|---|
| `sql` | string | yes | SQL statement to execute |
| `timeout_s` | int | no | Query timeout in seconds; clamped to `[1, 300]`; default 60 |
| `max_rows` | int | no | Row limit; clamped to `[1, 10 000]`; default 1 000 |
| `max_bytes` | int | no | Result byte limit; clamped to `[1, 10 485 760]` (10 MiB); default 1 MiB |

All three optional fields are clamped server-side to the operator-configured
ceilings. A value above the ceiling is silently reduced, never rejected.

**Success response (200):**

```json
{
  "schema": [
    {"name": "id",    "type": "INTEGER"},
    {"name": "label", "type": "VARCHAR"}
  ],
  "rows": [[1, "alpha"], [2, "beta"]],
  "truncated": false,
  "stats": {
    "duration_ms": 142,
    "rows_scanned": 2
  }
}
```

`truncated: true` means the result was cut at the `max_rows` or `max_bytes`
cap. The response is still 200; the rows present are a prefix of the full
result set. The scan itself is not limited — only the returned rows are capped.

**Error envelope:**

All error responses share the shape:

```json
{"error": "human-readable message", "kind": "<kind-code>"}
```

| HTTP status | `kind` | Meaning |
|---|---|---|
| 400 | `sql_error` | Bad SQL syntax, unresolved table, or FGA/lakekeeper authz denial (lakekeeper rejects the ATTACH and DuckDB surfaces it as a SQL error — authz denial appears as 400 `sql_error`, not 403) |
| 400 | `bad_request` | Malformed request body or empty `sql` field |
| 400 | `result_too_large` | Result exceeded the byte cap before truncation was possible |
| 408 | `timeout` | Query exceeded the clamped `timeout_s` |
| 429 | `rate_limited` | Caller has reached the per-principal in-flight cap (default 2 concurrent queries); `Retry-After: 2` is set |
| 503 | `capacity` | Query-worker semaphore is full (all concurrency slots busy); `Retry-After: 2` is set |

Note: 503 `capacity` is returned when the worker itself returns 429 (worker
saturation). The proxy translates worker-429 → client-503 so the distinction
between "your concurrent limit" (429 `rate_limited`) and "worker is full"
(503 `capacity`) is clear to callers.

### Deployed limits (default values.yaml)

| Limit | Default | Max |
|---|---|---|
| `timeout_s` | 60 s | 300 s |
| `max_rows` | 1 000 | 10 000 |
| `max_bytes` | 1 MiB | 10 MiB |
| per-principal in-flight | 2 | (operator-set) |

Override via `queryWorker.query.*` in `values.yaml` or the corresponding
`DATUPLET_QUERY_*` environment variables.

### Audit

One structured log line (`query_audit`) is emitted per authenticated request.
Fields: `principal` (user UUID), `jti` (catalog token ID for cross-system
correlation with lakekeeper logs), `statement_hash` (first 16 hex chars of
SHA-256 of the SQL), `duration_ms`, `outcome`, `truncated`. The raw SQL is
never stored or logged. A Prometheus counter (`pipelineapi_query_requests_total`,
labeled by `outcome`) is also incremented.

---

## Browser query console

Navigate to `/ui/query`. The console requires a project to be selected and
the server query service to be enabled.

**Layout:** schema tree (left) | SQL editor (top-right) | results grid
(bottom-right).

- **Schema tree:** lists lakekeeper namespaces and tables visible to your FGA
  tuples. Expand a namespace to see its tables; click a table name to insert
  it at the cursor. Column types are loaded lazily on first expand. Use the
  "Filter schema…" search box (or press `/`) to narrow the tree.
- **SQL editor:** enter SQL, then press `⌘↵` (macOS) or `Ctrl↵` (Windows/Linux)
  to run. The buffer is persisted in sessionStorage per user per project and
  survives page reloads.
- **Results grid:** shows query results with column names and types. A
  truncation banner appears when the server returned `truncated: true`.
  The UI applies a separate **display cap of 2 000 rows** on top of the
  server's `max_rows` cap — if the server returned more rows than that, a
  second note is shown. Neither cap limits the underlying scan.
- **Export:** "Copy TSV" copies the displayed rows to the clipboard; "Download
  CSV" saves a file. Both buttons are labeled "(capped)" when either cap
  applied.

**Error display:** query errors surface inline below the editor (no crash or
page reload). `sql_error` shows the DuckDB message. `timeout` and `rate_limited`
show human-readable guidance.

### Manual smoke checklist

The query console has no automated browser tests. When verifying the feature
manually after deployment:

- [ ] `/ui/query` loads without a JS error.
- [ ] The schema pane lists tables (only those your FGA grants allow).
- [ ] `⌘↵` / `Ctrl↵` submits a `SELECT 1` and the grid shows one row.
- [ ] Running `SELECT * FROM namespace.table LIMIT 2001` shows the truncation
      banner (server cap hit if `max_rows` default applies) or the UI display
      cap note.
- [ ] An invalid table name (e.g. `SELECT * FROM bad.table`) surfaces a 400
      `sql_error` inline — no crash, no blank page.
- [ ] A table outside your FGA grants surfaces a 400 `sql_error` (not a
      generic error or crash).

The `POST /api/v1/query` path itself is covered by the e2e suite in
`tests/e2e/scenarios_query_test.go`.

---

## BYO-local CLI (`datuplet-query`)

`datuplet-query` runs an embedded DuckDB engine on the user's laptop, reading
from the warehouse via lakekeeper-vended credentials. It is a **separate binary**
from the duckdb-free root `datuplet` CLI and must be built with `-tags duckdb_arrow`
(CGO required).

> **Operator gate.** The CLI requires `allowClientSideQuery: true` in
> `values.yaml` (default `false`). When off, `POST /api/v1/query/token` returns
> a 403 `forbidden` refusal — the gate is server-enforced, so an outdated CLI
> cannot bypass it.

### Prerequisites

1. `datuplet login --remote <pipeline-api-url>` has been run. This writes
   `~/.datuplet/cluster.json` and `~/.datuplet/api-token`.
2. The operator has set `allowClientSideQuery: true`.

### Usage

```
datuplet-query [flags]
```

**SQL source (first match wins):**

| Flag | Description |
|---|---|
| `--sql "SELECT …"` | Inline SQL |
| `-f FILE` | Read SQL from a `.sql` file |
| _(stdin)_ | Pipe SQL on stdin |

**Output and resource flags:**

| Flag | Default | Description |
|---|---|---|
| `--format table\|csv\|json` | `table` | Output format |
| `--memory-limit SIZE` | `2800MiB` | DuckDB memory limit (e.g. `4000MiB`) |
| `--timeout DURATION` | `5m` | Per-query deadline (e.g. `30s`, `2m`) |
| `--max-rows N` | `10000` | Maximum rows returned; 0 = engine default |

**Examples:**

```bash
# Inline SQL, table output
datuplet-query --sql "SELECT count(*) FROM sales.orders"

# SQL from file, JSON output
datuplet-query -f report.sql --format json

# Piped SQL
echo "SELECT * FROM events.clicks LIMIT 5" | datuplet-query
```

A truncation note is printed to stderr (not stdout) when the result was capped,
so stdout remains clean for piping.

### Per-invocation flow

1. Parse flags + resolve SQL.
2. Load `~/.datuplet/{cluster.json, api-token}`.
3. Call `POST /api/v1/query/token` with the api-token to mint a short-lived
   query JWT (gated by `allowClientSideQuery`).
4. Run DuckDB locally, attaching to lakekeeper with the minted JWT.
5. Render output on stdout.

The minted JWT is held only in memory; it is never written to disk or logged.

### Exit codes

| Code | Meaning |
|---|---|
| 0 | Success (including truncated results) |
| 1 | User error: bad SQL, FGA/authz denial, result too large |
| 20 | Application error: timeout, mint failure, infrastructure error |

### Audit

The server emits a **mint-level** audit record when it issues a query JWT:
`principal` + timestamp. The server does not see the SQL the CLI runs —
statement-level audit is not available for BYO-local queries.

### Config files

| File | Written by | Contents |
|---|---|---|
| `~/.datuplet/cluster.json` | `datuplet login --remote` | Lakekeeper URL, warehouse name, pipeline-api URL, project IDs |
| `~/.datuplet/api-token` | `datuplet login --remote` | Raw pipeline-api bearer JWT |
