# Built-in Components

Datuplet ships five component images. Each runs as an ordinary container alongside
the Data Gateway sidecar; the component communicates with the sidecar via gRPC and
HTTP — it never touches S3 directly.

Image registry: `ghcr.io/kacurez/<name>:v0.1.0`

---

## data-generator

Generates random or literal rows inline from pipeline YAML — useful for testing
pipelines without an external data source.

**Image:** `ghcr.io/kacurez/data-generator:v0.1.0`

**Config schema (random mode):**

```yaml
- name: gen
  image: ghcr.io/kacurez/data-generator:v0.1.0
  config:
    tables:
      - name: events
        random:
          schema:
            id: int
            ts: timestamp
            value: double
            label: string
          limit:
            rowsCount: 1000       # stop after 1000 rows
            # sizeInBytes: 1048576  # OR stop after ~1 MiB
            # timeoutInSeconds: 30  # OR stop after 30 s
  outputs:
    defaultBucket: raw
    defaultWriteMode: APPEND
```

**Config schema (literal mode):**

```yaml
- name: seed
  image: ghcr.io/kacurez/data-generator:v0.1.0
  config:
    tables:
      - name: lookup
        literal:
          columns: [id, country, code]
          rows:
            - [1, "US", "USD"]
            - [2, "DE", "EUR"]
  outputs:
    defaultBucket: raw
    defaultWriteMode: FULL_LOAD
```

**Supported column types (random mode):** `string`, `int`, `long`, `float`,
`double`, `boolean`, `date`, `timestamp`, `now`, `uuid`.

**Limit semantics:** OR — the first limit to trip wins.

**Fault injection:** Set `random.userErrorMessage` to simulate a user-error exit
at a random point during generation. Useful for testing pipeline failure paths.

**Reproducibility:** Same `run_id` + `table_name` → same row sequence (PCG seeded
from SHA-256 of the pair).

---

## http-json-extractor

Fetches JSON from an HTTP endpoint and writes it as an Iceberg table. Supports
single-request and paginated modes.

**Image:** `ghcr.io/kacurez/http-json-extractor:v0.1.0`

**Config schema (single request):**

```yaml
- name: fetch
  image: ghcr.io/kacurez/http-json-extractor:v0.1.0
  config:
    url: "https://api.example.com/items"
    array_path: "items"     # key of the JSON array in the response object
                            # omit if the response is a top-level array
    table_name: "items"     # output table name (defaults to array_path or "data")
    headers:
      Authorization: "Bearer $[api_token]"  # use $[name] for secrets
  outputs:
    defaultBucket: raw
    defaultWriteMode: APPEND
```

**Config schema (paginated):**

```yaml
- name: fetch-paged
  image: ghcr.io/kacurez/http-json-extractor:v0.1.0
  config:
    url: "https://api.example.com/records"
    array_path: "records"
    pagination:
      type: page            # "page" or "offset"
      param: page           # query parameter name
      start: 1              # starting value (default 1 for page, 0 for offset)
      increment: 1
      page_size: 100
      size_param: per_page  # query param for page size (optional)
      max_pages: 50         # 0 = unlimited
      max_records: 5000     # 0 = unlimited
      stop_when_empty: true # stop on empty page (default true)
  outputs:
    defaultBucket: raw
    defaultWriteMode: APPEND
```

For API keys, use `$[name]` in the `headers` map and provide the secret via
`spec.secretsRef` on the Pipeline. See [docs/secrets.md](secrets.md).

---

## finnhub-extractor

Fetches market data from the [Finnhub](https://finnhub.io/) API. Requires a
Finnhub API key.

**Image:** `ghcr.io/kacurez/finnhub-extractor:v0.1.0`

**Config schema:**

```yaml
- name: market
  image: ghcr.io/kacurez/finnhub-extractor:v0.1.0
  config:
    mode: quote             # see modes below
    symbols: [AAPL, MSFT, GOOG]
    apiKey: $[finnhub_key]  # use secret reference
  outputs:
    defaultBucket: raw
    defaultWriteMode: APPEND
```

**Supported modes:**

| Mode | Output table | Description |
|---|---|---|
| `quote` | `market_data` | OHLC + change for each symbol |
| `news` | `news_raw` | General market news (filtered by `lookback_days`) |
| `company-news` | `company_news` | Per-symbol news with date-range filter |
| `basic-financials` | `basic_financials` | P/E, P/B, market cap, 52w high/low |
| `earnings` | `earnings` | Earnings surprises (last N quarters, `limit: 4`) |
| `recommendations` | `recommendations` | Analyst consensus per symbol |
| `insider-transactions` | `insider_tx` | SEC insider trades, filtered by `lookback_days` |

**Additional config fields:**

```yaml
config:
  mode: company-news
  symbols: [TSLA]
  lookback_days: 7    # filter news/transactions to last N days
  limit: 8            # quarters for earnings mode (default 4)
  apiKey: $[finnhub_key]
```

Store the API key as a K8s Secret and reference it:

```yaml
spec:
  secretsRef:
    name: finnhub-secrets
```

```bash
kubectl create secret generic finnhub-secrets \
  --from-literal=finnhub_key=<your-api-key> \
  -n datuplet
```

See [docs/secrets.md](secrets.md) for the full secret resolution flow.

---

## sql-transform

Runs user-supplied SQL inside an embedded DuckDB engine. Inputs stream from the
Data Gateway via Arrow IPC and are materialized into DuckDB tables before the SQL
runs. Outputs are written back through the Data Gateway; no S3 credentials touch
the component.

**Image:** `ghcr.io/kacurez/sql-transform:v0.1.0`

**Config schema:**

```yaml
- name: transform
  image: ghcr.io/kacurez/sql-transform:v0.1.0
  inputs:
    tables:
      - bucket: raw
        table: orders
      - bucket: raw
        table: customers
        logicalName: cust     # name to use in SQL (overrides "customers")
  outputs:
    tables:
      - name: order_summary
        bucket: curated
        writeMode: FULL_LOAD
  config:
    sql: |
      CREATE TABLE order_summary AS
      SELECT
        cust.country,
        SUM(o.total)  AS total_revenue,
        COUNT(*)      AS order_count
      FROM orders o
      JOIN cust ON o.customer_id = cust.id
      GROUP BY cust.country;
    threads: 4                # DuckDB thread count (default 4)
    temp_directory: /tmp/spill  # spill-to-disk path
```

**Key points:**

- Inputs stream from the Data Gateway via Arrow IPC and are materialized into
  DuckDB in-memory tables before the user SQL runs. This is load-bearing: DuckDB's
  `arrow_scan` extension does not support multi-pass reads on a streaming source.
- The SQL `CREATE TABLE <name>` must match the `outputs.tables[].name` (or its
  `logicalName` if set).
- `logicalName` on inputs overrides the view name used in SQL.
- `logicalName` on outputs overrides the DuckDB table name the user writes
  `CREATE TABLE` against.
- `writeMode: FULL_LOAD` replaces the entire Iceberg table; `APPEND` adds rows.
- The component is credentials-clean: it holds no S3 or Lakekeeper credentials.

**Limitations:**

- No UPSERT (delete support deferred).
- `sinceDays` time-filter is wired through the CRD but the column-predicate push
  is not yet honoured by the Lakekeeper read path; full-snapshot reads are used.
- Schema evolution: if your SQL creates an output table with a different schema than
  the existing Iceberg target, the TableCommit Job will fail with `FailedUser`.

---

## stdout-writer

Reads input tables and prints them to stdout. For debugging only — no Iceberg
output.

**Image:** `ghcr.io/kacurez/stdout-writer:v0.1.0`

**Config schema:**

```yaml
- name: print
  image: ghcr.io/kacurez/stdout-writer:v0.1.0
  inputs:
    tables:
      - bucket: raw
        table: events
  config:
    format: csv     # "csv" (default) or "json"
```

**Example pipeline:**

```yaml
apiVersion: datuplet.io/v1
kind: Pipeline
metadata:
  name: debug-pipeline
  namespace: datuplet
spec:
  stages:
    - name: generate
      components:
        - name: gen
          image: ghcr.io/kacurez/data-generator:v0.1.0
          config:
            tables:
              - name: events
                random:
                  schema: { id: int, value: double }
                  limit: { rowsCount: 10 }
          outputs:
            defaultBucket: raw
            defaultWriteMode: APPEND
    - name: print
      components:
        - name: printer
          image: ghcr.io/kacurez/stdout-writer:v0.1.0
          inputs:
            tables:
              - bucket: raw
                table: events
          config:
            format: json
```

View output in component logs:

```bash
kubectl logs -n datuplet <pod-name> -c component
```

---

## Writing your own component

Use the Go SDK (`sdk/go/`) or Python SDK (`sdk/python/`) to build a component.
Both SDKs are ~200–300 LOC and expose three operations: `OpenWriter`,
`WriteChunk` / `Write`, and `Close`. The SDKs handle gRPC connection, config
resolution, and secret delivery from the Data Gateway sidecar.

See [`sdk/go/client.go`](../sdk/go/client.go) and
[`sdk/python/client.py`](../sdk/python/client.py) for the entry points.
