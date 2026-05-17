# data-generator

A Datuplet component that generates random or literal data inline from a pipeline YAML config.

## Quick start

```yaml
components:
  - name: gen
    image: datuplet/data-generator:latest
    config:
      tables:
        - name: products
          random:
            schema:
              id: int
              name: string
              price: double
              created_at: timestamp
            limit:
              rowsCount: 100
    outputs:
      defaultBucket: my-bucket
      defaultWriteMode: FULL_LOAD
```

## Two modes

Each table sets **exactly one** of `random:` or `literal:`. Both-set or neither-set → config error.

### Random mode (`random.schema` + `random.limit`)

Generates random rows until a limit is reached.

```yaml
- name: products
  rowInsertSpeed: 0          # optional, ms between rows
  random:
    schema:                  # required, map of name → type (at least one column)
      <colName>: <type>
    limit:                   # required; at least one field non-zero
      rowsCount: 100         # stop after N rows
      sizeInBytes: 1048576   # stop after ~1 MiB written
      timeoutInSeconds: 30   # stop after 30 s elapsed
    userErrorMessage: ""     # optional; see below
```

**OR semantics** for limits — the first one to trip wins.

### Literal mode (`literal.columns` + `literal.rows`)

Emits explicit rows with named columns. Schema is inferred from the first non-null value per column.

```yaml
- name: orders
  rowInsertSpeed: 0
  literal:
    columns: [id, customer, email, active]   # required; arity must match rows
    rows:
      - [1, "alice", null,    true]
      - [2, "bob",   "x@y.z", false]
```

Rules:
- At least one row required.
- All rows must have the same number of columns.
- `columns` is required and must have one entry per column. Names must match `^[A-Za-z_][A-Za-z0-9_]{0,127}$` (no dots, no hyphens) and be unique.
- Mixed types within a column → config error (no int→double widening in v1).
- All-null column → config error (no schema inferable).
- `rowInsertSpeed` is honoured.
- `userErrorMessage` is **not** honoured in literal mode — use random mode for failure injection.

## Column types (random mode)

| Type        | Description                                                       |
|-------------|-------------------------------------------------------------------|
| `string`    | Random hex string, 16–40 characters.                              |
| `int`       | Random int32.                                                     |
| `long`      | Random int64.                                                     |
| `float`     | Random float32 in [0, 1).                                         |
| `double`    | Random float64 in [0, 1).                                         |
| `boolean`   | Random true/false.                                                |
| `date`      | Random date in ISO 8601 (`YYYY-MM-DD`), within the last 365 days. |
| `timestamp` | Random RFC 3339 timestamp (ms precision), within the last 30 days.|
| `now`       | Current UTC time at row-generation time (not random).             |
| `uuid`      | UUIDv4.                                                           |

## Reproducibility

Each table is seeded with `sha256(runID || 0x00 || tableName)` (first 8 bytes feed PCG). The same run ID + table name always produces the same row sequence. An empty run ID falls back to a time-based seed.

## Per-table parallelism

All tables run concurrently. The DataGateway SDK supports concurrent writers across different tables in one component process.

## userErrorMessage (fault injection, random mode only)

When `random.userErrorMessage` is set:
- A random trigger point is chosen at goroutine start (within the row/byte/time budget).
- When the trigger fires, the component exits with code 1 (`FailedUser`) and emits `DUPLET_STATUS_MESSAGE: <userErrorMessage>` on stdout.
- The error always fires before `limit` is reached.

This is intentional failure injection for testing pipeline error paths.
