# The PipelineDoc format

A PipelineDoc is **envelope-free**: top-level `name`, `description`, `gateway`,
`stages`. There is no `apiVersion`, `kind`, `metadata`, or `spec` — those are
the old Kubernetes-CR envelope and the server rejects them. Canonical form is
JSON; YAML is just a human rendering (the CLI accepts either).

## Top level

```yaml
name: my-pipeline            # required — the pipeline's identity (used by put/trigger)
description: "…"             # optional — free text
gateway: { … }               # optional — Data Gateway tuning (see below)
stages: [ … ]                # required — ordered list; each stage runs, then the next
```

## gateway (optional)

Byte-valued tuning for the storage sidecar. Omit it entirely to accept
defaults; set only what you need.

```yaml
gateway:
  chunkSize: 33554432        # component<->gateway streaming chunk (default 32MB)
  bufferSize: 67108864       # in-memory buffer before a Parquet row group (default 64MB)
  rowGroupSize: 67108864     # target Parquet row-group size (default = bufferSize)
  targetFileSize: 134217728  # Parquet file rotation size (default 128MB)
```

## stages and components

Stages run **in order**. Components **within a stage** run in parallel. A later
stage reads (via `inputs`) tables that an earlier stage wrote (via `outputs`) —
that's how you chain extract → transform → load.

```yaml
stages:
  - name: extract                 # required — stage label
    components:
      - name: json-extractor      # required — INSTANCE name, unique in the pipeline
        component: http-json-extractor   # required — registry reference (a catalog name)
        version: v0.9.1            # optional — omit for the registry default version
        config: { … }             # component config — MUST match `components get <c> --schema`
        inputs: { … }             # optional — tables this component reads
        outputs: { … }            # optional — tables/buckets this component writes
        resources: { … }          # optional — cpu/memory limits (usually omit; registry defaults apply)
```

- `component` is the catalog name (what `datuplet components list` shows).
  `name` is your instance label for it in this pipeline.
- `config` is validated against that component version's JSON Schema. Discover
  it with `datuplet components get <component> --schema` and conform exactly.
- `version` omitted → the registry's default (highest stable) version. Pin it
  only when you need a specific one.

## inputs

What a component reads. Tables are addressed by `bucket` + `table`:

```yaml
inputs:
  tables:
    - bucket: raw
      table: posts
    - bucket: raw
      table: users
```

An input table must already exist — either produced by an earlier stage in the
same pipeline, or a pre-existing table in storage.

## outputs

What a component writes. Two forms:

**Explicit tables** (use this for transforms and when you name output tables):

```yaml
outputs:
  tables:
    - name: user_summary       # table name written
      bucket: etl              # destination bucket
      writeMode: FULL_LOAD     # APPEND | FULL_LOAD
      # logicalName: …         # optional — output table's logical name
      # partitionSpec: …       # optional — partitioning
```

**Extractor convenience defaults** (common for extractors that emit one table):

```yaml
outputs:
  defaultBucket: raw
  defaultWriteMode: APPEND
```

### Write modes

- `APPEND` — add rows to the table (incremental ingestion).
- `FULL_LOAD` — replace the table's data (idempotent rebuilds; typical for
  transforms and for re-runnable extracts).

## Secret references

Any `config` field the component schema marks `x-datuplet-secret` must be a
**whole-scalar reference**, never a literal:

```yaml
config:
  token: $[api_token]     # resolved from the project secret store at run time
```

`$[key]` is resolved from the managed per-project secret store. A referenced key
that isn't set surfaces as a `warning` at validate time and a `FailedUser` at
run time — set the secret (project secrets API/UI) before triggering.

## Partitioning and output field names

An output table may declare partitioning under `partitionSpec` — a list where
each entry has `source_column` (the data column to partition on) and `transform`
(`identity` | `day` | `month` | `year` | `hour`):

```yaml
outputs:
  tables:
    - name: events
      bucket: curated
      writeMode: APPEND
      logicalName: events          # optional SDK identifier (defaults to name)
      partitionSpec:
        - source_column: created_at
          transform: day
```

Use these exact doc field names — `logicalName`, `partitionSpec`,
`source_column`, `transform`. They are the normative names `datuplet pipeline
validate` accepts.

## The definitive check

Whatever you write, `datuplet pipeline validate -f <file> --json` is the oracle.
If it exits 0, the shape and semantics are accepted. If not, the finding `path`
tells you the exact field to fix.
