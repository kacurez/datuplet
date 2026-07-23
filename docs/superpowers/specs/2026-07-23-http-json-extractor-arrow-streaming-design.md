# http-json-extractor: streaming Arrow-IPC output + local debug rig — design

- **Date:** 2026-07-23
- **Component:** `components/http-json-extractor/`
- **Status:** approved design, pre-implementation
- **Baseline:** `v0.11.0` (post PR #34 — JSONL writes + `fields` projection)

## Problem

Pulling millions of rows exposed two memory ceilings and a throughput floor:

1. **Component OOM.** The extractor does `io.ReadAll(resp.Body)` then
   `json.Unmarshal` of a whole page (e.g. 50 000 records × ~30 fields) into
   `[]map[string]any`, then `projectRecords`, then `encodeJSONL`. Peak memory ≈
   several × the page body. A 50k page of NYC 311 data OOM-killed the component
   at 512Mi (exit 137).

2. **Gateway sidecar crash.** After bumping the component to 1Gi, a 5M-row run
   failed with `Post "http://127.0.0.1:50052/data/write/w1": EOF` — the Data
   Gateway sidecar died mid-write. On the **JSONL path the gateway parses every
   line JSON→Arrow before buffering to Parquet**; data-generator's
   `arrow_writer.go` documents this as *"~65% of write-side CPU per Pyroscope"*
   and is why data-generator sends **Arrow IPC**. Under the 5M-row JSONL load
   the sidecar OOM'd.

3. **Slow, opaque iteration.** Debugging required full K8s deploy + trigger
   cycles against a cluster we can't even inspect with local `kubectl`.

Download latency itself (~18s per 50k-row Socrata page, network-bound at
~4 MB/s) is **not** addressed by this work — see Non-goals / Phase 2.

## Goals

- Bound component memory to a single batch regardless of `page_size`, killing
  the component OOM.
- Remove the gateway's JSON-parse load by sending **Arrow IPC**, so the sidecar
  survives multi-million-row writes.
- Provide a **local run/debug rig** so the extractor can be exercised and
  profiled end-to-end without the cluster.

## Non-goals

- Faster *download* (Socrata-bound); parallel page fetches are **Phase 2**,
  optional, deferred (throttling risk).
- Type inference from JSON. Emitted columns are **all Arrow String** (decision
  below).
- Changing the gateway, the SDK, or other components.
- Fixing the data-generator row-drop finding (noted below; separate work).

## Design

### 1. Streaming Arrow-IPC output (both extraction paths)

Replace the read-all/unmarshal/JSONL path with a streaming pipeline:

- **Stream-decode** the HTTP response with `json.Decoder`: read the opening
  tokens to position at the record array (handling the three shapes the current
  `parseJSON` supports — bare array `[...]`, positional `[meta,[...]]`,
  object-wrapped `{key:[...]}`), then `Decode` one record object at a time. No
  `io.ReadAll`, no whole-page `[]map[string]any`.
- **Accumulate** decoded records into an Arrow `array.RecordBuilder` whose
  schema is **all `arrow.BinaryTypes.String`** (nullable). Column set:
  - `fields` set → the projected `name`s, in declared order; each value read via
    the existing `getValueRaw(record, path)` then **stringified**.
  - `fields` unset → columns = the **sorted keys of the first decoded record**,
    fixed for the run; later records project onto that set (missing key → null,
    extra keys → ignored). Documented as the no-projection behavior.
- **Stringify** a value: JSON string → as-is; number/bool → its canonical Go
  string; `null`/absent → Arrow null; nested object/array → compact JSON string
  (`json.Marshal`).
- **Flush every `arrowBatchRows` (default 8192) records**: build the record,
  serialize a **self-contained IPC stream** (schema + record + EOS, like
  data-generator's `serializeRecordToIPC`), and ship it.

Open the writer with `sdk.WithFormat(pb.DataFormat_FORMAT_ARROW_IPC)`. Peak
component memory ≈ one 8192-row batch, independent of `page_size` and of the
total row count.

### 2. WriteChunk per batch — MANDATORY (not batched `Write`)

The gateway's `ArrowIPCAdapter.parseFromReader`
(`pkg/datagateway/format/arrow_ipc.go`) creates **one** `ipc.Reader` per POST
body and reads until the **first** stream's EOS. It handles multiple record
batches *within one IPC stream*, **not** multiple concatenated IPC streams. The
SDK's batched `writer.Write` buffers bytes to a 1MiB threshold and ships the
concatenation as one POST — so several sub-1MiB IPC streams in one POST would be
**silently truncated to the first**.

Therefore each batch's IPC stream must be delivered as its **own POST**: use
**`writer.WriteChunk(ctx, ipcBytes)`** (immediate, one POST per call), never the
batched `Write`, on the Arrow path. This is the single most important
correctness constraint of this design and must be covered by a test.

> **Flagged finding (out of scope):** data-generator ships 8192-row Arrow
> batches (~80–200KB, sub-1MiB) via the *batched* `writer.Write`, so at large
> row counts it concatenates IPC streams into one POST and the gateway drops all
> but the first — a latent silent row-drop. Its e2e passes only because test row
> counts fit one POST. Worth a separate issue; this spec does not fix it.

### 3. Mock-DG local debug rig (`cmd/mock-datagateway/`)

A standalone Go program that lets the **real extractor binary** run locally:

- **gRPC server on `:50051`** implementing the `DataGateway` methods the SDK
  calls: `GetConfig`, `OpenWriter`, `WriteChunk`, `CloseWriter`, `Commit`,
  `Log`, `Shutdown`. Built on `pb.UnimplementedDataGatewayServer`.
  - `GetConfig` returns a `ComponentConfig` assembled from `--config <file>`
    (the extractor's config JSON), `--bucket` (default bucket), and a synthetic
    execution id.
  - `OpenWriter` returns an `OpenWriterResponse` with
    `HttpEndpoint: http://localhost:50052/data/write/<id>`.
  - `Commit` returns success with the accumulated per-table row count.
  - `Log` prints the component's log lines to stdout.
- **HTTP server on `:50052`** serving `/data/write/{id}`: reads the POST body,
  deserializes it with `ipc.NewReader`, **counts rows and validates the schema**
  (asserts all-String columns and the expected column set), then discards. An
  optional `--dump <dir>` writes the received rows to local NDJSON/Parquet for
  inspection.
- Prints a running tally (batches, rows, bytes) and a final summary.

Run it via a `make extractor-local URL=… CONFIG=…` target that: builds the
extractor + mock, starts the mock, then runs the extractor with
`DATUPLET_GATEWAY_ADDR=localhost:50051`, wrapping it in `/usr/bin/time -l`
(macOS) to report peak RSS. Memory is observed via peak RSS; an optional
`DATUPLET_PPROF_ADDR` env-gated `net/http/pprof` listener in the extractor is a
nice-to-have for allocation profiling.

The mock is a **dev/test tool**, not a deployment surface (K8s remains the only
deployment surface).

### 4. All-String output — behavior change

Every emitted column is Arrow String. This is a deliberate change from the
JSONL path, where the gateway inferred Int64/Float64/Bool from JSON scalar
types. Consequences and mitigations:

- Numeric/boolean sources now land as strings; downstream `sql-transform` uses
  `CAST` for typed math. Documented in `docs/components.md`.
- **APPEND onto a pre-existing JSONL-created table** with typed columns would
  fail iceberg's schema check (String vs Int64). Fresh tables (FULL_LOAD, or a
  table first created by this version) are self-consistent and unaffected.
- The e2e suite creates fresh tables per run, so it stays internally
  consistent; it is **re-verified** as part of this work (see Testing), since
  the previous PR characterized the JSONL change as type-preserving and this one
  is not.

### 5. Cluster config for the 5M run

- Component memory: with per-batch bounding, the component no longer needs the
  1Gi bump (512Mi default should suffice); the pipeline may still set
  `resources.memory` conservatively.
- Gateway sidecar: Arrow IPC removes the JSON-parse load that crashed it. The
  pipeline's optional `gateway:` block may lower `targetFileSize`/`bufferSize`
  to flush Parquet more often; the sidecar's K8s memory limit itself is an
  operator/chart lever, not doc-settable.

## Data flow

```
HTTP resp.Body --> json.Decoder (stream, one object at a time)
    --> stringify + project (fields | first-record keys)  [per record]
    --> array.RecordBuilder (all-String)                   [accumulate]
    --> every 8192 rows: NewRecord -> serialize IPC stream
    --> writer.WriteChunk(ipc)   [ONE POST PER BATCH]
    --> (paginated: repeat per page; single: one pass)
    --> writer.Close -> client.Commit -> commitAndStatus
```

## Error handling

- Config invalid → `ExitUserError` (unchanged).
- HTTP / stream-decode error → `ExitUserError` (bad source/response).
- Arrow build / WriteChunk / Close / Commit failure → `ExitAppError` (>=20).
- Zero records → commit empty, status "extracted 0 records" (unchanged).

## Testing

- **Unit (component):**
  - streaming decode over each of the three JSON shapes yields the right records
    in order;
  - stringification (string/number/bool/null/nested) → expected string cells;
  - projection column set (fields vs first-record-keys; missing→null);
  - batch boundary: N rows with `arrowBatchRows` small produces ⌈N/batch⌉
    IPC streams, each a valid standalone IPC stream that the gateway's
    `ArrowIPCAdapter.Parse` reads fully (round-trip a batch through
    `ipc.NewReader` and assert row/column counts) — this is the WriteChunk /
    no-concatenation guarantee.
- **Local rig:** run the real extractor against a bounded live NYC 311 pull
  (e.g. 200k rows) through the mock-DG; assert the mock's counted rows match and
  peak RSS stays bounded (well under 512Mi) regardless of `page_size`.
- **e2e re-verify:** run the http-json-extractor e2e scenarios against the new
  Arrow output; confirm row counts/joins still pass with all-String columns.
- **Cluster proof:** the NYC 311 pipeline pulls 5M rows to `Succeeded` with a
  clean row count.

## Phasing

- **Phase 1 (this spec):** streaming Arrow-IPC rewrite + WriteChunk-per-batch +
  mock-DG rig + local proof + e2e re-verify + 5M cluster proof.
- **Phase 2 (optional, later):** bounded-concurrency parallel page fetches to
  cut the Socrata-bound download wall-clock.

## Definition of done

- [ ] Extractor streams Arrow IPC in ≤8192-row batches via `WriteChunk`; peak
      RSS bounded and independent of `page_size`; component OOM cannot recur.
- [ ] A test proves each batch is an independently-parseable IPC stream (no
      concatenation reliance).
- [ ] `cmd/mock-datagateway/` runs the real extractor locally; `make
      extractor-local` reports rows + peak RSS.
- [ ] All-String output documented in `docs/components.md`; e2e re-verified
      green.
- [ ] `go build`/`go vet`/`go test` green for the component; `make tidy` clean.
- [ ] NYC 311 pipeline pulls 5M rows `Succeeded` on the cluster.
- [ ] data-generator row-drop finding filed separately (not fixed here).
