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
- data-generator changes — the flagged "row-drop" was investigated and is **not
  a bug** (the SDK disables Arrow-IPC batching); see §2.

## Design

### 1. Streaming Arrow-IPC output (both extraction paths)

Replace the read-all/unmarshal/JSONL path with a streaming pipeline:

- **Stream-decode** the HTTP response with `json.Decoder`: read the opening
  tokens to position at the record array (handling the three shapes the current
  `parseJSON` supports — bare array `[...]`, positional `[meta,[...]]`,
  object-wrapped `{key:[...]}`), then `Decode` one record object at a time. No
  `io.ReadAll`, no whole-page `[]map[string]any`. Preserve current edge-case
  behavior: **non-object array elements are skipped** (as `recordsFromSlice`
  does today, `main.go:408-417`), and an empty array yields zero records (not an
  error). For object-wrapped responses without `array_path`, select the first
  array-valued field in **document order** — the current code picks "a" first
  array field via unordered Go-map iteration (`main.go:384-393`), so this is a
  minor, benign determinism improvement.
- **Accumulate** decoded records into an Arrow `array.RecordBuilder` whose
  schema is **all `arrow.BinaryTypes.String`** (nullable). Column set:
  - `fields` set → the projected `name`s, in declared order; each value read via
    the existing `getValueRaw(record, path)` then **stringified**.
  - `fields` unset → columns = the **sorted union of keys across the first
    batch** of decoded records — matching the current JSONL path, whose gateway
    inference collects field names from all objects in the first chunk
    (`pkg/datagateway/schema/inference.go:343-356`). Fixed for the run; later
    records project onto that set (missing key → null, keys unseen in the first
    batch → ignored). (First-record-only keys would drop later-only columns and
    regress vs today — hence the whole-first-batch union.)
- **Stringify** a value: JSON string → as-is; number/bool → its canonical Go
  string; `null`/absent → Arrow null; nested object/array → compact JSON string
  (`json.Marshal`).
- **Flush every `DefaultBatchRows` (8192) records**: build the record,
  serialize a **self-contained IPC stream** (schema + record + EOS, like
  data-generator's `serializeRecordToIPC`), and ship it.

The accumulate/stringify/flush machinery is packaged as
**`sdk/go/arrow.StringSink`** *(revised 2026-07-28)* — the writer-side
counterpart of that opt-in SDK module's existing Arrow reader — so any Go
component can reuse the all-String batched writer and its optional
first-batch schema inference from the component container. Explicit columns
are supplied via a `WithColumns(names, extract)` option; the
http-json-extractor keeps only a thin component-side adapter mapping its
`fields[]` config (`FieldMapping` + `getValueRaw` dot-paths) onto that
option. Base `sdk/go` stays arrow-free (same opt-in split as the reader).

Open the writer with `sdk.WithFormat(pb.DataFormat_FORMAT_ARROW_IPC)`. Peak
component memory ≈ one 8192-row batch, independent of `page_size` and of the
total row count.

### 2. One IPC stream per POST (the SDK already guarantees this for Arrow)

The gateway's `ArrowIPCAdapter.parseFromReader`
(`pkg/datagateway/format/arrow_ipc.go:74`) creates **one** `ipc.Reader` per POST
body and reads only the **first** IPC stream (its `for reader.Next()` loop
handles multiple record batches *within one stream*, not multiple concatenated
streams). So the correctness invariant is: **each batch's IPC stream must reach
the gateway as its own POST** — the gateway must never receive two concatenated
IPC streams in one body.

The SDK already enforces this for Arrow IPC: `OpenWriter` **force-disables
`Write` batching** when the format is `FORMAT_ARROW_IPC`
(`sdk/go/client.go:378-380`, *"so callers can't accidentally re-enable it"* —
Arrow IPC is not append-safe, unlike JSONL/CSV), so `writer.Write` sends each
call immediately (`sdk/go/client.go:576-579`) — one POST per batch. The
extractor may therefore use **`writer.Write` per batch** (matching
data-generator); `WriteChunk` is equally valid (also immediate). Either way the
rule is: **write exactly one IPC stream per `Write`/`WriteChunk` call, never
pre-concatenate batches.** A test must assert this no-concatenation invariant —
that each batch round-trips through `ArrowIPCAdapter.Parse` as a complete,
fully-read IPC stream.

> **Investigated, NOT a bug:** an earlier draft flagged data-generator's batched
> `writer.Write` of sub-1MiB Arrow batches as a row-drop risk. It is safe — the
> SDK override above makes Arrow-IPC `Write` immediate. Filed then closed as
> kacurez/datuplet#35.

### 3. Mock-DG local debug rig (`cmd/mock-datagateway/`)

A standalone Go program that lets the **real extractor binary** run locally:

- **gRPC server on `:50051`** implementing the `DataGateway` methods the
  extractor's non-sample path actually calls: `GetConfig`, `OpenWriter`,
  `CloseWriter`, `Commit`, `Log` (plus `WriteChunk` for the gRPC write fallback
  when no HTTP endpoint is offered). `Shutdown` is **not** required — the SDK's
  `Close` only closes the gRPC connection (`sdk/go/client.go:1119-1121`). Built
  on `pb.UnimplementedDataGatewayServer`.
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
- **A pre-existing typed table** (columns the JSONL path inferred as
  Int64/Float64) can mismatch String — and this is **not only an APPEND
  concern**: the gateway loads an existing table as-is
  (`pkg/datagateway/lakekeeper/lakekeeper.go:150-151`) and FULL_LOAD dispatches
  `ReplaceDataFiles` **without** any catalog-schema replacement
  (`pkg/icebergjob/commit_shared.go:225-230`), so even replacing data on an
  existing typed table can hit the String-vs-Int64 mismatch. Only a
  **fresh/missing table** first created by this all-String version is
  unambiguously safe. Practically: land into a new table name, or drop/recreate
  an existing typed table.
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
    --> stringify + project (fields | first-batch key union)  [per record]
    --> array.RecordBuilder (all-String)                   [accumulate]
    --> every 8192 rows: NewRecord -> serialize IPC stream
    --> writer.Write(ipc)   [ONE POST PER BATCH]
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
  - batch boundary / no-concatenation invariant: N rows with `arrowBatchRows`
    small produces ⌈N/batch⌉ IPC streams, each a valid **standalone** IPC stream
    that the gateway's `ArrowIPCAdapter.Parse` reads fully (round-trip one batch
    through `ipc.NewReader`, assert row/column counts). The SDK guarantees one
    POST per batch for Arrow IPC, so the test asserts the per-batch stream is
    self-contained — not a specific write method.
- **Local rig:** run the real extractor against a bounded live NYC 311 pull
  (e.g. 200k rows) through the mock-DG; assert the mock's counted rows match and
  peak RSS stays bounded (well under 512Mi) regardless of `page_size`.
- **e2e re-verify:** run the http-json-extractor e2e scenarios against the new
  Arrow output; confirm row counts/joins still pass with all-String columns.
- **Cluster proof:** the NYC 311 pipeline pulls 5M rows to `Succeeded` with a
  clean row count.

## Phasing

- **Phase 1 (this spec):** streaming Arrow-IPC rewrite + one-IPC-stream-per-`Write` +
  mock-DG rig + local proof + e2e re-verify + 5M cluster proof.
- **Phase 2 (optional, later):** bounded-concurrency parallel page fetches to
  cut the Socrata-bound download wall-clock.

## Definition of done

- [ ] Extractor streams Arrow IPC in ≤8192-row batches via `writer.Write`; peak
      RSS bounded and independent of `page_size`; component OOM cannot recur.
- [ ] A test proves each batch is an independently-parseable IPC stream (no
      concatenation reliance).
- [ ] `cmd/mock-datagateway/` runs the real extractor locally; `make
      extractor-local` reports rows + peak RSS.
- [ ] All-String output documented in `docs/components.md`; e2e re-verified
      green.
- [ ] `go build`/`go vet`/`go test` green for the component; `make tidy` clean.
- [ ] NYC 311 pipeline pulls 5M rows `Succeeded` on the cluster.
- [ ] (done) data-generator "row-drop" investigated — NOT a bug (SDK disables
      Arrow-IPC batching); kacurez/datuplet#35 closed.
