# All-Go write plane spike — benchmark (2026-11-01)

Question: can the gateway drop DuckDB for writes too, without losing the DuckDB write gains?

## Subjects

| impl | what | binary |
|---|---|---|
| base | main `ecff5c5`, `datuplet gateway` (buffer pkg + pqarrow + minio upload + AddFiles commit) | `bin/datuplet-base` |
| cand | `feat/duckdb-sidecar` DuckDB plane (COPY TO parquet, ZSTD) + AddFiles commit | `bin/datagateway` (179 MB, CGO) |
| go | SPIKE `spikes/go-write-plane` (uncommitted, worktree): pqarrow ZSTD `WriteBuffered`, same row-group rows (229376) and rotation (4 x 128 MiB uncompressed) as duckplane, 4-batch channel, streamed upload through iceberg-go IO (gocloud s3blob multipart), commit = existing commit pool (AddFiles) | `bin/gogateway` (108 MB, CGO_ENABLED=0) |

Harness: the Task 9 perf harness (`baseline/run.sh`, `GW_IMPL=base|cand|go`), dataset L = 2,000,000 rows x 12 columns from `jsonsrv`, unmodified `http-json-extractor`. Interleaved base, cand, go per round; w0 warm-up discarded; r1..r3 measured; 10 s settle. Every run verified with DuckDB CLI: 2,000,000 rows, 2 files, 9 row groups (max 229376), 1 Lakekeeper commit.

## Results — interleaved (results/perf/write3)

| impl | wall s | gw RSS MiB | sum RSS MiB | gw CPU s | file bytes | wall vs base | sum RSS vs base |
|---|---|---|---|---|---|---|---|
| base | 18.85 | 569.8 | 635.9 | 4.38 | 202,771,348 | 1.000 | 1.000 |
| cand (DuckDB) | 15.92 | 333.6 | 399.2 | 3.09 | 183,278,669 | 0.844 | 0.628 |
| go | 15.40 | 513.1 | 579.4 | 4.53 | 202,005,648 | 0.817 | 0.911 |

## Results — go with GOMEMLIMIT=200MiB (results/perf/write3-gomem, separate round, same harness)

| impl | wall s | gw RSS MiB | sum RSS MiB | gw CPU s | file bytes |
|---|---|---|---|---|---|
| go + GOMEMLIMIT=200MiB | 15.57 | 316.7 | 381.8 | 5.40 | 202,005,648 |

## Reading

- Wall: the Go plane matches DuckDB (15.4–15.6 s vs 15.9 s). Main's 18.9 s was its buffer/upload pipeline, not pqarrow.
- Memory: default Go GC keeps ~513 MiB RSS; with GOMEMLIMIT=200MiB it is 317 MiB, at DuckDB's level (334 MiB). The gap was GC headroom, not live data.
- CPU: Go uses 4.5 s (5.4 s with the memory limit, extra GC) vs DuckDB 3.1 s. This run is component-bound (extractor ~18 s CPU), so it does not show in wall; with a faster component on a 1-CPU sidecar the gateway CPU would start to matter.
- Files: DuckDB's are ~10% smaller (183 vs 202 MB), same codec and row groups.

## Not covered by the spike

GCS; Abort cleanup of partially uploaded objects; credential refresh mid-file; the duckplane type casts (go writes Arrow types as given, like main); behaviour under a real cgroup CPU/memory limit; reads (not implemented in the spike); XL dataset.
