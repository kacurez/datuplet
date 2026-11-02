# All-Go gateway: verification report (Task 9)

Branch `feat/all-go-gateway`, verified 2026-11-02. e2e ran at `1bb2053b`; the final perf numbers come from `de5f45a8`.
Specs: `2026-11-01-all-go-gateway-design.md`, `2026-11-02-gateway-iceberg-partitioning-design.md` (rev 6).

## e2e on OrbStack

`make e2e-k8s` (isolated kubeconfig, namespace `datuplet-e2e`, CRDs reloaded by the target) exited 0 in 285 s. Every
scenario passed. Three tests were skipped by their own guards, as they are on `main`:

- `TestQuery_NetworkPolicyEgress`: kindnet does not enforce NetworkPolicy.
- `TestBigDataJoinProof`: needs `RFC011_BIG_DATA_PROOF=1`.
- `TestScenarios/error-bad-config`: hard-coded skip for a known operator/harness race.

The e2e suite has no partitioned scenario. Partitioning is verified by unit and integration tests and by the
benchmarks below. The CRD change (partition transform `enum` → `pattern`) is applied by the e2e target; any other
cluster needs `make k8s-reload-crds`.

## Performance

Local fixture: Postgres, Lakekeeper `v0.12.1` and silo on the host network. The gateway runs as a host process. The
source is http-json-extractor over `jsonsrv`, writing the L dataset: 2 000 000 rows, 12 columns. Each figure is the
median of 3 measured runs after one warm-up, unless *n* is given. Peak memory is the gateway's max RSS. The host load
average was 6–8 throughout, so times are noisy. `base` = `main` `ecff5c5` `datuplet gateway`; `allgo` = this branch.

### Unpartitioned write and read

| | base | allgo |
|---|---|---|
| Write L, wall time | 22.07 s | 18.77 s |
| Write L, peak memory | 536 MiB | 517 MiB |
| Write L, gateway CPU | 5.1 s | 5.1 s |
| Write L, `GOMEMLIMIT=200MiB` | — | 21.74 s / 315 MiB |
| Full `FORMAT_PARQUET` read, table written by base | 0.502 s | 0.365 s |
| Full `FORMAT_PARQUET` read, table written by allgo | 0.495 s | 0.319 s |

allgo serves clean files raw and reads about 10 % more bytes from storage than base: one extra footer read per file,
for the raw-eligibility check.

**Merge-on-read delete.** 1 % of rows (ids 0–19 999) were deleted from one file of an allgo table with iceberg-go.
Every planned task then carried delete files, because of the default `truncate(16)` file-path metrics (a known
limitation). The read returned 1 980 000 rows (correct; base would return the deleted rows), `raw_files=0
decoded_files=2`, in 4.74 s. Decoding is about 15× slower than raw passthrough.

### Partitioned writes (identity bucket on `id`, L dataset)

Peak memory and wall time as the fixes landed (Tasks 8b–8e):

| Config | 8b (cap 32) | 8c budget + cap 16 | 8d 5 MiB uploads | 8e 64 KiB dictionaries |
|---|---|---|---|---|
| bucket[8] | 31.6 s / 1 174 MiB | 17.7 s / 1 221 MiB | 17.3 s / 1 172 MiB | **16.9 s / 1 031 MiB**, 8 files, commit 0.18 s |
| bucket[16] | — | 20.4 s / 1 520 MiB | 20.1 s / 1 355 MiB | **17.3 s / 961 MiB** |
| bucket[200] (n=1) | 532 s / 564 MiB, 48 324 files | 524 s / 472 MiB, 48 829 files | 581 s / 550 MiB | **506 s / 530 MiB, 48 829 files, commit 52.7 s** |
| Unpartitioned reference | 517 MiB | 549 MiB | 522 MiB | 553 MiB (n=2) |

Heap profiles of bucket[16] were taken with a throwaway pprof build via `go build -overlay`:

- Before 8e, peak in-use heap was 968 MB. About 450 MB of it was Parquet dictionary-encoder hash tables (float64
  288 MB), plus dictionary buffers. That was the real cost; the 8c budget and 8d upload sizes barely moved it.
- After 8e, peak in-use heap was 573 MB: buffered data pages (about 420 MB, bounded by the 256 MiB budget plus page
  build buffers) and zstd state (64 MB). The dictionary tables are gone.
- RSS is roughly in-use heap plus Go GC headroom. With bucket[16] at `GOMEMLIMIT=512MiB`, peak memory was 557 MiB but
  wall time was 39.6 s and CPU 21 s. A 64 MiB budget gave 882 MiB, a minor gain. In Kubernetes the gateway sets
  `GOMEMLIMIT` to 70 % of the pod limit (Task 8), which bounds peak memory at a CPU cost.

**Accepted bound (ruling R9-1):** about 1 GiB peak memory with no pod memory limit, for 8–16 open partitions on this
dataset. The default budget stays at 256 MiB.

**High cardinality.** More distinct tuples per batch than the open-file cap cannot be streamed cheaply. bucket[200]
writes about 41 rows per file, takes 8–10 minutes and commits in 50–85 s. The writer now logs the remedy: cluster the
input by the partition column or use a coarser transform. This is documented in `known-limitations.md` and
`troubleshooting.md`.

## Not verified

- GCS on the Go data plane: only the integration tests against fake-gcs-server exercise the `gs://` IO.
- A cgroup-limited run in a real pod. `GOMEMLIMIT` was set by hand in the benchmarks, and Task 8's unit tests cover
  the cgroup read.
- A partitioned pipeline end to end on the cluster. It is covered by `TestIntegrationServerPartitionedWrite` against
  Lakekeeper and silo.

## Commands and raw results

- Harness (scratch, not committed):
  `/private/tmp/claude-501/-Users-tomaskacur-mydevel-datuplet/3d389a6c-eeb7-4104-b41d-80cf4ba3d039/scratchpad/perf`.
  It contains `baseline/run.sh` (`GW_IMPL=base|allgo|allgoprof`, `GW_CONFIG`), `interleave3.sh`, `read-interleave.sh`,
  `allgo-*.sh`, `sum-allgo.py`, `sum-reads.py`, `mordelete/` and `prof/`. Results are under `results/perf/` in the
  `allgo-*`, `rm-*`, `rm2-*`, `rm3-*`, `prof16*` and `diag-*` directories.
- e2e log: `.../scratchpad/e2e-allgo.log`.
- Cleanup: the fixture was torn down (`fixture/down.sh`), all Docker volumes were pruned (10.56 GB reclaimed, 0
  left), and the harness helper processes were stopped.
