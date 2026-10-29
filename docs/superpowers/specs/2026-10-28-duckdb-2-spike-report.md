# DuckDB 2.0 upgrade: spike report (Task 1)

**Date:** 2026-10-28
**Branch / base:** `feat/duckdb-2.0` @ `16753ba`
**Design:** [`2026-10-28-duckdb-2-upgrade-design.md`](2026-10-28-duckdb-2-upgrade-design.md) (D5, D11)
**Engine under test:** `github.com/duckdb/duckdb-go/v2 v2.20000.0`. `pragma_version()` reports `v2.0.0`, source id `d54cbbe3df`.
**Host:** darwin/arm64 (`osx_arm64` extensions), go1.25.7, Docker 29.8.2.

## Status: GO for Tasks 2–4. No deny row or Quack statement succeeded. Two design points need the controller's attention before Task 3 (see `## Escalations`).

The spike's pin bump, `attach.go` edit and temporary test file were all reverted. This report is the only file committed.

## Build

**Step 1: pins + `make tidy`.** I ran `go mod edit -require=github.com/duckdb/duckdb-go/v2@v2.20000.0` in `components/sql-transform`, `components/queryengine` and `utils/docker/install-duckdb-ext`, then `make tidy` from the repo root, which printed `✓ all modules tidy`.

- `git status --short` showed exactly the six expected files changed: `go.mod`/`go.sum` in each of the three modules. The only other entry was a pre-existing untracked plan file that has nothing to do with this work.
- `git diff --exit-code go.mod go.sum sdk/go/go.mod sdk/go/arrow/go.mod tests/e2e/go.mod components/data-generator/go.mod components/http-json-extractor/go.mod` → exit 0. No other module drifted.
- The per-module go.mod delta was the same shape everywhere (7 lines changed in each):
  - `github.com/duckdb/duckdb-go/v2 v2.10503.0` → `v2.20000.0`
  - `github.com/duckdb/duckdb-go-bindings v0.10503.0` → `v0.20000.0` (indirect)
  - The five `github.com/duckdb/duckdb-go-bindings/lib/<platform> v0.10503.0` lines are **replaced** by `go.duckdb.org/libduckdb/{darwin-amd64,darwin-arm64,linux-amd64,linux-arm64,windows-amd64} v0.20000.0` (indirect). The static libs now resolve from the `go.duckdb.org` module path, which confirms the spec's fact.
  - Nothing else moved. `install-duckdb-ext` keeps `arrow-go/v18 v18.5.1` (indirect); queryengine and sql-transform keep `v18.6.0`.

**Step 2: build.** I ran `CGO_ENABLED=1 go build -tags duckdb_arrow ./...` in each module:

| Module | Result |
|---|---|
| `components/sql-transform` | exit 0, clean (no compiler output) |
| `components/queryengine` | exit 0, clean |
| `utils/docker/install-duckdb-ext` | exit 0, clean |

The bump compiles with zero source changes.

## Unit tests (queryengine)

Command: `cd components/queryengine && CGO_ENABLED=1 go test -tags duckdb_arrow -count=1 ./... 2>&1 | tee /tmp/qe-2.0.txt`

```
ok  	github.com/datuplet/datuplet/components/queryengine	27.735s
ok  	github.com/datuplet/datuplet/components/queryengine/cmd/datuplet-query	1.987s
ok  	github.com/datuplet/datuplet/components/queryengine/cmd/query-worker	5.554s
```

A verbose re-run (`-v`, saved to `/tmp/qe-2.0-v.txt`) gave exit 0, with **62/62 top-level tests PASS and 277 PASS lines including subtests. There were 0 FAIL and 0 SKIP.** No skips means the warmed helper `lockedEngineViaAttach` ran for real. Every test that uses it ran: `TestLockdownDenyUnredacted`, `TestLockdownEgressErrorClass` and the warmed `TestLockdownAllowed` rows. The iceberg/httpfs `v2.0.0` extensions installed into `~/.duckdb/extensions/v2.0.0/osx_arm64/` on first use.

**Failing tests: none.** So there is nothing to classify as *error-text reword*, *result-shape change* or *behaviour change*.

Verbatim 2.0 texts observed in passing assertions, for reference by later tasks (all unchanged in meaning from 1.5.3):

- Unredacted readback (`TestLockdownDenyUnredacted`, warmed): `Invalid Input Error: Displaying unredacted secrets is disabled`
- Read egress (`TestLockdownEgressErrorClass`): `IO Error: Could not connect to server error for HTTP HEAD to 'http://127.0.0.1:1/x.parquet'`
- Write egress (`TestLockdownEgressErrorClass`): `HTTP Error: HTTP PUT error uploading to 's3://127.0.0.1:1/exfil.parquet' (HTTP 400 Bad Request)`

## Deny matrix

`TestLockdownDeny`: 34 rows, **34 PASS** on 2.0.

- **Rows that FAILED because the statement succeeded:** none.
- **Rows that failed only on the layer marker:** none.

Verbatim 2.0 deny text by row family, from the `-v` log:

| Rows | 2.0 verbatim error |
|---|---|
| `lock-integrity/*` (10 rows) | `Invalid Input Error: Cannot change configuration option ""<option>"" - the configuration has been locked`. The doubled double-quotes are DuckDB's own output. |
| `local-fs/*` (15 rows), `replacement-scan/duckdb-extension`, `indirect/*` (2), `extensions/*` (4), `meta/duckdb-extensions` | `Permission Error: File system LocalFileSystem has been disabled by configuration` |
| `replacement-scan/no-extension-etc-passwd` | `Catalog Error: Table with name /etc/passwd does not exist!` + newline + `Did you mean "pg_class"?` |

**Note on existing row notes (not a 2.0 delta).** `extensions/install-json`, `extensions/install-httpfs` and `extensions/load-spatial` deny at the **FS layer** on 2.0, with the LocalFileSystem text. Their notes say "(extension layer)" and they set `wantFSLayer=false`. I checked 1.5.3 in an isolated scratch module (`duckdb-go v2.10503.0`, same six-statement posture), and it returns the identical FS-layer text for all three. So the notes were already imprecise on 1.5.3. Nothing changed and nothing needs fixing for this upgrade.

**Integration deny rows** (`runLockdownDenyIntegration`, run inside `TestAttachIntegration`; see `## ATTACH URI`): `iceberg_scan-local`, `iceberg_metadata-local` and `iceberg_snapshots-local` all PASS with `Permission Error: File system LocalFileSystem has been disabled by configuration`. On 2.0 the message carries a trailing `LINE 1: SELECT * FROM iceberg_scan('/tmp/x')` + caret line, so it is multi-line. Substring markers still match. The iceberg INSERT write-probe still logs "SUCCEEDED post-lock (expected under allowall fixture authz …)", which is pre-existing and expected, and the temp dir stayed file-free.

## Statically linked extensions

`TestZZSpikeInventory`: `openEngine(ctx, Request{TempDir: t.TempDir()})`, **no `lock()`**, then `SELECT extension_name, loaded, installed, install_mode FROM duckdb_extensions() ORDER BY 1`. Full output (2.0.0, darwin/arm64 dev laptop):

| extension_name | loaded | installed | install_mode |
|---|---|---|---|
| autocomplete | true | true | STATICALLY_LINKED |
| avro | false | true | REPOSITORY |
| aws | false | false | NOT_INSTALLED |
| azure | false | false | NOT_INSTALLED |
| core_functions | true | true | STATICALLY_LINKED |
| delta | false | false | NOT_INSTALLED |
| ducklake | false | false | NOT_INSTALLED |
| encodings | false | false | NOT_INSTALLED |
| excel | false | false | NOT_INSTALLED |
| fts | false | false | NOT_INSTALLED |
| httpfs | false | true | REPOSITORY |
| iceberg | false | true | REPOSITORY |
| icu | true | true | STATICALLY_LINKED |
| inet | false | false | NOT_INSTALLED |
| json | true | true | STATICALLY_LINKED |
| lance | false | false | NOT_INSTALLED |
| motherduck | false | false | NOT_INSTALLED |
| mysql_scanner | false | false | NOT_INSTALLED |
| odbc_scanner | false | false | NOT_INSTALLED |
| parquet | true | true | STATICALLY_LINKED |
| postgres_scanner | false | false | NOT_INSTALLED |
| quack | false | false | NOT_INSTALLED |
| spatial | false | false | NOT_INSTALLED |
| sqlite_scanner | false | false | NOT_INSTALLED |
| tpcds | true | true | STATICALLY_LINKED |
| tpch | true | true | STATICALLY_LINKED |
| ui | false | false | NOT_INSTALLED |
| unity_catalog | false | false | NOT_INSTALLED |
| vortex | false | false | NOT_INSTALLED |
| vss | false | false | NOT_INSTALLED |

`avro`, `httpfs` and `iceberg` read `REPOSITORY` only because this laptop's `~/.duckdb/extensions/v2.0.0/osx_arm64/` already holds them; `INSTALL iceberg` pulls `avro` in as a dependency, exactly as on 1.5.3. `install_mode` separates them correctly from linked-in extensions.

**Explicit answers for the named extensions:**

| Name | `install_mode = 'STATICALLY_LINKED'`? | `loaded = true`? | Note |
|---|---|---|---|
| `quack` | **no** (NOT_INSTALLED) | **no** | Not reachable except via INSTALL/LOAD from disk; see Quack probe |
| `httpfs` | **no** (REPOSITORY on this laptop) | **no** | Loaded only by `attachCatalog` pre-lock |
| `iceberg` | **no** (REPOSITORY on this laptop) | **no** | Same |
| `postgres` | **no**: the row is named **`postgres_scanner`** (NOT_INSTALLED) | **no** | No row is literally named `postgres` |
| `mysql` | **no**: the row is named **`mysql_scanner`** (NOT_INSTALLED) | **no** | No row is literally named `mysql` |
| `json` | **yes** | **yes** | |
| `parquet` | **yes** | **yes** | |
| `icu` | **yes** | **yes** | |
| `core_functions` | **yes** | **yes** | |

**Full static set on 2.0.0:** `autocomplete, core_functions, icu, json, parquet, tpcds, tpch`.

**Delta vs 1.5.3:** I ran the same query in an isolated scratch module pinned to `duckdb-go v2.10503.0`, outside the repo. 1.5.3 links `autocomplete, core_functions, icu, json, parquet`. So **`tpch` and `tpcds` are newly statically linked and loaded in 2.0.0.**

I probed this on a cold `lockedEngine` (supplementary `TestZZSpikeTPCHPostLock`). The new surface is **reachable post-lock**:

- `CALL dbgen(sf=0.01)` → ok
- `SELECT count(*) FROM lineitem` → `60175`
- `SELECT count(*) FROM tpch_queries()` → `22`
- `SELECT count(*) FROM tpcds_queries()` → `99`
- `CALL dsdgen(sf=0.01)` → `Catalog Error: Table with name "customer" already exists!` This is a name clash with dbgen's `customer`; the function itself exists and runs.

The engine temp dir stayed empty. This is in-memory data generation only, with no file or network I/O, bounded by `memory_limit` / `max_temp_directory_size` / the request timeout. On 1.5.3 the same `CALL dbgen` fails with `Catalog Error: Table Function with name "dbgen" is not in the catalog, but it exists in the tpch extension.` See `## Escalations`.

**`duckdb_schemas()` columns on 2.0** (DESCRIBE, same pre-lock engine): `oid BIGINT, database_name VARCHAR, database_oid BIGINT, schema_name VARCHAR, comment VARCHAR, tags MAP(VARCHAR, VARCHAR), internal BOOLEAN, sql VARCHAR, parent_schema VARCHAR, parent_schema_oid BIGINT`. `database_name` is still the owning-catalog column, and there is no `catalog_name`. `parent_schema` and `parent_schema_oid` are new and relate to nested namespaces.

## Quack probe (warmed engine)

`TestZZSpikeQuack`: `e, _ := lockedEngineViaAttach(t)`. It did **not** skip: iceberg/httpfs installed and loaded, the ATTACH warm-up failed at the ATTACH stage as required, and lock() succeeded. Each statement ran on the locked warmed engine. Verbatim `t.Logf("%s → %v", stmt, err)` text follows. Blank lines inside the messages are DuckDB's own output.

1. `ATTACH 'quack:127.0.0.1:1' AS q (TOKEN 'x')` →
   ```
   Permission Error: File system LocalFileSystem has been disabled by configuration
   ```
2. `ATTACH 'quack:127.0.0.1:1' AS q (TYPE quack)` →
   ```
   Permission Error: File system LocalFileSystem has been disabled by configuration
   ```
3. `CONNECT 'quack:127.0.0.1:1'` →
   ```
   Permission Error: File system LocalFileSystem has been disabled by configuration
   ```
4. `CONNECT 'postgres:host=127.0.0.1 port=1'` →
   ```
   Permission Error: File system LocalFileSystem has been disabled by configuration
   ```
5. `SELECT * FROM quack_query('quack:127.0.0.1:1', 'SELECT 1', token = 'x')` →
   ```
   Catalog Error: Table Function with name "quack_query" is not in the catalog, but it exists in the quack extension.

   Please try installing and loading the quack extension by running:
   INSTALL quack;
   LOAD quack;

   Alternatively, consider enabling auto-install and auto-load by running:
   SET autoinstall_known_extensions=1;
   SET autoload_known_extensions=1;
   ```
6. `CREATE SECRET qs (TYPE quack, TOKEN 'x', SCOPE 'quack:127.0.0.1')` →
   ```
   Invalid Input Error: Secret type 'quack' does not exist, but it exists in the quack extension.

   Please try installing and loading the quack extension by running:
   INSTALL quack;
   LOAD quack;

   Alternatively, consider enabling auto-install and auto-load by running:
   SET autoinstall_known_extensions=1;
   SET autoload_known_extensions=1;
   ```
7. `SET s3_enable_deprecated_url_parameters = true` →
   ```
   Invalid Input Error: Cannot change configuration option ""s3_enable_deprecated_url_parameters"" - the configuration has been locked
   ```

**No statement succeeded.** None of the errors contains `connection refused`, `connect`, `timed out`, `unauthori[sz]ed`, `invalid token`, `parser error` or `syntax error`. Port 1 is closed, and no error is network-class, so no socket attempt surfaced. Task 3's listener counter is the stronger proof.

**Supplementary 1: cold locked engine** (`TestZZSpikeQuackCold`, `lockedEngine`). All seven texts are **byte-identical** to the warmed run, including row 7. On a cold engine without httpfs the lock check fires before the setting lookup, so row 7's lock text there does not prove the setting exists. D5 already scopes row 7 to the warmed engine.

**Supplementary 2: why rows 1–4 deny at the FS layer** (`TestZZSpikeQuackNoFSDeny`). This engine had the extension posture applied (`autoinstall_known_extensions=false`, `autoload_known_extensions=false`, `allow_community_extensions=false`) but **no** `disabled_filesystems` and **no** lock. The cwd was moved to a temp dir and listed after each statement; it stayed empty, so no stray local DB file was created. Results:
- Rows 1–3:
  ```
  IO Error: Extension "/Users/tomaskacur/.duckdb/extensions/v2.0.0/osx_arm64/quack.duckdb_extension" not found.
  Extension "quack" is an existing extension.

  Install it first using "INSTALL quack".
  ```
- Row 4:
  ```
  IO Error: Extension "/Users/tomaskacur/.duckdb/extensions/v2.0.0/osx_arm64/postgres_scanner.duckdb_extension" not found.
  Extension "postgres_scanner" is an existing extension.

  Install it first using "INSTALL postgres_scanner".
  ```
- Rows 5–6: identical to the locked runs.
- Row 7: `Catalog Error: Setting with name "s3_enable_deprecated_url_parameters" is not in the catalog, but it exists in the httpfs extension.` followed by the same INSTALL/LOAD/auto-install advice block.

Interpretation: DuckDB maps the `quack:` / `postgres:` prefix of an ATTACH/CONNECT target to its extension and attempts an **explicit load of the extension file from the local extension directory, even with `autoload_known_extensions=false`**. On the locked engine, that file read is what the disabled LocalFileSystem refuses. So for rows 1–4 the FS layer is the actual containment layer, behind "not statically linked". 1.5.3 behaves the same for `ATTACH 'postgres:…'`: same FS-layer text, verified in the 1.5.3 scratch module. I did not test whether the load would succeed if `quack.duckdb_extension` were present on disk with FS enabled, because that needs `INSTALL quack`, a download outside this spike's scope.

## ATTACH URI

Command: `cd components/queryengine && CGO_ENABLED=1 go test -tags 'duckdb_arrow integration' -count=1 -run TestAttachIntegration ./... 2>&1 | tee /tmp/qe-int-2.0.txt`

**Pre-existing environment break (not a 2.0 delta).** The first run failed during fixture provisioning, before DuckDB was involved:
```
docker run qe-it-minio: exit status 125
Unable to find image 'minio/minio:RELEASE.2024-12-18T13-15-44Z' locally
docker: Error response from daemon: pull access denied for minio/minio, repository does not exist or may require 'docker login': denied: requested access to the resource is denied
```
MinIO deleted its community images (see the comment in `charts/datuplet-infra/values.yaml`). `integration_test.go` still pins `minio/minio:RELEASE.2024-12-18T13-15-44Z` for the server and `minio/mc` for `makeBucket`, and both are unpullable. This breaks `TestAttachIntegration` on `main` too.

Workaround used, local only and reverted, with no repo change: the chart's drop-in `pgsty/silo:latest` (`RELEASE.2026-10-26T00-00-00Z`, the chart-pinned release, bundles `mc`) was already present locally. I aliased it with:
```
docker tag pgsty/silo:latest minio/minio:RELEASE.2024-12-18T13-15-44Z
docker tag pgsty/silo:latest minio/mc:latest
```
After both runs I removed the aliases with `docker rmi minio/minio:RELEASE.2024-12-18T13-15-44Z minio/mc:latest` (untag only). No `qe-it-*` containers were left behind.

| Variant | Result |
|---|---|
| `attach.go` unmodified (`ENDPOINT '%s'`) | **PASS** (`TestAttachIntegration` 11.21s; run-count, run-maxrows, 3× iceberg-local deny). No deprecation warning in the test log. |
| `attach.go` with `URI '%s'` | **PASS** (9.26s; same subtests, same outputs) |

After the URI run I restored `attach.go` with `git checkout components/queryengine/attach.go` (`git diff --quiet` → clean). Both runs exercised the post-attach `USE lk."<first namespace>"` path with the `duckdb_schemas()`/`database_name` filter (bare `posts` resolved; 7 rows seeded and read).

## Images

Command (repo root): `make build-component-sql-transform build-component-datuplet-query && docker build -t datuplet/query-worker:latest -f utils/docker/query-worker.Dockerfile .` — all three images built on this arm64 host. Each builder stage downloaded `go.duckdb.org/libduckdb/linux-arm64 v0.20000.0`, so the new static-lib module path resolves inside Docker too.

| Image | Arch | Size | Linked (`go version -m` on the extracted binary) |
|---|---|---|---|
| `datuplet/sql-transform:latest` | arm64 | 219 MB | `duckdb-go/v2 v2.20000.0`, `duckdb-go-bindings v0.20000.0`, `go.duckdb.org/libduckdb/linux-arm64 v0.20000.0`, `-tags=duckdb_arrow` |
| `datuplet/datuplet-query:latest` | arm64 | 406 MB | same |
| `datuplet/query-worker:latest` | arm64 | 321 MB | same |

Baked extensions (`find /home/datuplet/.duckdb -type f` inside the final image), identical in `datuplet-query` and `query-worker`:

- `/home/datuplet/.duckdb/extensions/v2.0.0/linux_arm64/httpfs.duckdb_extension` (+ `.info`)
- `/home/datuplet/.duckdb/extensions/v2.0.0/linux_arm64/iceberg.duckdb_extension` (+ `.info`)
- `/home/datuplet/.duckdb/extensions/v2.0.0/linux_arm64/avro.duckdb_extension` (+ `.info`) — pulled in by `INSTALL iceberg` as a dependency, exactly as on 1.5.3

The version directory moved from `v1.5.3` to `v2.0.0`; Task 4's verify stage must pass against this layout. `sql-transform` bakes no extensions, and needs none: parquet, json and icu are statically linked.

Provenance: the spike subagent stalled after the image builds (its final extract-and-inspect call never returned). The controller re-ran that step, filled this section from the measurements, and performed Step 8 (revert + commit) by hand. Every other section is the subagent's own output.

## Findings for Task 2

1. **ATTACH: `ENDPOINT` → `URI` works on 2.0.** `TestAttachIntegration` PASSES with `URI '%s'`, with no other change. `ENDPOINT` also still passes and emits no visible deprecation warning.
2. **No reworded strings and no result-shape changes.** The full queryengine unit suite is green on 2.0 unmodified (62/62 top-level, 0 skips). No test needs a text or shape adaptation for the bump itself.
3. **`duckdb_schemas()` column name on 2.0: `database_name`**, unchanged; there is no `catalog_name`. `attach.go`'s `WHERE database_name = 'lk'` filter is correct on 2.0. The comment "in this DuckDB build (1.5.3)" and the doc comment "(lakekeeper v0.12.1 + DuckDB 1.5.3 iceberg ext)" can be reworded to 2.0.0. New columns `parent_schema`/`parent_schema_oid` exist and are not needed.
4. **`TestAttachIntegration` cannot provision on any branch today.** Its MinIO images are gone (see `## ATTACH URI`). Task 2 must either alias the images locally as above, or swap `minio/minio:RELEASE.2024-12-18T13-15-44Z` and `minio/mc` in `integration_test.go` for the chart's pinned `docker.io/pgsty/silo:RELEASE.2026-10-26T00-00-00Z@sha256:7bb2aff6…` (it accepts `server /data --address :<port>` and bundles `mc`; verified working via the alias). The second option is a code change outside the spike's file list, so it is the controller's call.
5. The make-tidy delta to expect is in `## Build`: the bindings plus five `go.duckdb.org/libduckdb/*` lines replace the five `duckdb-go-bindings/lib/*` lines.
6. On 2.0, iceberg table-function FS denies append a `LINE 1: …` + caret context, making them multi-line. Substring markers (`fsLayerMarker`) are unaffected.

## Findings for Task 3

1. **Static set on 2.0.0:** `autocomplete, core_functions, icu, json, parquet, tpcds, tpch`. 1.5.3 was `autocomplete, core_functions, icu, json, parquet`.
   - `TestStaticallyLinkedExtensions` should assert present: `json`, `parquet`, `core_functions`, **`icu`** (linked).
   - It should assert absent: `quack`, `httpfs`, `iceberg`, **`postgres_scanner`**, **`mysql_scanner`**. `duckdb_extensions()` lists the scanners under those canonical names; there is no row named `postgres` or `mysql`. Asserting absence of `postgres`/`mysql` literally would pass vacuously.
   - Whether to pin `tpch`/`tpcds`/`autocomplete` as present depends on the escalation decision below.
2. **`quack` is NOT statically linked**, and `postgres_scanner` is NOT statically linked.
3. **Recorded text → marker per Quack row** (warmed and cold texts are identical):

   | Row | 2.0 verbatim deny (first line) | Marker that actually matches | D5 class as designed fits? |
   |---|---|---|---|
   | `ATTACH 'quack:…' AS q (TOKEN 'x')` | `Permission Error: File system LocalFileSystem has been disabled by configuration` | `LocalFileSystem` / `disabled` (FS layer); text does **not** contain `quack` | **No.** `extensionUnavailable` cannot match |
   | `ATTACH 'quack:…' AS q (TYPE quack)` | same | same | **No** |
   | `CONNECT 'quack:…'` | same | same | **No** |
   | `CONNECT 'postgres:host=… port=…'` | same | same; text does **not** contain `postgres` | **No** |
   | `quack_query(…)` | `Catalog Error: Table Function with name "quack_query" is not in the catalog, but it exists in the quack extension.` | `quack` + `but it exists in the quack extension` / `Please try installing and loading the quack extension` | Yes: `extensionUnavailable` |
   | `CREATE SECRET qs (TYPE quack, …)` | `Invalid Input Error: Secret type 'quack' does not exist, but it exists in the quack extension.` | `quack` + `Secret type 'quack' does not exist` | Yes: `secretTypeAbsent` |
   | `SET s3_enable_deprecated_url_parameters = true` (warmed) | `Invalid Input Error: Cannot change configuration option ""s3_enable_deprecated_url_parameters"" - the configuration has been locked` | `configuration has been locked` | Yes: `configLocked` |

   Suggested regexes, case-insensitive, built from the verbatim text:
   - `extensionUnavailable`: `but it exists in the quack extension` (with `quack` also present). The phrase "not in the catalog, but it exists in the <ext> extension" is DuckDB's generic missing-from-unloaded-extension text.
   - `secretTypeAbsent`: `secret type 'quack' does not exist`.
   - `configLocked`: `the configuration has been locked`.
   - None of the seven texts contains any word on D5's reject list.

   **Rows 1–4 need a different class.** I suggest an FS-layer class (`fsLayerMarker`, i.e. `LocalFileSystem`/`disabled`) plus the zero-connection listener assertion. `## Quack probe (warmed engine)` → Supplementary 2 documents why: prefix-routed explicit extension load from the local extension dir, refused by `disabled_filesystems`. Optionally, a companion pre-lock assertion on an extension-posture-only engine (FS enabled, unlocked) can pin the underlying `Extension "quack" is an existing extension` / `Extension "postgres_scanner" is an existing extension` text, so the layer is named rather than inferred. This is a design change to D5's row/class table and needs approval; see `## Escalations`.
4. The `extensions/install-*` / `load-spatial` rows of the existing matrix already deny at the FS layer on both 1.5.3 and 2.0 (see `## Deny matrix`). Their "(extension layer)" notes are imprecise but pre-existing. Under D5/D6 the existing matrix stays untouched.

## Findings for Task 6

- **`icu` IS statically linked and loaded on 2.0.0** (`install_mode = STATICALLY_LINKED`, `loaded = true`; also on 1.5.3). The sql-transform README does **not** need to tell users to `LOAD icu`. ICU collations and time-zone functions are available out of the box.
- For the README's "DuckDB engine" section: also statically linked and loaded are `json`, `parquet`, `core_functions` and `autocomplete`, plus, new in 2.0.0, `tpch` and `tpcds` (`dbgen`/`dsdgen`, `tpch_queries()`/`tpcds_queries()`). Whether the README mentions the latter is the controller's call.
- Engine version string to cite: `v2.0.0` (`pragma_version()` source id `d54cbbe3df`), from `duckdb-go/v2 v2.20000.0`.

## Escalations

**Brief criteria: none triggered.** No `TestLockdownDeny` row succeeded and no Quack statement succeeded, on either the warmed or the cold locked engine.

The controller should route two judgement calls to the maintainer before Task 3. Neither is a succeeding deny row:

1. **D5 class table does not fit 2.0 for four rows.** `ATTACH 'quack:…' (TOKEN …)`, `ATTACH 'quack:…' (TYPE quack)`, `CONNECT 'quack:…'` and `CONNECT 'postgres:…'` deny at the **FS layer** (`Permission Error: File system LocalFileSystem has been disabled by configuration`), not with an extension-named message. D5's `extensionUnavailable` predicate (extension name AND a not-loaded phrase) would fail all four. The deny is real and layered: the explicit extension-file load is refused by `disabled_filesystems`, and quack is not linked. But Task 3 needs an approved class for these rows, for example an FS-layer class plus the listener counter. Weakening the predicate silently would be an anti-goal.
2. **New statically linked surface in 2.0.0: `tpch` + `tpcds`**, reachable post-lock (`CALL dbgen(sf=…)`, `CALL dsdgen(sf=…)`, `tpch_queries()`, `tpcds_queries()`). It has no FS or network reach; it is in-memory generation bounded by `memory_limit`, `max_temp_directory_size` and the request timeout. It is still new user-reachable capability. The deny-matrix header says new capability surface must be reconciled on every bump. Options: accept it and pin it in the inventory test as present-by-design, or deny it, which would require a posture change, the only way the `lock()` list may grow per D5.

## Cluster verification

**Date:** 2026-10-29 (Task 7). OrbStack Kubernetes `v1.36.5+orb1`, arm64, docker 29.8.2. Images built from `feat/duckdb-2.0` at `67a93d7`–`6531e82` (no code changes in between). Local DuckDB CLI for the e2e assertions: `v1.4.1`.

### e2e gate

**Run 1: `make e2e-k8s` PASS** (suite `ok ... tests/e2e 303.164s`, teardown ran). Every test passed except the two expected skips:

- PASS: `TestK8sFixturesAreValidDocs` (+9), `TestAgentLoop_UserApps`, `TestAgentLoop_SchemaToVerify`, `TestApps_ViewerAndSecurity` (+13), `TestAuditTrail_ClusterRun`, `TestQuery_LocalMint_Enabled`, **`TestQuery_HappyPath`**, `TestQuery_AuthzDenied`, **`TestQuery_WriteProbe`** (viewer write denied by S3 `403 AccessDenied`), **`TestQuery_StoragePreview`** (881 ms, 4 columns, 100 rows), `TestRegistry_ErrorSchemaInvalid`, `TestRegistry_UnpinnedResolvesStable`, `TestRegistry_Freeze_MidRunMutation`, `TestResources_PutGate` (+3), `TestSecretsLadder`, `TestScenarios/write-modes`, **`TestScenarios/multi-table-join`**, `TestScenarios/fga-matrix-bob-trigger-and-browse`.
- SKIP: `TestQuery_NetworkPolicyEgress` (expected on OrbStack: "NetworkPolicy unenforced on this cluster"), `TestScenarios/error-bad-config` (pre-existing permanent skip, `scenarios_test.go:82-92`), and `TestBigDataJoinProof` (opt-in: needs `RFC011_BIG_DATA_PROOF=1`, which `make e2e-k8s` never sets).
- The query-worker image's offline `verify` stage passed on every build: `OK: INSTALL iceberg / OK: LOAD iceberg / OK: INSTALL httpfs / OK: LOAD httpfs`.

**Run 2: `RFC011_BIG_DATA_PROOF=1 make e2e-k8s-deploy`** (same images). Every other result was identical to run 1. **`TestBigDataJoinProof` FAILED at pipeline PUT, before any component ran**:

```
PUT pipeline "e2e-7124-manual-large-input-join": status=400 body={"error":"validation failed","findings":[{"path":"stages[1].components[0].resources","message":"resource \"memory\" is not allowed for component sql-transform (not listed in registry max)","severity":"error"}]}
```

This is pre-existing and unrelated to DuckDB. The fixture `tests/e2e/pipelines/k8s/manual-large-input-join.yaml` sets `resources: {memory: 2Gi}` on sql-transform, but the e2e bootstrap's sql-transform ComponentDefinition declares no `max`, so the RFC 026 resource gate rejects the doc. It was not fixed. **The big-data join on DuckDB 2.0 is therefore not verified on a cluster.**

### Kept stack (`make deploy-local`, namespace `datuplet`)

**Baseline `examples/pipelines/etl-duckdb.yaml` (`duckdb-transform`, run `5de2e743`): Succeeded.** The run took 28 s. sql-transform exited 0 (`sql-transform completed (1 inputs, 1 outputs)`), and the gateway logged `commit ok for etl.user_summary ... files=1 rows=10`. `datuplet query --sql 'SELECT count(*) FROM etl.user_summary'` returned **10**, and `raw.data` returned 100. `make k8s-retry-duckdb` was not used: it is pre-existing broken because it `kubectl apply`s an envelope-free PipelineDoc.

**Untyped-NULL probe (`duckdb-null-probe` → `etl.null_probe`, SQL `... NULL AS probe_null ...`, run `20d1f465`): FailedApplication.** sql-transform exited 20 in stage `transform`, with the same result on both Job attempts:

```
write output etl.null_probe: WriteChunk: HTTP write failed: 500 Internal Server Error - {"error":"failed to parse input: failed to convert schema: field \"probe_null\": unsupported Arrow type: null","code":"PROCESS_ERROR"}
```

The DuckDB 2.0 output keeps `probe_null` NULL-typed, so it reaches the Data Gateway as Arrow type `null`. The gateway's Arrow→Datuplet type mapping rejects that type (`pkg/datagateway/format/parquet.go:81` wrapping `pkg/datagateway/schema/types.go:253`) before any writer or table exists. `DESCRIBE etl.null_probe` returns `Catalog Error: Table with name null_probe does not exist!`, so nothing was written or committed. The component was not patched; how to handle this is the maintainer's decision. An explicit cast such as `CAST(NULL AS VARCHAR)` is the obvious user-side workaround, but it was not tested.

**query-worker RSS baseline (D9).** `kubectl top` is unavailable on OrbStack (`Metrics API not available`), so memory was read from the pod's cgroup v2 files and `/proc/1/status`. The pod's limit is 1Gi.

- The representative query was a `raw.data` ⋈ `etl.user_summary` GROUP BY. It ran three times, each in 0.35–0.79 s and returning 10 rows.
- Sampling `memory.current` every 2 s gave a maximum of **188.1 MiB** (anon 24.3 MiB, file 162.3 MiB).
- The lifetime cgroup **`memory.peak` was 208.3 MiB**, including page cache. Before the pod's first query it was 63.2 MiB.
- The lifetime process **peak RSS (`VmHWM`) was 100.6 MiB**.
- No DuckDB 1.5.3 figure was measured on this machine for comparison.

**Image IDs.**

| image | e2e runs (Step 1) | kept stack |
|---|---|---|
| `datuplet/query-worker` | `sha256:7e1b2dcadf13…` | `sha256:5000cb585a30…` |
| `datuplet/sql-transform` | `sha256:295d8aae787f…` | `sha256:15701a1a40a6…` (`:v0.13.3`) |
| `datuplet/gateway` | `sha256:6fd7a51314fb…` | `sha256:01de9a2c67c2…` |
| `datuplet/http-json-extractor` | `sha256:d03e5c390558…` | `sha256:f5c6d5d644c0…` (`:v0.13.3`) |
| `datuplet/data-generator` | `sha256:77acbc8f4bf7…` | `sha256:386c6d35bc63…` (`:v0.13.3`) |
| `datuplet/pipeline-api` | `sha256:a37db676396e…` | `sha256:aa9d1278f769…` |
| `datuplet/pipeline-operator` | `sha256:ce58f442d55a…` | `sha256:15bc18b61b73…` |
| `datuplet/pipeline-observer` | `sha256:f7ef87cb68d7…` | `sha256:faed51e7ce0d…` |
| `datuplet/app-worker` | `sha256:d5f5ffe2c2e5…` | `sha256:14288f4ae11c…` |

**Environment incidents (not DuckDB):**

- **Disk.** The first `make deploy-local` failed in the image build with `ResourceExhausted`. The host disk was 97% full (13 GiB free), which put the node under DiskPressure, and the kubelet garbage-collected the e2e images. The retry rebuilt everything once the pressure cleared.
- **Observer.** The local stack's `pipeline-observer` could not reach OrbStack's API endpoint at `:26443`. `tests/local/values-local-app.yaml` lacks the `apiServerEgressPorts` entry that `tests/e2e/values-app.yaml` has, so `datuplet runs` showed the runs as stuck `Pending`. The controller patched the NetworkPolicy live (maintainer-approved), and run phases were read from the PipelineRun CRs.
- **Stale credentials.** After a host sleep and node restart, the first NULL-probe attempt (run `745f2e19`) and the first count queries failed with `InvalidAccessKeyId` on vended S3 credentials. The pure-Go gateway failed in stage `extract` the same way, so the cause is not DuckDB. The errors cleared on their own within about two minutes with no restarts, and the probe was re-run unchanged.
