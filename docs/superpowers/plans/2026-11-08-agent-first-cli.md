# Agent-first `datuplet` CLI — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the `datuplet` CLI installable with one command on macOS/Linux, authenticated with a revocable agent token, uniform in flags/JSON/exit codes, and self-describing — per the approved spec.

**Architecture:** The gateway sidecar leaves the CLI binary (`cmd/gateway/`), so `cmd/datuplet` becomes a thin `CGO_ENABLED=0` HTTP client cross-compiled and attached to GitHub Releases. The CLI is rebuilt on cobra with one global flag set, one exit-code table, one error envelope and one shared HTTP/session layer. pipeline-api gains hashed, revocable `dt_…` API tokens that replace the cli-api JWT as the only CLI credential, a request-id middleware, `/api/v1/version`, a project-aware query-token mint and a live-pod run-logs endpoint.

**Tech Stack:** Go 1.27 (`github.com/spf13/cobra`), GitHub Actions (`softprops/action-gh-release`), POSIX `sh` install script, Postgres (one new table in the fresh-install schema), client-go for log tailing.

**Spec:** `docs/superpowers/specs/2026-11-08-agent-first-cli-design.md` (rev. after the 2026-11-08 Codex review; its 13 findings are folded into the tasks below and marked *(CR-n)*).

## Global Constraints

- Green field: no compatibility shims, no legacy files, no aliases kept for continuity. Internal consumers (e2e, skill, docs) are updated in the same task that breaks them.
- `cmd/datuplet` is built with `CGO_ENABLED=0`; after Phase 1 `go list -deps ./cmd/datuplet` contains no `arrow-go`, `iceberg-go`, `grpc` or `datupleticeio` package.
- Never print a bearer token (`dt_…` or JWT) except the one-time `auth token create` / `auth login` output defined in Phase 3. Envelope messages are built from the server's `error` field, never by echoing request input.
- Exit codes: `0` ok · `1` user-side · `2` usage · `3` auth · `4` not found · `9` conflict · `20` platform/transport. Streams under `--json`: exit 1 **with** a result document → document on stdout, stderr empty; exit 1 **without** one, and every 2/3/4/9/20 → one envelope on stderr, stdout empty. Nothing else is ever written to either stream under `--json`.
- Error envelope: `{"error":{"code":"<snake_case>","message":"…","http_status":<int|0>,"request_id":"<uuid|omitted>","hint":"…","exit_code":<int>,"details":{…optional}}}`.
- Every leaf has a JSON success document under `--json` (see each task's Interfaces); human-mode banners are suppressed under `--json`.
- Config: `~/.datuplet/config.json` (0600), `$DATUPLET_CONFIG` overrides the path. Env: `DATUPLET_REMOTE`, `DATUPLET_API_TOKEN`, `DATUPLET_PROJECT`, `DATUPLET_CONTEXT`, `DATUPLET_WORKSPACE`, `DATUPLET_CONFIG`. **Binding rule** (spec §2c): explicit token ⇒ explicit remote required, saved metadata ignored; no explicit token ⇒ context token, remote must equal the context's remote.
- Token format `dt_<token_id>.<secret>`: `token_id` UUID (36 chars), `secret` 32 random bytes base64url unpadded (43 chars). Stored as `salt BYTEA` + `secret_hash = sha256(salt || secret)`, compared with `crypto/subtle.ConstantTimeCompare`. Default TTL `2160h`, max `8760h`.
- Version check: `MAJOR.MINOR` equality, probed in the shared session preflight (2 s, best-effort, skipped under `--quiet`); human-mode stderr warning, silent under `--json`, never a non-zero exit.
- Monorepo tidy: always `make tidy`.
- Workflow: each phase is one feature branch off `public/main` ending in one **draft** PR; phase N+1 stacks on N if unmerged. Never push `main` or tag. e2e runs in CI. Reviewer per task: Codex via the companion runner (`task`, read-only), fallback a fresh Claude reviewer. `make lint` before each PR.

## Review Focus

1. `pipeline list --json` with no token anywhere → stdout empty, stderr one envelope `code:"auth_missing"`, exit 3. (Task 2.1)
2. `install-cli.sh` with an unwritable `/usr/local/bin` and no sudo → installs to `~/.local/bin`, prints a PATH hint, exit 0. (Task 1.4)
3. `DATUPLET_PROJECT=nope` headless with two accessible projects → exit 4, `code:"project_not_found"`, `details.available` lists the names. (Task 3.3)
4. `dt_` token revoked while `runs start --wait` polls → next poll's 401 ends the command with exit 3 and a hint, no further polls. (Task 2.3)
5. `runs logs <id>` after the Jobs were garbage-collected but the PipelineRun still exists → exit 4, `code:"logs_expired"`, hint names the 1 h window. (Task 5.2)
6. *(CR-1)* saved context for remote A, user passes `--remote B` and no token → exit 2 `code:"context_remote_mismatch"`, **no HTTP request is made**. (Task 3.3)

---

# Phase 1 — Distribution (branch `feat/cli-p1-distribution`)

### Task 1.1: Move the gateway entrypoint out of the CLI

**Files:**
- Create: `cmd/gateway/main.go`; move `cmd/datuplet/{gateway.go,gateway_test.go,gateway_boot_test.go}` → `cmd/gateway/`
- Modify: `cmd/datuplet/main.go` (drop `gateway` case + help, the `datupleticeio` import, `resolveRunTokenPath`), `utils/docker/Dockerfile.gateway:21` (`./cmd/gateway` → `/gateway`, `ENTRYPOINT ["gateway"]`), `Makefile` (`build-gateway`), `tests/e2e/scenarios_agent_loop_test.go:32` (comment)
- Create: `cmd/datuplet/deps_test.go`

**Interfaces:**
- Produces binary `gateway` with today's flags; image ENTRYPOINT `gateway`, CMD `--local`. The controller passes `Args` only (`pkg/k8s/controllers/pipelinerun_jobs.go`), so it is untouched.

- [ ] **Step 1: Test** — `TestCLIBinaryHasNoDataPlaneDeps`: `go list -deps .` output contains none of `arrow-go`, `iceberg-go`, `google.golang.org/grpc`, `pkg/datupleticeio`, `pkg/datagateway`.
- [ ] **Step 2: Run** `go test ./cmd/datuplet -run TestCLIBinaryHasNoDataPlaneDeps` → FAIL.
- [ ] **Step 3: Implement** the move; `cmd/gateway/main.go` = `os.Exit(gatewayMain(os.Args[1:], os.Getenv, os.Stderr))` with the `datupleticeio` blank import; Dockerfile + Makefile edits.
- [ ] **Step 4: Verify** — `go test ./cmd/datuplet/... ./cmd/gateway/...` PASS; `go build -o /tmp/d ./cmd/datuplet && stat -f%z /tmp/d` < 30 MB; `docker build -f utils/docker/Dockerfile.gateway .` succeeds.
- [ ] **Step 5: Commit** — `refactor(cli): move the gateway sidecar entrypoint to cmd/gateway`

### Task 1.2: Build-time version info; `GET /api/v1/version`

**Files:**
- Create: `pkg/buildinfo/{buildinfo.go,buildinfo_test.go}`, `cmd/datuplet/{version.go,version_test.go}`, `pkg/pipelineapi/http/{version_handler.go,version_handler_test.go}`
- Modify: `cmd/datuplet/main.go` (`version` case), `pkg/pipelineapi/http/server.go` (register `GET /api/v1/version` next to `/healthz`, unauthenticated), `Makefile` (`LDFLAGS`), the six service Dockerfiles (`ARG VERSION=dev COMMIT=none` → `-ldflags`), `.github/workflows/_release-image.yml` (`build-args:` in both build steps)

**Interfaces:**
- `package buildinfo`: `var Version, Commit, Date = "dev","none","unknown"`; `type Build struct{Version,Commit,Date string}`; `func Info() Build`; `func MinorMismatch(client, server string) bool` (both parse as `v?X.Y[.Z]` and `X.Y` differ; `dev`/unparseable → false).
- `GET /api/v1/version` → `{"version","commit"}`.
- CLI this phase: `datuplet version [--json]` → `{"client":{…},"server":{"version","commit"},"minor_mismatch":bool}`; `server` omitted when no remote resolves; human mode prints both and the warning line on mismatch; exit 0 always. *(CR-13: the per-command preflight is Task 2.1's `resolveSession`, which calls `buildinfo.MinorMismatch`; this task only ships the pieces.)*

- [ ] **Step 1: Tests** — `TestMinorMismatch` table (`0.14.1/0.14.3`→false, `0.14.0/0.15.0`→true, `dev/0.15.0`→false, `v0.14.0/0.14.9`→false); `TestVersionHandlerUnauthenticated`; `TestVersionJSONWithoutRemote`; `TestVersionJSONMinorMismatchFlag` (httptest returns 0.99.0 → `minor_mismatch:true`, stderr empty under `--json`, exit 0); `TestVersionHumanWarns` (stderr contains "minor versions differ").
- [ ] **Step 2–4:** fail → implement → PASS; `make build && bin/datuplet version --json | jq -e .client.version`; `docker build --build-arg VERSION=0.0.1 -f utils/docker/Dockerfile.pipeline-api .`.
- [ ] **Step 5: Commit** — `feat: build-time version info for the CLI and pipeline-api`

### Task 1.3: CLI tarballs on GitHub Releases, plus the pre-release smoke workflow *(CR-12)*

**Files:**
- Create: `scripts/build-cli.sh`, `tests/scripts/build_cli_test.sh`, `.github/workflows/cli-prerelease-smoke.yml`
- Modify: `.github/workflows/release.yml` (`cli` job, `needs: verify-tag`), `.github/workflows/pr.yml` (`cli-cross-build` job)

**Interfaces:**
- `scripts/build-cli.sh <version-without-v> <outdir>` → `datuplet_<version>_{darwin,linux}_{amd64,arm64}.tar.gz` (each exactly one `datuplet` file) + `checksums.txt` (`sha256sum` format, basenames).
- `release.yml` `cli` job: checkout, `setup-go` from `go.mod`, `build-cli.sh "${GITHUB_REF_NAME#v}" dist`, `softprops/action-gh-release@v2` with `files: dist/*`, `fail_on_unmatched_files: true`, `generate_release_notes: true`; `permissions: contents: write`.
- `cli-prerelease-smoke.yml`: `on: workflow_dispatch` (input `ref`, default the dispatched ref); same build; `softprops/action-gh-release@v2` with `tag_name: cli-smoke-${{ github.run_id }}`, `target_commitish: ${{ github.sha }}`, `prerelease: true`, `name: "CLI smoke (not a release)"`. The tag is outside `v*.*.*`, so `release.yml`/`release-verify.yml` never fire. Job summary prints the asset URLs; the maintainer deletes the release and tag afterwards (`gh release delete <tag> --cleanup-tag -y`). Acceptance for this task = that workflow is green once with five assets on the pre-release page. If it 403s, switch both jobs to `peaceiris/actions-gh-pages@v4` (`keep_files: true`, `destination_dir: cli/<version>`) + a `cli/latest.txt` write, and update the spec.

- [ ] **Step 1: Test** — `tests/scripts/build_cli_test.sh`: four tarballs + checksums exist; `tar tzf` lists exactly `datuplet`; `sha256sum -c` passes; `file` on linux/amd64 says `statically linked`.
- [ ] **Step 2–4:** fail → implement → PASS; `actionlint` on the three workflows clean.
- [ ] **Step 5: Commit** — `ci: build CLI tarballs; publish on release tags; pre-release smoke workflow`

### Task 1.4: One-line installer, docs, release-verify coverage

**Files:**
- Create: `scripts/install-cli.sh`, `tests/scripts/install_cli_test.sh`
- Modify: `.github/workflows/release-verify.yml` (new step: installer for `${{ steps.ver.outputs.version }}`, then `datuplet version --json`), `README.md`, `docs/install.md` ("Install the CLI"), `docs/agent-quickstart.md`, `.claude/skills/datuplet-operator/SKILL.md`

**Interfaces:**
- `curl -fsSL https://raw.githubusercontent.com/kacurez/datuplet/main/scripts/install-cli.sh | sh`; env `DATUPLET_VERSION` (default latest via `api.github.com/repos/kacurez/datuplet/releases/latest` `tag_name`), `DATUPLET_INSTALL_DIR` (default `/usr/local/bin` if writable else `$HOME/.local/bin`), `DATUPLET_RELEASE_BASE` (default `https://github.com/kacurez/datuplet/releases/download`). Downloads tarball **and** `checksums.txt` from the same `<base>/<tag>/` directory and verifies before `install -m 0755`; prints `installed datuplet <version> to <path>` and a PATH hint when needed; exit 1 on any failure with nothing left in the install dir.

- [ ] **Step 1: Test** — serve `build-cli.sh` output with `python3 -m http.server`; case A installs and `version` prints the version; case B (Review Focus 2) read-only decoy for the default dir → lands in `$HOME/.local/bin`, stdout contains `export PATH`; case C corrupt checksum → exit 1, no file.
- [ ] **Step 2–4:** fail → implement (POSIX `sh`, `uname`, `curl -fsSL`, `shasum -a 256 -c`/`sha256sum -c`, `mktemp -d`) → PASS; add to `pr.yml` `cli-cross-build`; docs updated; `actionlint` clean.
- [ ] **Step 5: Commit** — `feat(cli): one-line installer and install docs`; `make lint`; push; draft PR.

---

# Phase 2 — One contract (branch `feat/cli-p2-contract`)

### Task 2.1: cobra root, exit codes, envelope, shared HTTP client, request-id middleware *(CR-8, CR-10, CR-13)*

**Files:**
- Create: `cmd/datuplet/{root.go,exitcodes.go,output.go,httpclient.go,output_test.go,root_test.go,httpclient_test.go}`, `pkg/pipelineapi/http/{requestid.go,requestid_test.go}`
- Modify: `cmd/datuplet/main.go` (`os.Exit(execute(os.Args[1:], os.Stdin, os.Stdout, os.Stderr))`), `pkg/pipelineapi/http/server.go` (wrap the mux with `withRequestID`), `pkg/pipelineapi/http/json.go` (`writeError` adds `"request_id"` from ctx), `go.mod` (`github.com/spf13/cobra`; `make tidy`)
- Delete: `exitCodeErr` (pipeline.go), per-command `http.Client` vars as each command is ported

**Interfaces:**
```go
// exitcodes.go
const ( ExitOK=0; ExitUser=1; ExitUsage=2; ExitAuth=3; ExitNotFound=4; ExitConflict=9; ExitPlatform=20 )
type cliError struct { Code, Message string; HTTPStatus int; RequestID string; Hint string; Exit int; Details map[string]any }
func (e *cliError) Error() string
func errf(exit int, code, format string, a ...any) *cliError
// Maps HTTP → cliError. message = body.error when the body is {"error":…}, else "HTTP <status>". request_id kept only if uuid.Parse succeeds.
func fromHTTP(op string, resp *apiResponse) *cliError // 401/403→ExitAuth "auth_rejected" (hint: auth login / token revoked?); 404→ExitNotFound "not_found"; 409→ExitConflict "conflict"; 408/429/5xx→ExitPlatform "server_error"; other 4xx→ExitUser "request_rejected"
// httpclient.go
type apiResponse struct { Status int; Body []byte; RequestID string }
// No http.Client.Timeout (a ctx deadline cannot extend one); CheckRedirect refuses every redirect
// (the bearer and a request body must never replay to another host/port). (CR2-2, CR2-3)
type apiClient struct { base, token string; http *http.Client }
// Every call sets its own deadline from this table (ctx.WithTimeout inside do when the caller's ctx has none):
var opTimeouts = map[string]time.Duration{ "default": 30*time.Second, "query": 6*time.Minute, "preview.get": 2*time.Minute, "preview.delete": 10*time.Minute, "apps.render": 60*time.Second, "apps.put": 2*time.Minute }
func (c *apiClient) do(ctx context.Context, op, method, path, contentType string, body io.Reader, maxBytes int64) (*apiResponse, error) // network error → cliError ExitPlatform "transport"; redirect → ExitPlatform "redirect_refused"
// redact masks credentials before any text reaches an envelope or human error: `dt_<uuid>.<43 chars>` → "dt_***",
// `(?i)bearer\s+\S+` → "Bearer ***". Applied to Message, Hint and every string in Details. (CR-10)
func redact(s string) string
// output.go
type globals struct { Remote, Project, TokenFile, Context string; JSON, Quiet bool }
type result struct { Doc any; Raw []byte; Exit int }                // Exit 0 or 1 only
func emit(stdout io.Writer, g *globals, r result, human func(io.Writer)) int   // --json: Doc or Raw to stdout, newline; human: calls human(); returns r.Exit
func fail(stderr io.Writer, g *globals, err error) int               // envelope or "Error: msg\nhint"; non-*cliError → ExitPlatform "internal"
// root.go
func newRootCmd(g *globals, in io.Reader, out, errw io.Writer) *cobra.Command // SilenceUsage/SilenceErrors; persistent flags --remote --project --token-file --context --json --quiet; env fallbacks in PersistentPreRunE
func execute(args []string, in io.Reader, out, errw io.Writer) int
```
- Server: `withRequestID(next http.Handler) http.Handler` generates `uuid.NewString()` per request (ignores inbound `X-Request-Id`), stores it in ctx (`requestIDFromCtx`), sets the response header; `writeError` includes it. Every command body returns `(result, error)`; the cobra `RunE` adapter routes `error` → `fail`, `result` → `emit`. A command that ends in exit 20 returns an `error`, so stdout stays empty *(CR-8)*.

- [ ] **Step 1: Tests** — `TestFromHTTPMapping` (401/403/404/409/429/500/400 incl. body `{"error":"x"}` → message `x`); `TestFromHTTPRejectsNonUUIDRequestID`; `TestEnvelopeRedactsReflectedCredential` (server body `{"error":"token dt_<uuid>.<secret> rejected"}` → stderr contains `dt_***` and not the secret; same for `Bearer abc`) (CR-10); `TestEnvelopeStreams` (exit-1-with-doc → stdout only; exit-1-without → stderr only; exit 20 → stderr only, stdout empty); `TestNoTokenIsAuthExit3` (Review Focus 1, with `pipeline list` ported here as the first leaf); `TestClientRefusesRedirect` (httptest 307 to a second server → second server records zero requests, error `redirect_refused`) (CR2-3); `TestClientOpTimeoutOutlivesDefault` (op `query` against a handler sleeping 35 s succeeds; op `default` against the same handler → `transport` within ~30 s; run with a shortened table via a test hook) (CR2-2); `TestRequestIDMiddlewareIgnoresInbound` (server); `TestWriteErrorIncludesRequestID`.
- [ ] **Step 2–4:** fail → implement → PASS. Remaining commands stay reachable via a temporary `legacyCmd` forwarding to the old dispatch (deleted in 2.6).
- [ ] **Step 5: Commit** — `feat(cli): cobra root, exit-code table, JSON envelope, shared HTTP client; api: request-id middleware`

### Task 2.2: Port `components` and `pipeline` *(CR-9)*

**Files:** create `cmd/datuplet/{cmd_components.go,cmd_pipeline.go}`; modify `components.go`, `pipeline.go` (drop parsers/help; use `apiClient`), their tests.

**Interfaces:**
- `components list [--json]` → server array verbatim; `components get <name> [--version v] [--json|--schema]`.
- `pipeline list|get <name>|put [<name>] -f <file>|delete <name> [-y]|validate -f <file> [--name n]`. JSON success docs: `put` → `{"name","project","action":"created|updated"}` (`created` when the server returns 201, else `updated`; the server returns 204 today — change `handlePutPipeline` to 201/200 in this task), `delete` → `{"name","deleted":true}`, `validate` → server findings body. `put` on 400 whose body decodes as `{"findings":[…]}` → result doc on stdout exit 1 (`code` n/a); other 400 → envelope `request_rejected` exit 1. `delete` 409 → exit 9. `validate` transport → exit 20.

- [ ] **Step 1: Tests** — `TestPipelinePutCreatedVsUpdatedJSON`, `TestPipelinePut400FindingsOnStdoutExit1`, `TestPipelinePut400GenericEnvelopeExit1`, `TestPipelineDeleteConflictExit9`, `TestPipelineGetNotFoundExit4`, `TestComponentsGetSchemaVerbatim`, `TestValidateTransportExit20`; server test `TestPutPipelineStatusCodes`.
- [ ] **Step 2–5:** fail → implement → PASS → `feat(cli): components and pipeline on cobra; put returns created/updated`

### Task 2.3: Port `runs`; `trigger` → `runs start`; `runs cancel` *(CR-8)*

**Files:** create `cmd_runs.go`; rename `trigger.go` → `runs_start.go` (keep helpers); modify `runs.go`, tests; e2e `scenarios_agent_loop_test.go:269`; every doc/skill occurrence of `datuplet trigger`.

**Interfaces:**
- `runs start <pipeline> [--wait] [--timeout 1h] [--json]`: no `--wait` → `{"id","phase","pipeline_name","created_at"}` exit 0. `--wait`: `Succeeded` → run doc exit 0; `FailedUser|Cancelled|Expired` → run doc exit 1; `FailedApplication` → **envelope** `code:"run_failed_application"`, `details:{"run_id","phase","message"}`, exit 20, stdout empty; `--timeout` → cluster-side cancel, then envelope `run_timeout` `details:{run_id}` exit 1 (no result doc: the run is Cancelling, not terminal); poll 401/403 → exit 3 immediately.
- `runs list` / `runs get` → server body; `runs cancel <id>` → `{"id","cancel_requested":true}`; 404 → exit 4.

- [ ] **Step 1: Tests** — `TestRunsStartWaitFailedApplicationEnvelopeExit20` (stdout empty), `TestRunsStartWaitFailedUserDocExit1`, `TestRunsStartWaitPollUnauthorizedExit3` (one GET after 401, no more), `TestRunsStartTimeoutCancelsExit1`, `TestRunsCancelNotFoundExit4`.
- [ ] **Step 2–5:** fail → implement → PASS; `grep -rn 'datuplet trigger'` outside `docs/tmp docs/superpowers` = 0 → `feat(cli): runs start/cancel on cobra; trigger renamed`

### Task 2.4: Port `preview`, `storage`, `query` *(CR-8, CR-9)*

**Files:** create `cmd_preview.go`, `cmd_storage.go`, `cmd_query.go`; modify `preview.go`, `storage.go`, `query.go`, tests.

**Interfaces:**
- `preview run|get|list|delete|cancel` as today's flags. `run`: `Succeeded` → result doc 0; `FailedUser|Cancelled|Expired|timeout` → result doc 1; `FailedApplication`/5xx → envelope 20; 409 → 9; `--no-wait` → `{"id"}`. `delete` → `{"deleted":n}`; `cancel` → `{"id","cancel_requested":true}`.
- `storage tables|info|schema|sample|history` → server JSON verbatim; `storage delete <ns.table> --confirm <ns.table>` → `{"table","deleted":true}`, missing/mismatched `--confirm` → exit 2 `confirm_required`; `storage compact` → run doc with the same `--wait` mapping as `runs start`.
- `query`: `--format table|csv|json`; `--json` ≡ `--format json`; server 400 (bad SQL) → envelope `request_rejected` exit 1 (no doc).

- [ ] **Step 1: Tests** — `TestPreviewRunFailedApplicationEnvelopeExit20`, `TestStorageDeleteConfirmMismatchExit2`, `TestQueryBadSQLEnvelopeExit1`, `TestQueryJSONFlagEqualsFormatJSON`.
- [ ] **Step 2–5:** → `feat(cli): preview, storage, query on cobra`

### Task 2.5: Port `apps`

**Files:** create `cmd_apps.go`; modify `apps.go`, `apps_test.go`; re-run `tests/e2e/scenarios_agent_loop_apps_test.go` unchanged.

**Interfaces:** same tree and flags; `render` failure document → stdout exit 1; transport → envelope 20; `promote` 409 → envelope `promote_conflict` exit 9. JSON success documents for every leaf (CR-9): `init` → `{"dir","files":[…]}`; `put` → existing `appPutResponseJSON`; `get`/`list`/`logs` → server body; `delete` → `{"name","deleted":true}`; `promote` → `{"name","production":"<hash>"}`; `token create` → `{"token","id"}` with no stderr prose; `token list` → server body; `token delete` → `{"token_id","revoked":true}`. Render uses op `apps.render`, put uses `apps.put` from the timeout table.

- [ ] **Step 1: Tests** — rewire `apps_test.go` through `execute`; add `TestAppsRenderFailureDocumentOnStdoutExit1`, `TestAppsTokenCreateJSONNoProse`, `TestAppsInitJSON`, `TestAppsTokenDeleteJSON`, `TestAppsDeleteJSON`.
- [ ] **Step 2–5:** → `feat(cli): apps on cobra`

### Task 2.6: `auth login` on cobra, `help --json`, generated `docs/cli.md`, drop legacy dispatch

**Files:** create `cmd_auth.go` (login only), `cmd_help.go`, `cmd/datuplet/docgen/main.go`, `docs/cli.md`; modify `login.go`, `root.go` (delete `legacyCmd`, `printUsage`), `Makefile` (`docs-cli`, `check-docs-cli`), `pr.yml`.

**Interfaces:** `auth login --remote URL [--email e --password-stdin]` (behaviour unchanged this phase; `--json` → `{"logged_in":true,"email","expires_at"}`); `help --json` → `{"version","exit_codes":[{"code","meaning","stream"}],"commands":[{"path","short","args","flags":[{"name","shorthand","type","default","usage","global"}],"json_success":"<one-line schema note>"}]}`; `docs/cli.md` from the same tree + exit-code table.

- [ ] **Step 1: Tests** — `TestHelpJSONListsEveryLeaf`, `TestHelpJSONExitCodesSeven`; `make check-docs-cli` diff empty.
- [ ] **Step 2–5:** → `feat(cli): help --json, generated docs/cli.md; legacy dispatch removed`; `make lint`; push; draft PR.

---

# Phase 3 — Auth (branch `feat/cli-p3-auth`)

### Task 3.1: `api_tokens` table and store

**Files:** create `pkg/pipelineapi/db/migrations/016_api_tokens.sql`, `pkg/pipelineapi/store/{apitoken.go,apitoken_test.go}`; modify `db/schema_check.go` if it enumerates tables.

**Interfaces:**
```sql
CREATE TABLE api_tokens (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  name TEXT NOT NULL CHECK (length(name) BETWEEN 1 AND 64),
  salt BYTEA NOT NULL, secret_hash BYTEA NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(), expires_at TIMESTAMPTZ NOT NULL,
  last_used_at TIMESTAMPTZ, revoked_at TIMESTAMPTZ);
CREATE INDEX idx_api_tokens_user ON api_tokens (user_id);
```
```go
type APIToken struct { ID, UserID uuid.UUID; Name string; CreatedAt, ExpiresAt time.Time; LastUsedAt, RevokedAt *time.Time }
const ( APITokenPrefix="dt_"; DefaultAPITokenTTL=90*24*time.Hour; MaxAPITokenTTL=365*24*time.Hour )
func CreateAPIToken(ctx, pool, userID uuid.UUID, name string, ttl time.Duration) (plaintext string, tok *APIToken, err error)
func ParseAPIToken(plaintext string) (id uuid.UUID, secret string, ok bool)
func LookupAPIToken(ctx, pool, id uuid.UUID, secret string) (*APIToken, error) // ErrAPITokenInvalid on no row/mismatch/revoked/expired; ConstantTimeCompare; last_used_at touched ≤ once per minute
func ListAPITokensForUser(ctx, pool, userID uuid.UUID) ([]APIToken, error)
func RevokeAPIToken(ctx, pool, userID, id uuid.UUID) error // ErrAPITokenNotFound unless owned
```
- [ ] **Step 1: Tests** — `TestAPITokenRoundTrip`, `WrongSecret`, `Revoked`, `Expired`, `TTLClamp`.
- [ ] **Step 2–5:** `TEST_DATABASE_URL=… go test -p 1 ./pkg/pipelineapi/store -run APIToken` → `feat(api): api_tokens table and store`

### Task 3.2: `Principal` resolver contract, `dt_` resolver, token endpoints; delete the CLI JWTs *(CR-2)*

**Files:**
- Create: `pkg/pipelineapi/auth/{apitoken_resolver.go,apitoken_resolver_test.go,chain_resolver.go}`, `pkg/pipelineapi/http/{api_token_handlers.go,api_token_handlers_test.go}`
- Modify: `pkg/pipelineapi/auth/resolver.go` (`UserResolver.UserFor` → returns `*Principal`), `auth/middleware.go` (binds the principal; `UserFromContext` keeps working, add `PrincipalFromContext`), `auth/resolver.go` `PostgresResolver` (returns `Principal{User:u}`), `http/cli_token_handler.go` (password grant creates an `api_tokens` row), `http/server.go` (routes; drop `WithCLIClusterInfo`), `http/auth_handlers.go` (`handleMe` adds `token`), `cmd/pipeline-api/main.go` (chain `APITokenResolver`), `tokens/mint.go` (drop `cli-api`, `local-cli` kinds), `tests/e2e/framework/agent_loop.go` (`MintAdminCLIToken` → `POST /api/v1/auth/token`), `docs/auth-flow.md`
- Delete: `auth/bearer_resolver.go` (ChainResolver moves), `tokens/cli_api.go`, `tokens/cli.go`, their tests, `DATUPLET_CLI_TOKEN_LIFETIME`

**Interfaces:**
```go
type Principal struct { User *store.User; TokenID *uuid.UUID; TokenName string; TokenExpiresAt *time.Time }
type UserResolver interface { UserFor(w http.ResponseWriter, r *http.Request) (*Principal, bool, error); Mode() string; SupportsLogin() bool }
func PrincipalFromContext(ctx) (*Principal, bool)
type APITokenResolver struct { Pool *pgxpool.Pool }  // Authorization: Bearer dt_… only; others fall through
```
- Routes (under `WithUser`): `GET /api/v1/auth/tokens` → `[{"id","name","created_at","expires_at","last_used_at","revoked_at"}]`; `POST /api/v1/auth/tokens` `{"name","ttl":"720h"}` → 201 `{"token","id","name","expires_at"}`; `DELETE /api/v1/auth/tokens/{id}` → 204/404.
- `POST /api/v1/auth/token` body `{"email","password","client_name"}` → 200 `{"token","token_id","expires_at","user_id","projects":[{"id","name","lakekeeper_project_id"}]}`.
- `GET /api/v1/auth/me` → adds `"token":{"id","name","expires_at"}|null`.

- [ ] **Step 1: Tests** — resolver: `Valid`, `Revoked`, `DisabledUser`, `IgnoresNonDtBearer`; **through the real middleware**: `TestMiddlewareCarriesPrincipalTokenID` (ChainResolver{APIToken, Postgres} → handler sees `PrincipalFromContext().TokenID`); handlers: `CreateListRevoke`, `RevokeOthersTokenIs404`, `PasswordGrantReturnsDtToken`, `MeShowsTokenMetadata`; `grep -rn 'cli-api\|local-cli\|MintCLIAPIToken\|MintLocalCLIToken\|DATUPLET_CLI_TOKEN_LIFETIME\|WithCLIClusterInfo' pkg cmd tests docs charts` = 0.
- [ ] **Step 2–5:** → `feat(api): Principal resolver contract; dt_ API tokens replace the cli-api JWT`

### Task 3.3: Config contexts, binding rule, `auth` and `projects` commands *(CR-1, CR-11, CR-13)*

**Files:** create `cmd/datuplet/{config.go,config_test.go,session.go,session_test.go,cmd_projects.go}`; modify `cmd_auth.go`, `login.go`, every `cmd_*.go` (`loadRemoteArgs` → `resolveSession`), skill, `docs/agent-quickstart.md`, `README.md`; delete `remote.go`, `remote_test.go`, `cluster_meta.go`, old `login_test.go`.

**Interfaces:**
```go
type Context struct { Remote, Token, ExpiresAt, UserID string; Projects []Project }
type Project struct { ID, Name, LakekeeperProjectID string }
type Config struct { Current string; Contexts map[string]Context }
func configPath() string; func loadConfig() (*Config, error); func (c *Config) save() error // 0700/0600, atomic
type session struct { Remote, Token, ProjectID, ProjectName, ContextName string; client *apiClient }
func resolveSession(ctx context.Context, g *globals, needProject bool, stderr io.Writer) (*session, error)
```
Resolution order (spec §2c binding rule):
1. `tokenSrc`: `--token-file` > `$DATUPLET_API_TOKEN` > none.
2. If `tokenSrc` set: remote must be `--remote`/`$DATUPLET_REMOTE`, else `ExitUsage "remote_required"`; saved context ignored entirely; project value: UUID → as-is, name → `GET /api/v1/projects` with that token (`project_not_found` lists names in `details.available`).
3. Else: context = `--context`/`$DATUPLET_CONTEXT`/`Current`; none → `ExitAuth "auth_missing"` hint `datuplet auth login --remote <url>`; explicit remote ≠ context remote → `ExitUsage "context_remote_mismatch"` with no request (Review Focus 6); expired → `ExitAuth "auth_expired"`; project from the saved list (UUID or name), several + none chosen → `ExitUsage "project_required"` with `details.available`.
4. Preflight: unless `--quiet`, `GET /api/v1/version` (2 s); on mismatch and not `--json`, one stderr warning; errors ignored *(CR-13)*.

Commands: `auth login --remote URL [--email e --password-stdin] [--context name]` (context name default = host; stores `dt_`; `--json` → `{"context","remote","email","expires_at"}`); `auth logout [--context n]` (DELETE token, drop context → `{"context","logged_out":true}`); `auth status [--json]` → `{"context","remote","user":{"id","email"},"token":{"id","name","expires_at"},"projects":[…]}`; `auth token create --name n [--ttl 2160h]` (human: token on stdout + stderr note; `--json`: `{"token","id","name","expires_at"}` only); `auth token list`; `auth token revoke <id>` → `{"id","revoked":true}`; `projects list` → server array.

- [ ] **Step 1: Tests** — `config_test`: `RoundTripPerms`, `EnvOverride`. `session_test`: `ExplicitTokenRequiresRemote`, `ExplicitTokenIgnoresExpiredContext` (CR-11), `ExplicitTokenResolvesNameViaAPI`, `ContextRemoteMismatchNoRequest` (httptest counts 0 hits), `ProjectNotFoundListsNames` (exit 4), `NoTokenExit3`, `PreflightWarnsHumanSilentJSON`. `cmd_auth`: `LoginStoresDtTokenNeverPrints`, `TokenCreateJSONOnlyDoc`, `StatusJSON`, `ProjectsListJSON`.
- [ ] **Step 2–5:** PASS; `grep -rn 'cluster.json\|api-token\|~/.datuplet/token'` outside `docs/tmp docs/superpowers` = 0 → `feat(cli): config contexts with credential binding; auth and projects commands`

### Task 3.4: Project-aware query-token mint; `datuplet-query` on the new config *(CR-3)*

**Files:**
- Modify: `pkg/pipelineapi/http/query_token_handler.go` (+ test), `cmd/pipeline-api/main.go` (pass `cfg.LakekeeperPublicURL`, the project gate and `warehouseResolver` to `NewLocalQueryMintHandler`), `pkg/pipelineapi/http/server.go` (`WithLocalQueryMint` unchanged signature), `components/queryengine/cmd/datuplet-query/{config.go,mint.go,main.go}` + tests, `docs/ad-hoc-query.md`, `tests/e2e/scenarios_local_query_test.go`

**Interfaces:**
- `NewLocalQueryMintHandler(signer *tokens.Signer, allow bool, lakekeeperURL string, gate *projectgate.Gate, warehouseFor func(ctx, lakekeeperProjectID string) (string, error)) http.Handler`; request `{"project":"<datuplet project uuid>"}` (required); gate check `describe_effective`; response `{"token","expires_at","lakekeeper_url","lakekeeper_project_id","warehouse"}` (`warehouse` = `<lkpid>/<name>` from `warehouseFor`); 400 missing project, 403 gate, 503 resolver.
- `datuplet-query`: `loadConfig(dir)` → `config{Remote, Token, ProjectID}` via the same binding rule as Task 3.3 (shared logic duplicated in this module, documented as such); `mint(ctx, cfg) (mintResult{Token, LakekeeperURL, LakekeeperProjectID, Warehouse}, error)`; `--project`/`$DATUPLET_PROJECT` honoured; single-project default via `GET /api/v1/projects`.

- [ ] **Step 1: Tests** — server: `MintRequiresProject400`, `MintGateForbidden403`, `MintReturnsCoordinates`; client: `LoadConfigBindingRule`, `MintResultDecode`.
- [ ] **Step 2–5:** `make tidy`; → `feat(query): project-aware query-token mint; datuplet-query reads config.json`; `make lint`; push; draft PR (e2e must be green).

---

# Phase 4 — Surface and self-description (branch `feat/cli-p4-surface`)

### Task 4.1: `secrets` commands

**Files:** create `cmd_secrets.go`, `secrets_test.go`; modify `docs/secrets.md`, skill guardrails.

**Interfaces:** `secrets list` → server array; `secrets set <key> [--value-stdin|--from-file p]` (non-TTY stdin default; TTY → hidden prompt) → `{"key","action":"set"}`; `secrets delete <key> [-y]` → `{"key","deleted":true}`; key regex copied from `secrets_handlers.go`, invalid → exit 2 `invalid_key`; value never written to stdout/stderr.

- [ ] Tests: `SetFromStdinNeverEchoes`, `ListJSON`, `SetInvalidKeyExit2`. → `feat(cli): secrets list/set/delete`

### Task 4.2: Embedded agent guide and `skill install` *(CR-9)*

**Files:** create `cmd/datuplet/agentguide/` (synced copy, `//go:embed`), `cmd_agentguide.go`, `agentguide_test.go`; modify `Makefile` (`sync-agent-guide`, `check-agent-guide`), `pr.yml`, `README.md`, `docs/agent-quickstart.md`.

**Interfaces:** `agent-guide [--section skill|components|pipeline-doc-format|scenarios] [--list]`: human prints markdown; `--json` → `{"section","content"}` or with `--list` `{"sections":[{"name","title"}]}`. `skill install [--claude|--codex|--cursor] [--dir p] [--force]` → human one line; `--json` → `{"installed_to","files":[…]}`; refuses overwrite without `--force` (exit 9 `exists`).

- [ ] Tests: `SectionsMatchEmbed`, `AgentGuideJSONShape`, `SkillInstallWritesTreeAndRefusesOverwrite`; `make check-agent-guide` clean. → `feat(cli): embedded agent guide and skill install`; `make lint`; push; draft PR.

---

# Phase 5 — Run logs, live-pod only (branch `feat/cli-p5-run-logs`)

### Task 5.1: Availability-aware tailer; `GET /api/v1/projects/{pid}/runs/{id}/logs` *(CR-4, CR-5, CR-6, CR-7)*

**Files:**
- Modify: `pkg/pipelineapi/preview/logs.go` (+ test): `LogTailer.Tail` → `TailResult`; `pkg/k8s/controllers/pipelinerun_jobs.go` (export `ComponentJobName(pr, compName)`; keep the unexported wrapper); `pkg/pipelineapi/http/server.go` (`WithRunLogs(d RunLogsDeps)`), `cmd/pipeline-api/main.go`, `charts/datuplet-app/templates/pipeline-api/rbac.yaml` (add `resources: ["pods/log"], verbs: ["get"]` next to the pods rule) (CR2-1), `docs/pipeline-api.md`
- Create: `pkg/pipelineapi/http/{run_logs_handler.go,run_logs_handler_test.go}`, `tests/e2e/scenarios_run_logs_test.go`

**Interfaces:**
```go
// preview
type TailResult struct { Log string; Available bool; Truncated bool }
type LogTailer interface { Tail(ctx, namespace, jobName, container string, lines int64, limitBytes int64) (TailResult, error) } // Available=false when the Job has no pods / pod or container not found; uses PodLogOptions{TailLines, LimitBytes}
// http
type RunLogsDeps struct { Runs RunReader; Projects ProjectReader; K8s client.Client; Tailer preview.LogTailer }
```
- Handler: `mustHaveRelation(w, r, "describe_effective")` (same as `runs get`); load the run (phase, `project_id`); the namespace comes from the **project** record's `K8sNamespace` via `ProjectReader` — the same source `runbackend/k8s.go` uses for cancel; `store.RunView` has no namespace (CR2-4); `Get PipelineRun run-<id>` in that namespace; missing CR → 404 `logs_expired`; `status.resolvedSpec == nil` → 409 `logs_not_ready`; enumerate `status.resolvedSpec.stages[i].components[j]` → `controllers.ComponentJobName`; filters `stage`, `component` (unknown → 404 `not_found`); `tail` default 200 max 5000; `limit_bytes` default 65536 max 524288 per container, aggregate cap 1 MiB (later entries marked `"truncated":true,"log":""` once exceeded); containers `component`, `gateway`. Response 200 `{"run_id","phase","entries":[{"stage","component","container","job","log","available","truncated"}]}`. When the run phase is terminal and **no** entry is `available` → 404 `{"error":"run logs expired","code":"logs_expired"}`.

- [ ] **Step 1: Tests** — tailer: `TailDistinguishesNoPodsFromEmptyLog`, `TailLimitBytesSetsTruncated`; handler (fake clientset + fake CR getter): `HappyPathFilters`, `NamespaceFromProjectNotAPI` (project ns `proj-a` ≠ API ns; the CR lookup and pod list hit `proj-a`) (CR2-4), `MissingCR404Expired`, `JobsDeletedCRPresentTerminal404Expired` (CR-6), `RunningRunJobsGone200Unavailable`, `UnresolvedSpec409`, `AggregateByteCapTruncates`, `ViewerWithDescribeEffectiveAllowed` / `NoRelation403`, `TailAndLimitClamp`. e2e (`E2E_K8S=1`): `TestRunLogs_DeployedRBAC` triggers the simple pipeline, waits for terminal, calls the endpoint through the deployed pipeline-api and asserts both `component` and `gateway` entries are `available` with non-empty `log` (CR2-1; a Forbidden from `pods/log` fails here, not in unit tests).
- [ ] **Step 2–5:** → `feat(api): live-pod run logs endpoint with availability and byte bounds; rbac pods/log`

### Task 5.2: `runs logs`

**Files:** modify `cmd_runs.go`, `runs_test.go`, skill ("observe and verify"), `docs/agent-quickstart.md`; regenerate `docs/cli.md`.

**Interfaces:** `runs logs <id> [--stage s] [--component c] [--tail n] [--limit-bytes n] [--json]`; human: `=== <stage>/<component> (<container>) ===` blocks, `(no longer available)` / `(truncated)` markers; `--json` → server body; 404 `logs_expired` → exit 4 hint `logs are retained for 1 h after a run completes`; 409 `logs_not_ready` → exit 9 hint `run has not started; retry shortly`.

- [ ] Tests: `LogsExpiredExit4Hint`, `LogsNotReadyExit9`, `FiltersForwarded`, `JSONPassthrough`. → `make docs-cli` → `feat(cli): runs logs`; `make lint`; push; draft PR.

---

## Codex round 3 (2026-11-08) — final

All round-2 items confirmed resolved; no new findings. Verdict: **approve**.
The review cap is reached; execution may start on the maintainer's go.

## Codex round 2 (2026-11-08) — folded in

CR-9 completed (every `apps` leaf has a JSON success document + tests, Task 2.5); CR-10 completed (`redact` in the envelope path + `TestEnvelopeRedactsReflectedCredential`, Task 2.1); CR2-1 RBAC `pods/log get` + e2e `TestRunLogs_DeployedRBAC` (Task 5.1); CR2-2 no global client timeout, per-op deadline table + tests (Task 2.1); CR2-3 redirect refusal in the shared client + test (Task 2.1); CR2-4 namespace from the project record via `ProjectReader` + `NamespaceFromProjectNotAPI` (Task 5.1).

## Self-review notes (author, after Codex round 1)

- Spec coverage unchanged (§1a–1d → 1.x; §2a → 3.1–3.2; §2b–2c → 3.3–3.4; §3 → 2.x; §4 → 2.3, 3.3, 4.1, 5.x; §5 → 2.6, 4.2; §6 → 1.4, 3.3).
- CR-1/CR-11 → Task 3.3 binding rule + Review Focus 6; CR-2 → Task 3.2 `Principal`; CR-3 → Task 3.4 rework; CR-4/5/6/7 → Task 5.1; CR-8 → Tasks 2.1/2.3/2.4 (`result`/`error` split, exit 20 envelope-only); CR-9 → per-leaf JSON docs in 2.2–2.6, 3.3, 4.1, 4.2; CR-10 → Task 2.1 middleware + `apiResponse.RequestID`; CR-12 → Task 1.3 smoke workflow; CR-13 → Task 1.2 pieces + Task 3.3 preflight.
- Names reused across tasks: `globals`, `cliError`, `fromHTTP`, `apiClient`, `apiResponse`, `result`, `emit`, `fail`, `execute`, `resolveSession`, `session`, `Config`/`Context`, `Principal`, `PrincipalFromContext`, `APITokenResolver`, `TailResult`, `ComponentJobName`, `RunLogsDeps`.
