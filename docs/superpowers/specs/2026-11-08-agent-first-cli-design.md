# Agent-first `datuplet` CLI — audit and design

Status: APPROVED by the maintainer (2026-11-08); all decisions below are
closed. Codex design review: three rounds (13 + 4 Important, all folded
in), final verdict **approve** with no new findings. Implementation plan:
`docs/superpowers/plans/2026-11-08-agent-first-cli.md`.

## Intent

The `datuplet` CLI is the primary surface through which coding agents
(Claude Code, Codex, Cursor, scripts) operate the platform. Today it was
written human-first with agent affordances bolted on (RFC 027 §7 env vars,
`--json` on some commands, exit-code bands on three of them). This design
makes the agent the first-class user: a fresh machine with no Go toolchain
and no repo checkout must be able to install the CLI with one command,
authenticate without a human password, discover the whole contract from the
binary itself, and script every operation on a uniform JSON + exit-code
contract.

## Audit — weak spots found (verified against `cmd/datuplet/*.go` and
`pkg/pipelineapi/http/*.go` at `eed7146a`)

### A. Installation / distribution

| # | Finding | Evidence |
|---|---------|----------|
| A1 | **No published CLI binary.** The only install path is `make build` from a checkout (needs Go). `release.yml` publishes images + charts only. | `release.yml` jobs: verify-tag, services, components, charts. No binary job. |
| A2 | **Fat binary (113 MB, 1069 packages).** `datuplet gateway` (the sidecar entrypoint, arrow-go + iceberg-go + gRPC) is compiled into the user CLI. Slow build, large download, big attack surface for a thin HTTP client. | `go build ./cmd/datuplet` → 113,708,626 bytes; `utils/docker/Dockerfile.gateway` uses `ENTRYPOINT ["datuplet","gateway"]`. |
| A3 | **`version` is a hard-coded string** (`0.1.0-poc`); no commit, no build date, no server-version check. | `main.go` `case "version"`. |
| A4 | **GitHub Releases API constraint.** `release.yml` notes that `GITHUB_TOKEN` 403s on `POST /releases` for this personal-account repo, which is why charts go to gh-pages. A binary channel must work around this. | comment in `release.yml` chart-releaser step. |

### B. Authentication

| # | Finding | Evidence |
|---|---------|----------|
| B1 | **Password grant only.** `POST /api/v1/auth/token` takes email+password and mints two 24 h JWTs. There are no revocable long-lived tokens, no refresh, no service accounts. An agent either holds a human's password or re-logs-in daily. | `cli_token_handler.go`, `defaultCLITokenLifetime = 24h`, max 30 d via env. |
| B2 | **Headless path needs the project UUID; the on-disk path needs the project *name*.** Same flag, different semantics. Agents cannot discover the UUID from the CLI (no `projects` command) although `GET /api/v1/projects` exists. | `remote.go` fast path: `ID: effectiveProject`; `server.go:419`. |
| B3 | **No way to get `DATUPLET_API_TOKEN` from the CLI.** `login` writes `~/.datuplet/api-token` but never surfaces it; the agent quickstart tells users to get it "from whoever provisioned your project". | `login.go` success banner; `docs/agent-quickstart.md` prerequisites. |
| B4 | **Legacy lakekeeper token still written and still the `--token-file` default.** `~/.datuplet/token` (local-cli, `aud=datuplet-catalog`) was for the deleted `run --remote`; `--token-file` reads it but feeds it as the *api* token (wrong audience → 401). | `remote.go` `case tokenFileFlag != "": rawAPIToken = rawToken`. |
| B5 | **Single remote.** `cluster.json` holds one URL; `--remote` that differs from it errors. No profiles / contexts. | `validateRemoteURL`. |
| B6 | **Empty-remote error text.** With an expired token and no `--remote`, the hint reads `run `datuplet login --remote ` first`. | reproduced live. |
| B7 | **No `auth status` / `whoami`.** `GET /api/v1/auth/me` exists; the CLI has no command for it, so an agent cannot check "am I logged in, as whom, until when, on which projects" without parsing `cluster.json`. | `server.go:412`. |

### C. Output, errors, exit codes

| # | Finding | Evidence |
|---|---------|----------|
| C1 | **Three flag-parsing styles.** Go `flag` (login/trigger/storage/query: single-dash, flags must precede positionals), hand-rolled (pipeline/runs/components/apps: flags anywhere), preview mixes both. `datuplet trigger my-pipe --wait` silently ignores `--wait` (flag pkg stops at the first positional). | `main.go` + each `parse*Flags`. |
| C2 | **`--json` is not universal.** Missing on `storage tables/info/schema/sample/history/delete` (always pretty JSON, fine) but also on `login`, `version`, `pipeline put/delete`, `query` (uses `--format`). Errors are always free-text `Error: …` on stderr — no JSON error envelope except `apps render`. No `request_id` surfaced. | grep `asJSON` per file. |
| C3 | **Exit-code bands differ per command.** validate 0/1/≥2; preview 0/1/20; apps render 0/1/20; apps promote 9; trigger 0/1 for *every* failure (FailedUser, FailedApplication, 401, network all → 1). No single documented contract. | `pipeline.go`, `preview.go`, `apps.go`, `trigger.go` `exitCodeForPhase`. |
| C4 | **Help output is inconsistent.** `pipeline --help` prints a curated text; `trigger --help` prints Go flag dump; `--help` on `storage` is partially in the top-level usage. No `help --json`. | reproduced live. |
| C5 | **Unknown subcommand prints full usage to stdout** and exits 1; usage goes to stdout, errors to stderr — fine for humans, noisy for agents. | `main.go` default case. |

### D. Missing surface (the API exists, the CLI does not)

| # | Gap | API |
|---|-----|-----|
| D1 | `secrets list/set/delete` — needed for every real pipeline (`$[name]` refs); docs use `curl`. | `/api/v1/projects/{pid}/secrets[/{key}]` |
| D2 | `projects list`, `whoami` | `/api/v1/projects`, `/api/v1/auth/me` |
| D3 | `runs cancel <id>` | `POST …/runs/{id}/cancel` (used internally by `trigger --timeout`) |
| D4 | `runs logs <id>` — a failed *real* run exposes only `message`; `log_tail` exists for previews only. Debugging a production run needs `kubectl`. | **no endpoint**; pipeline-observer already holds a `kubernetes.Interface`. |
| D5 | `pipeline put` on 400 prints the raw server body; it should surface the same `findings[]` shape `validate` does. | `handlePutPipeline` |

### E. Discoverability for agents

| # | Finding |
|---|---------|
| E1 | The `datuplet-operator` skill (the real agent manual) lives only in the repo's `.claude/skills/`. An agent that installs the binary on a fresh machine never sees it. |
| E2 | No machine-readable self-description: no `help --json`, no JSON-schema for outputs, no exit-code table queryable from the binary. |
| E3 | No shell completion (minor; agents rarely need it). |

### What is already good (keep)

Env-var headless auth; `pipeline validate` findings + exit codes; `preview`
result document with `log_tail`; `trigger --wait --timeout` with cluster-side
cancel; `storage delete --confirm <ns>.<table>`; `apps render` error
envelope + 1/20 split; `apps promote` CAS → exit 9; secrets never printed;
bounded response reads; `docs/agent-quickstart.md`.

## Green-field premise

Datuplet has no deployment anywhere (maintainer, 2026-11-08). There is no
installed base, no stored `~/.datuplet` state to honour, no external script
that depends on today's flags or exit codes. Every choice below is made as
if the CLI were being designed from scratch; where the current code happens
to match, it is kept because it is right, not because it exists. Internal
consumers of the CLI — the e2e suite (`tests/e2e/`), the operator skill,
`docs/agent-quickstart.md`, README — are updated in the same PR as the
change that affects them, never kept on a compatibility shim.

## Goals

1. One-command install on macOS/Linux (amd64 + arm64) with no toolchain.
2. Agent authentication without a human password and without daily re-login.
3. One contract: every command takes `--json`, every error is a JSON
   envelope under `--json`, exit codes come from one table.
4. The binary is self-describing: it can print the operator guide and a
   machine-readable command/exit-code catalog.
5. Close the surface gaps agents hit first: secrets, projects, run logs,
   cancel.

## Non-goals

- Rewriting the browser UI or the REST API shapes (CLI mirrors them).
- An MCP server (could sit on top later; out of scope).
- Windows builds (nice-to-have, not in this pass).
- Upgrade/migration/compatibility paths of any kind — new tables are part
  of the fresh install's schema, old config files are simply not read, old
  command spellings are simply gone.

## Anti-goals

- Printing any bearer token to stdout by default.
- Keeping two ways to do one thing (two token files, two parsers, two
  project-identifier semantics, per-command exit-code bands) for the sake of
  continuity.

## Design

### 1. Distribution

**1a. Split the binary.** Move `gatewayMain` to `cmd/gateway/` and point
`Dockerfile.gateway` at it. `cmd/datuplet` becomes a pure `net/http` client:
`CGO_ENABLED=0`, no arrow/iceberg/gRPC, ~10–15 MB, cross-compiles trivially.
The `datupleticeio` blank import leaves with the gateway.

**1b. Build + publish on every `v*.*.*` tag.** New job `cli` in
`release.yml` (after `verify-tag`): matrix `{darwin,linux} × {amd64,arm64}`,
`-ldflags "-s -w -X main.version=$TAG -X main.commit=$SHA -X main.date=…"`,
tarball `datuplet_<version>_<os>_<arch>.tar.gz` + `checksums.txt`.

Publishing channel — **decided (maintainer, 2026-11-08): GitHub Release
assets** via `softprops/action-gh-release`, one release per `v*.*.*` tag
carrying the four tarballs + `checksums.txt`. Standard, discoverable,
`gh release download` works. Known risk: `release.yml` records that
`GITHUB_TOKEN` 403'd on `POST /releases` for this personal-account repo
when chart-releaser tried it. A throwaway `v*` tag cannot prove the channel
because `release.yml`'s `verify-tag` gate rejects any tag that does not
match the committed chart versions. The proof is therefore a separate
maintainer-triggered `workflow_dispatch` workflow
(`cli-prerelease-smoke.yml`) that builds the tarballs from the dispatched
ref and publishes them as a **pre-release** under a tag outside the
`v*.*.*` pattern (`cli-smoke-<run_id>`), touching no image or chart. If it
403s, fall back to publishing the same tarballs to gh-pages
(`cli/<version>/…` + `cli/latest.txt`) through the existing chart-releaser
push, and the install script resolves `latest` from there instead of the
Releases API.

**1c. `scripts/install-cli.sh`** at a stable raw URL:

```bash
curl -fsSL https://raw.githubusercontent.com/kacurez/datuplet/main/scripts/install-cli.sh | sh
```

Detects OS/arch, resolves latest (or `DATUPLET_VERSION=vX.Y.Z`), verifies
the checksum, installs to `/usr/local/bin` or `~/.local/bin` (no sudo
required), prints the resolved path and `datuplet version`. Idempotent;
`--version`/`--to` flags. Agents get this one line in README and in the
operator skill.

**1d. `datuplet version --json`** → `{client:{version,commit,date},
server:{version,commit},minor_mismatch:bool}` (server part only when a
remote is resolvable; pipeline-api gains unauthenticated
`GET /api/v1/version`). There is no separate API level: client and server
share the repo's semver, and the only check is "same `MAJOR.MINOR`?". The
check lives in the shared session resolution every remote command runs
through: a best-effort `GET /api/v1/version` with a 2 s timeout, once per
invocation, skipped under `--quiet`; any failure of that probe is ignored.
On a mismatch, human mode prints one stderr line
`warning: client vX.Y, server vA.B (minor versions differ)`; under `--json`
nothing is printed (stderr is reserved for the error envelope) and the fact
is only reported by `version --json`'s `minor_mismatch`. Never a non-zero
exit. The server's version is injected the same way as the CLI's:
`_release-image.yml` passes `VERSION`/`SHA` as build-args and the service
Dockerfiles forward them to `-ldflags -X`. *(Builder-check call; preflight
owner and `--json` behaviour fixed after Codex review.)*

Optional later: Homebrew tap (`kacurez/homebrew-datuplet`) generated from
the same release job.

### 2. Authentication for agents

**2a. One CLI credential: server-side API tokens.** Table `api_tokens(id,
user_id, name, token_hash, created_at, expires_at, last_used_at,
revoked_at)` in the fresh-install schema. Plaintext `dt_<id>.<secret>`,
shown once, stored hashed (same pattern as app viewer tokens). Endpoints
`/api/v1/auth/tokens` (list/create/revoke). A new `APITokenResolver`
resolves `dt_…` by hash lookup (constant-time compare); the token acts as
its user, so FGA grants and user disablement still govern it. Default TTL
90 d, max 1 y, revocable. The resolver interface is widened so that
authentication metadata travels with the user: `UserFor` returns a
`Principal{User *store.User; TokenID *uuid.UUID}` and the `WithUser`
middleware binds the whole principal into the request context — the
current interface returns only the user and the middleware rebuilds the
context from the original request, so a token id set inside a resolver
would be lost. `/auth/me` reads the principal to report `token`.

Green field lets this *replace* the `cli-api` JWT rather than sit beside
it: `auth login` (password grant, humans) asks the server to create an
`api_tokens` row named `cli:<hostname>` and stores that `dt_…` in the
config; `auth token create` mints a second one for an agent. The CLI then
has exactly one credential kind. `tokens.MintCLIAPIToken`,
`MintLocalCLIToken`, the `local-cli`/`cli-api` token kinds and the
`DATUPLET_CLI_TOKEN_LIFETIME` knob are deleted. Run tokens, impersonation,
query and app JWTs are untouched.

CLI:

```
datuplet auth login --remote URL [--email E --password-stdin]
datuplet auth token create --name <n> [--ttl 90d] [--json]      # prints dt_… once
datuplet auth token list|revoke <id>
datuplet auth status [--json]   # remote, user, token name, expiry, projects
datuplet auth logout            # revokes the cli:<hostname> token, clears context
```

The command tree is designed fresh (see §3a); nothing is kept as an alias
for continuity.

**2b. Project resolution.** `--project`/`$DATUPLET_PROJECT` accepts a name
*or* a UUID on both paths. Headless path with a non-UUID value calls
`GET /api/v1/projects` once and caches nothing. Add `datuplet projects list
[--json]` (D2). Error B6 fixed by always carrying the resolved remote.

**2c. Config file.** One `~/.datuplet/config.json` holding named contexts
(`{current, contexts: {name: {remote, token, expires_at, user_id,
projects}}}`), mode 0600. `cluster.json`, `token` and `api-token` do not
exist. `--token-file` means "a file containing a `dt_…` token" and nothing
else. `DATUPLET_CONFIG` overrides the path so parallel agents can isolate
state. Precedence for every setting: flag > env > current context.

**Credential binding rule.** A saved context's token is bound to that
context's remote. Resolution picks the *credential source* first:
- explicit token (`--token-file` or `$DATUPLET_API_TOKEN`) → the remote
  must also be explicit (`--remote` or `$DATUPLET_REMOTE`); the saved
  context's expiry and project list are **not** consulted, and project
  names resolve through `GET /api/v1/projects` with that token;
- no explicit token → the token comes from the selected context, and the
  remote **must** equal that context's remote (an explicit `--remote` that
  differs is `usage` exit 2: "context X is bound to <remote>; pass a token
  for <other> or `auth login --remote <other> --context <name>`"). Only
  then are the saved expiry and project list used.
This keeps the protection today's `validateRemoteURL` gives (a saved token
never goes to a remote it was not issued for) and stops a stale context
from rejecting or misrouting a fresh env token. *(Added after Codex
review.)*

`datuplet-query` (`components/queryengine/cmd/datuplet-query/config.go`,
separate CGO module) today reads `cluster.json` + `api-token` and
qualifies the warehouse from `projects[0]`. It moves to `config.json`,
`dt_…` tokens and the same binding rule, and takes its project like every
other command (`--project` / `$DATUPLET_PROJECT` / single-project
default). The catalog coordinates it needs come from the server:
`POST /api/v1/query/token` gains a body `{"project":"<datuplet project id>"}`,
checks `describe_effective` on that project through the shared project
gate, resolves the warehouse with the existing per-project
`WarehouseResolver`, and returns
`{"token","expires_at","lakekeeper_url","lakekeeper_project_id","warehouse"}`
(`warehouse` already project-qualified). `WithCLIClusterInfo` is deleted;
`LakekeeperPublicURL` is passed to the mint handler instead. No legacy
files are written. *(Builder-check call, reworked after Codex review: the
old call site passed an empty warehouse on purpose.)*

### 3. Uniform contract

**3a. One parser — decided (maintainer, 2026-11-08): `spf13/cobra`.**
Industry default; agents know its `--help` shape; gives a walkable command
tree (for `help --json` and generated docs), completions, and
flags-anywhere for free. Cost: one new dependency and a mechanical rewrite
of `main.go` dispatch; subcommand bodies (`runTrigger`, `runPipelinePut`, …)
are untouched. The three hand-rolled `parse*Flags` functions are deleted.

Command tree (fresh; every leaf takes the global flags of §3b):

```
datuplet auth        login | logout | status | token {create,list,revoke}
datuplet projects    list
datuplet secrets     list | set | delete
datuplet components  list | get [--schema]
datuplet pipeline    list | get | put | delete | validate
datuplet preview     run | get | list | delete | cancel
datuplet runs        start <pipeline> [--wait] | list | get | cancel | logs
datuplet storage     tables | info | schema | sample | history | delete | compact
datuplet query
datuplet apps        init | put | get | list | delete | render | logs | promote | token
datuplet version | help [--json] | agent-guide | skill install
```

`trigger` becomes `runs start` so every verb lives under its noun; the
operator skill, docs and e2e are updated in the same PR.

**3b. Global flags** on every command: `--remote`, `--project`,
`--token-file`, `--context`, `--json`, `--quiet`. There is no global
`--timeout`: per-request HTTP ceilings stay internal constants, and
`--timeout` keeps its one meaning as the run/preview/compaction deadline on
`runs start`, `preview run`, `storage compact`. *(Builder-check call.)*

`--json` means: stdout carries at most one JSON document (or NDJSON for
streams), stderr carries at most one JSON error envelope, and nothing else
is ever written to either. Every leaf, including every mutation, has a
defined success document (`pipeline put` → `{"name","project","action":
"created|updated"}`, `pipeline delete` → `{"name","deleted":true}`,
`secrets set` → `{"key","action"}`, `auth token create` → `{"token","id",
"name","expires_at"}` with no prose, `agent-guide` → `{"section","content"}`,
`skill install` → `{"installed_to","files"}`, …); human-mode banners and
hints are suppressed under `--json`. Which stream a non-zero exit uses:
- **exit 1 with a result document** (`validate` findings, a `runs start
  --wait` run document in `FailedUser`, an `apps render` failure document,
  a `preview` result in `FailedUser`) → the document on stdout, stderr
  empty;
- **exit 1 without a result document** (a generic HTTP 400 such as bad SQL,
  a locally rejected argument value) → envelope on stderr, stdout empty;
- **exit 2/3/4/9/20** → envelope on stderr, stdout empty — including a
  `runs start --wait` that ends in `FailedApplication` (the run id and
  phase ride inside the envelope's `details`).
*(Builder-check call, tightened after Codex review.)*

**3c. Error envelope** (stderr, only under `--json`):

```json
{"error":{"code":"auth_expired","message":"api token expired at …","http_status":401,"request_id":"…","hint":"run `datuplet auth login --remote …`","exit_code":3}}
```

Human mode keeps `Error: …` + hint. An optional `details` object carries
structured context (`{"run_id","phase"}` for a failed run, `{"available":
[…]}` for a project list). `request_id`: pipeline-api gains a middleware
that generates a UUID per request (never trusting an inbound header),
sets `X-Request-Id` on every response and includes `"request_id"` in every
error body written by `writeError`. The CLI's shared HTTP helper returns
`(status, body, requestID)`; the envelope includes `request_id` only when
it parses as a UUID, so a reflected credential or arbitrary header text
can never ride along. Envelope `message`s are built from the server's
`error` field, never from echoing the request. *(Wired end to end after
Codex review.)*

**3d. Exit-code table** (one place, `cmd/datuplet/exitcodes.go`, printed by
`datuplet help exit-codes`):

| Code | Meaning |
|------|---------|
| 0 | success / run Succeeded / no error findings |
| 1 | user-side failure: invalid doc, run `FailedUser`, render error, Cancelled/Expired |
| 2 | usage error (bad flag, missing arg) |
| 3 | authentication / authorization (401/403, expired, not logged in) |
| 4 | not found (pipeline, run, table, project) |
| 9 | conflict / precondition (409, CAS) |
| 20 | platform failure: run `FailedApplication`, 5xx, transport |

Every command maps onto this table and nothing else; `runs start --wait`
returns 1 for `FailedUser` and 20 for `FailedApplication`, `validate`
returns 20 on transport failure, and so on.

**3e. `pipeline put` 400** decodes `{"findings":[…]}` when present and
renders it like `validate` (D5).

### 4. Surface gaps

- `datuplet secrets list|set <key> [--value-stdin|--from-file]|delete <key>`
  (value never echoed; `set` reads stdin by default).
- `datuplet projects list`, `datuplet auth status` (whoami).
- `datuplet runs cancel <id>`.
- `datuplet runs logs <id> [--stage s] [--component c] [--tail n]` — needs
  `GET /api/v1/projects/{pid}/runs/{id}/logs` on pipeline-api. Two very
  different sizes, **maintainer decision** (open decision 2):
  - *Live-pod only*: pipeline-api tails the run's pods through the same
    clientset path preview `log_tail` uses (`pkg/pipelineapi/preview/logs.go`),
    last N KiB per container (component + gateway). Works while the Jobs
    exist — `TTLSecondsAfterFinished` is 1 h — then returns 404 with a hint.
    Thin endpoint, no new storage. Details fixed after Codex review:
    authz is `describe_effective` (the relation `runs get` uses); Job names
    are enumerated from `PipelineRun.status.resolvedSpec` + `status.runId`
    through the controller's `componentJobName` helper (exported), never
    from `spec`; a run whose snapshot is not yet resolved returns 409
    `logs_not_ready`; the tailer gains an availability result so "pods
    gone" and "empty log" are distinguishable — when the run is terminal
    and no requested container is available the endpoint returns 404
    `logs_expired` (Job TTL deletes Jobs, not the PipelineRun, so the CR's
    presence is not the signal); output is bounded in **bytes**
    (`LimitBytes` per container, default 64 KiB, max 512 KiB; aggregate
    1 MiB) with a `truncated` flag per entry.
  - *Persisted*: the observer captures the tail at terminal phase into a
    bounded `run_logs` table (or object-storage blob) with retention; logs
    survive Job TTL. Multi-component feature, its own spec.
  **Decided (maintainer, 2026-11-08): live-pod only.** Persistence is a
  follow-up RFC if agents actually hit the 1 h window.

### 5. Self-description

- `datuplet agent-guide [--section …] [--list]` prints the operator guide embedded
  at build time (`//go:embed` of `.claude/skills/datuplet-operator/**`
  copied to `cmd/datuplet/agentguide/` by a `make sync-agent-guide` target,
  CI drift-checked like the component schemas).
- `datuplet skill install [--claude|--codex|--cursor] [--dir …]` writes that
  guide to the host's skill directory (`~/.claude/skills/datuplet-operator/`
  by default) so a fresh agent gets the manual with the binary.
- `datuplet help --json` dumps the cobra command tree with flags and the
  exit-code table.
- `docs/cli.md` is generated from the same tree (`make docs-cli`, drift-
  checked).

### 6. Install story the README and skill will carry

```bash
curl -fsSL https://raw.githubusercontent.com/kacurez/datuplet/main/scripts/install-cli.sh | sh
datuplet skill install --claude
export DATUPLET_REMOTE=https://pipeline-api.example.com
export DATUPLET_API_TOKEN=dt_…        # created once by a human: datuplet auth token create --name my-agent
datuplet auth status --json           # confirms user, expiry, projects
datuplet projects list --json         # pick DATUPLET_PROJECT (name or uuid)
```

## Phasing (one PR each; each is independently shippable)

1. **Distribution**: binary split (1a), release job + install script (1b,
   1c), `version --json` (1d). Highest leverage, no API changes beyond
   `/api/v1/version`.
2. **Contract**: cobra migration (3a), global flags (3b), error envelope
   (3c), exit-code table (3d), `put` findings (3e), help consistency.
3. **Auth**: API tokens (2a), project name-or-UUID (2b), config contexts
   (2c), `auth status`, `projects list`.
4. **Surface + self-description**: `secrets`, `runs cancel`, `agent-guide`,
   `skill install`, `help --json`, `docs/cli.md`.
5. **Run logs** endpoint + `runs logs` (needs observer work; e2e-covered).

## Decisions

- Binary channel: GitHub Release assets (maintainer, 2026-11-08).
- Parser: cobra (maintainer, 2026-11-08).

- Green field: no compatibility, migration or alias obligations
  (maintainer, 2026-11-08).
- `dt_…` API tokens are the only CLI credential; `cli-api`/`local-cli` JWT
  kinds are deleted (maintainer, 2026-11-08).
- `runs logs` is in scope as live-pod tailing only (maintainer,
  2026-11-08).

## Open decisions for the maintainer

None.

## Builder-check calls (settled by the author, 2026-11-08)

- `datuplet-query` migrates to `config.json` + `dt_…` in phase 3; no legacy
  files kept for it.
- Exit 1 with a result document → stdout; every other non-zero exit → one
  envelope on stderr, stdout empty.
- No global `--timeout`; the flag keeps its run-deadline meaning on the
  three long-running commands.
- Version compatibility is `MAJOR.MINOR` equality checked in the shared
  session preflight; human-mode stderr warning, silent under `--json`,
  never a failure; server version comes from build-args → `-ldflags`.

## Codex design review (2026-11-08) — 13 Important, all folded in

Remote/token binding rule (§2c); resolver `Principal` (§2a); query-token
mint carries catalog coordinates with project selection (§2c); run logs
use `status.resolvedSpec`, `describe_effective`, availability-aware tailer,
byte bounds, `logs_expired`/`logs_not_ready` (§4); exit-20 and
no-result-exit-1 stream rules and per-leaf JSON success documents (§3b);
server request-id middleware + validated propagation (§3c); version
preflight owner and `--json` silence (§1d); pre-release smoke workflow
instead of a throwaway `v*` tag (§1b).

## Left to the builder

Exact cobra layout and alias set; tarball naming; whether `install-cli.sh`
also installs `datuplet-query` (recommend no — separate, CGO binary);
`agent-guide` section anchors; e2e coverage for the install script
(release-verify.yml already installs published artefacts on kind and is the
natural home).
