# Delete TableCommit Remnants + All Local CLI Execution — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove every remnant of the pre-RFC-021 TableCommit Job architecture (image, CLI stub, CI wiring, chart env, dead orchestration code) AND delete the CLI's local-Docker execution surface (`datuplet run --remote`, `sample`, `test-component`, `pkg/pipeline` runner, `pkg/lib/orchestrator`), which is triple-broken and unused. The CLI becomes a pure remote client (login/trigger/pipeline/storage/query) plus the K8s gateway entrypoint.

**Architecture:** Since RFC 021 the Data Gateway sidecar commits Iceberg tables inline via its commit pool (`pkg/datagateway/commit_pool.go` → `icebergjob.CommitTableFiles`). The K8s operator never schedules commit Jobs. The only remaining TableCommit caller chain was `datuplet run --remote` → `pkg/pipeline/runner.go` → `DockerOrchestrator.ExecuteTableCommit` → a container whose entrypoint hard-exits 2. That chain is also unreachable: the local gateway sidecar fatals at boot (no `pipeline_api_jwks_url` in its config, no `RUN_ID` env, login token ≠ run token). Everything local-execution is deleted wholesale; nothing replaces it (cluster-side runs via `datuplet trigger` are the supported path).

**Tech Stack:** Go (multi-module monorepo), Helm charts, GitHub Actions, Make.

**Rebase note (2026-07-10):** this branch was rebased onto `public/main` @ `9636585` (after PRs #24 RFC 026 and #25 RFC 025 merged, 102 commits). Collision analysis done: `cmd/datuplet/main.go`, `pkg/pipeline/runner.go`, `pkg/lib/orchestrator/*` were **untouched** by the merges (all line numbers/deletion targets below still valid). `examples/local-dev` and its CLAUDE.md reference were **already deleted** on main (old Task 7 is now obsolete — see below). Line numbers in the chart + Makefile tasks were refreshed against the rebased tree. No merged code imports any deletion target (`commit_types.go`'s `orchestrator` mention is a comment, not an import).

## Global Constraints

- **KEEP `pkg/icebergjob/` entirely** — `CommitTableFiles` is the DG commit pool's production commit function (`pkg/datagateway/server_v2.go:296`). `CommitTable` + `manifest_loader.go` keep existing as the `files.json` consumer (comments updated, code untouched).
- **KEEP `pkg/pipeline/config/`** — the pipeline YAML parser is imported by `pkg/k8s/controllers`, `pkg/pipelineapi/{runbackend,http,store,tokens}`. Only the runner dies.
- **KEEP the CRD `tableCommits` status field** (`pkg/k8s/api/v1/`, both CRD YAML copies) — deliberately retained in RFC 021; changing CRD shape is out of scope.
- **KEEP in `cmd/datuplet/remote.go`:** `remoteArgs`, `loadRemoteArgs`, `resolveProject`, `RequireAPIToken`, `normalizeURL`, `validateRemoteURL` — shared by `trigger`, `storage`, `query`, `pipeline` commands.
- **KEEP `datuplet gateway`** and all of `pkg/datagateway/` — the gateway image entrypoint is `datuplet gateway` (production K8s sidecar).
- The `--remote` flag on `login`/`trigger`/`storage`/`query`/`pipeline` is the cluster-address flag and MUST survive. Only the `run` command is deleted.
- Never push to `main`, never tag. Land via draft PR on branch `claude/exciting-aryabhata-64fd3d`.
- macOS/BSD environment: do not use GNU-only sed flags; make edits with the file-edit tool, not sed.
- Commit message style follows repo history (`chore(scope): ...`, `docs: ...`); end every commit message with `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`.
- Out of scope (already tracked as a separate follow-up task/chip): prose-doc sweep of docs/auth-flow.md Leg 3, docs/troubleshooting.md §9, docs/components.md:245, docs/known-limitations.md, components/sql-transform/README.md:170. CHANGELOG.md historical entries stay untouched.

---

### Task 0: Commit the pending RFC 021 doc alignment (already edited, uncommitted)

The worktree already contains uncommitted doc/comment fixes from earlier in this session (CLAUDE.md, docs/architecture.md, two comments in pipelinerun_jobs.go). Land them first so every later task starts from a clean tree.

**Files:**
- Already modified (verify only): `CLAUDE.md`, `docs/architecture.md`, `pkg/k8s/controllers/pipelinerun_jobs.go`, `docs/superpowers/plans/2026-07-10-delete-tablecommit-and-local-execution.md` (this plan, new)

- [ ] **Step 1: Verify working-tree contents**

Run: `git status --short`
Expected: exactly ` M CLAUDE.md`, ` M docs/architecture.md`, ` M pkg/k8s/controllers/pipelinerun_jobs.go`, `?? docs/superpowers/plans/2026-07-10-delete-tablecommit-and-local-execution.md`. If anything else appears, STOP and reconcile before committing.

- [ ] **Step 2: Compile the touched Go package**

Run: `go build ./pkg/k8s/controllers/`
Expected: exit 0, no output.

- [ ] **Step 3: Commit**

```bash
git add CLAUDE.md docs/architecture.md pkg/k8s/controllers/pipelinerun_jobs.go docs/superpowers/plans/2026-07-10-delete-tablecommit-and-local-execution.md
git commit -m "docs: align CLAUDE.md + architecture.md with RFC 021 inline commits

TableCommit Jobs no longer exist; the DG sidecar commits inline via its
commit pool. Also corrects impersonation-JWT lifetime 5min -> 60s
(tokens/mint.go ImpersonationLifetime) and adds the deletion plan.

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 1: Delete `datuplet run` (CLI command + `runRemote`)

**Files:**
- Modify: `cmd/datuplet/main.go:32-35` (flagset), `:126-143` (case), `:272` + `:288-291` (usage)
- Modify: `cmd/datuplet/remote.go` (delete `runRemote` ~lines 260-352, `generateRunID` ~lines 227-232, unused imports)
- Modify: `cmd/datuplet/remote_test.go` (delete `TestRunRemote_GeneratesUniqueRunID` ~lines 180-226 incl. its comment block)
- Delete: `tests/e2e/scenarios_remote_cli_test.go` (the whole file — the `datuplet run --remote` integration smoke; its harness `make e2e-remote-cli` / `install-smoke.sh` no longer exists, and it exercises the deleted command). It is in the `tests/e2e` Go module and only execs the CLI binary (no import of deleted packages), so its removal is independent — but land it here so no test references a deleted command.

**Interfaces:**
- Consumes: nothing from other tasks.
- Produces: `cmd/datuplet/remote.go` retains exactly `remoteArgs`, `loadRemoteArgs`, `resolveProject`, `RequireAPIToken`, `normalizeURL`, `validateRemoteURL` (unchanged signatures) — Task 2 and later tasks assume these still exist.

- [ ] **Step 1: Remove the `run` flagset from main.go**

Delete these lines (main.go:32-35):

```go
	runCmd := flag.NewFlagSet("run", flag.ExitOnError)
	runRemoteFlag := runCmd.String("remote", "", "pipeline-api URL of the target cluster (required for remote runs)")
	runTokenFile := runCmd.String("token-file", "", "Path to JWT token file (default: ~/.datuplet/token)")
	runProject := runCmd.String("project", "", "Project name to run under (required if you have access to >1 project; auto-defaulted if you have exactly one)")
```

- [ ] **Step 2: Remove the `case "run":` block from main.go**

Delete these lines (main.go:126-143):

```go
	case "run":
		runCmd.Parse(os.Args[2:])
		if *runRemoteFlag == "" {
			fmt.Fprintln(os.Stderr, "Error: --remote is required")
			fmt.Fprintln(os.Stderr, "Usage: datuplet run --remote <pipeline-api-url> <pipeline.yaml>")
			os.Exit(1)
		}
		pipelineArgs := runCmd.Args()
		if len(pipelineArgs) == 0 {
			fmt.Fprintln(os.Stderr, "Error: pipeline YAML path is required")
			fmt.Fprintln(os.Stderr, "Usage: datuplet run --remote <pipeline-api-url> <pipeline.yaml>")
			os.Exit(1)
		}
		if err := runRemote(*runRemoteFlag, *runTokenFile, *runProject, pipelineArgs[0]); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
```

- [ ] **Step 3: Remove `run` from the usage text**

In `printUsage()` delete the command line (main.go:272):

```
  run                    Run a pipeline against a remote Datuplet cluster
```

and the options section (main.go:288-291):

```
Options for 'run':
  -remote string         pipeline-api URL of the target cluster (required)
  -token-file string     Path to JWT token file (default: ~/.datuplet/token)
  <pipeline.yaml>        Path to pipeline YAML file (positional, required)
```

- [ ] **Step 4: Delete `runRemote` + `generateRunID` from remote.go**

Delete the whole `runRemote` function including its doc comment (starts `// runRemote implements \`datuplet run --remote <url> <pipeline.yaml>\`.`, ends with `fmt.Printf("run %s succeeded — snapshots: %s/ui/storage\n", runID, args.Remote)` / `return nil` / `}`), and delete `generateRunID` with its comment:

```go
// generateRunID returns a fresh UUID string for use as a pipeline run
// identifier. Extracted as a function so tests can assert uniqueness without
// launching Docker containers.
func generateRunID() string {
	return uuid.New().String()
}
```

Then shrink the import block. New imports for remote.go (delete `context`, `os/signal`, `syscall`, `github.com/datuplet/datuplet/pkg/lib/orchestrator/docker`, `github.com/datuplet/datuplet/pkg/pipeline`, `github.com/google/uuid`):

```go
import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
)
```

- [ ] **Step 5: Delete `TestRunRemote_GeneratesUniqueRunID` from remote_test.go**

Delete the test function AND the 4-line comment block immediately above it (starting `// loadRemoteArgs calls (which underpins the run-id generation path)`). Keep every `TestLoadRemoteArgs_*` test and the shared helpers (`writeDatupletFiles`, `fakeJWT`, `clusterMeta` fixtures).

- [ ] **Step 6: Delete the run --remote e2e smoke**

Run: `git rm tests/e2e/scenarios_remote_cli_test.go`
(All its helpers — `startPortForward`, `patchPipelineAPIEnv`, `waitRollout`, `readAdminPasswordFromSecret`, `remoteCLI*` — are file-local; verified nothing else in the `tests/e2e` package uses them.)

- [ ] **Step 7: Build + test (both modules)**

Run: `go build ./cmd/datuplet/ && go test ./cmd/datuplet/ && (cd tests/e2e && go vet ./...)`
Expected: build OK; all remaining tests PASS; e2e module still vets clean.

- [ ] **Step 8: Commit**

```bash
git add cmd/datuplet/main.go cmd/datuplet/remote.go cmd/datuplet/remote_test.go tests/e2e/scenarios_remote_cli_test.go
git commit -m "chore(cli): delete 'datuplet run --remote' local execution

Triple-broken since the JWT hardening + RFC 021: the local gateway
sidecar fatals at boot (no pipeline_api_jwks_url, no RUN_ID env, login
token is not a run token) and the commit stage spawned an iceberg-job
container that exits 2. Cluster-side runs via 'datuplet trigger' are
the supported path. loadRemoteArgs and friends stay (shared by
trigger/storage/query/pipeline). Also drops the dormant remote-cli e2e
smoke (its Helm-smoke harness no longer exists).

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 2: Delete `sample` + `test-component` commands

**Files:**
- Delete: `cmd/datuplet/test_sample.go`
- Modify: `cmd/datuplet/main.go:18-27` (flagsets), `:79-101` (cases), `:280-281` + `:326-335` + `:337-340` (usage/examples)

**Interfaces:**
- Consumes: Task 1's main.go state (run already gone).
- Produces: `cmd/datuplet` no longer imports `pkg/lib/orchestrator` anywhere — precondition for Task 3's package deletion.

- [ ] **Step 1: Remove the two flagsets from main.go**

Delete (main.go:18-27):

```go
	testCmd := flag.NewFlagSet("test-component", flag.ExitOnError)
	testImage := testCmd.String("image", "", "Component image to test")
	testConfig := testCmd.String("config", "{}", "Component config (JSON)")
	testEndpoint := testCmd.String("endpoint", "localhost:9000", "Data lake endpoint")
	testBucket := testCmd.String("bucket", "datuplet", "Data lake bucket")

	sampleCmd := flag.NewFlagSet("sample", flag.ExitOnError)
	sampleImage := sampleCmd.String("image", "", "Component image to sample (required)")
	sampleConfig := sampleCmd.String("config", "{}", "Component config (JSON)")
	sampleLimit := sampleCmd.Int("limit", 10, "Maximum number of rows to return")
```

- [ ] **Step 2: Remove the two cases from main.go**

Delete `case "test-component":` (main.go:79-89) and `case "sample":` (main.go:91-101) — both full blocks ending with their closing `}` before the next `case`.

- [ ] **Step 3: Update usage text**

Delete the command lines:

```
  test-component         Test a single component
  sample                 Get sample data from a component (for AI/automation)
```

Delete both options sections (`Options for 'test-component':` 4 lines + `Options for 'sample':` 3 lines). Replace the Examples block so only the gateway example remains:

```
Examples:
  datuplet gateway --minio --config minio.yaml`)
```

- [ ] **Step 4: Delete the implementation file**

Run: `git rm cmd/datuplet/test_sample.go`

- [ ] **Step 5: Build + test + verify no orchestrator imports remain in cmd/**

Run: `go build ./cmd/... && go test ./cmd/... && grep -rn 'lib/orchestrator' cmd/ ; true`
Expected: build/tests pass; the grep prints nothing.

- [ ] **Step 6: Commit**

```bash
git add -A cmd/datuplet/
git commit -m "chore(cli): delete 'sample' + 'test-component' local-docker commands

test-component was the legacy pre-lakekeeper flow (static minioadmin
creds, legacy staging outputs); sample only ran a container and printed
stdout. Neither is documented or covered by e2e. Last users of
pkg/lib/orchestrator.

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 3: Delete `pkg/pipeline` runner + entire `pkg/lib/orchestrator`

**Files:**
- Delete: `pkg/pipeline/runner.go`, `pkg/pipeline/runner_test.go`
- Delete: `pkg/lib/orchestrator/` (whole directory: `orchestrator.go`, `docker/docker.go`, `docker/docker_test.go`)
- Keep untouched: `pkg/pipeline/config/` (parser.go, parser_test.go, types.go)

**Interfaces:**
- Consumes: Tasks 1+2 must be done first (they remove the last importers).
- Produces: repo-wide zero references to `pkg/lib/orchestrator` and to the root `pkg/pipeline` package (only `pkg/pipeline/config` remains importable).

- [ ] **Step 1: Verify no remaining importers**

Run: `grep -rn 'lib/orchestrator"\|lib/orchestrator/docker"\|datuplet/pkg/pipeline"' --include='*.go' . | grep -v '^./pkg/pipeline/runner\|^./pkg/lib/orchestrator'`
Expected: no output. If anything prints, STOP — a caller was missed.

- [ ] **Step 2: Delete**

```bash
git rm pkg/pipeline/runner.go pkg/pipeline/runner_test.go
git rm -r pkg/lib/orchestrator
```

- [ ] **Step 3: Build + full root-module test**

Run: `go build ./... && go test ./...`
Expected: everything compiles; all tests pass (`pkg/pipeline/config` tests still run).

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "chore: delete local pipeline runner + docker orchestrator

pkg/pipeline/runner.go and pkg/lib/orchestrator existed solely for the
now-deleted local CLI execution (run/sample/test-component). The K8s
operator drives Job/Pod lifecycle directly and never used this
interface. pkg/pipeline/config (the pipeline YAML parser shared by the
operator and pipeline-api) stays.

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 4: Delete the removed-command CLI stubs (iceberg-job + table-gateway), Dockerfile, and Makefile wiring

**Files:**
- Modify: `cmd/datuplet/main.go:69-71` (comment), `:120-124` (iceberg-job case), `:238-240` (table-gateway case), `:278-279` (usage lines)
- Delete: `utils/docker/Dockerfile.iceberg-job`
- Modify: `Makefile:5` (.PHONY), `:43-47` (targets), `:114-115` (docker-build-k8s — now has a query-worker recipe line, RFC 025), `:307-308` (k8s-rebuild-services)

- [ ] **Step 1: Remove both removed-command CLI stubs**

Delete from main.go the iceberg-job case:

```go
	case "iceberg-job":
		fmt.Fprintln(os.Stderr, "Error: `datuplet iceberg-job --mode=table-commit` is removed in RFC 021.")
		fmt.Fprintln(os.Stderr, "Inline commit now lives in the data gateway sidecar. This binary will")
		fmt.Fprintln(os.Stderr, "grow --mode=compact / expire-snapshots / remove-orphans in a future RFC.")
		os.Exit(2)
```

the table-gateway case:

```go
	case "table-gateway":
		fmt.Fprintln(os.Stderr, "Error: the `table-gateway` subcommand has been removed. Lakekeeper now serves the catalog directly.")
		os.Exit(1)
```

the explanatory comment above the arg check (main.go:69-71):

```go
	// The table-gateway subcommand has been removed; lakekeeper is now the
	// catalog of record. The case below still exists so users running the old
	// subcommand see a clear error instead of a generic "unknown command".
```

and both usage lines:

```
  iceberg-job            (REMOVED in RFC 021) Inline commit now lives in the data gateway sidecar
  table-gateway          (REMOVED) Lakekeeper now serves the catalog directly
```

- [ ] **Step 2: Delete the Dockerfile**

Run: `git rm utils/docker/Dockerfile.iceberg-job`

- [ ] **Step 3: Update the Makefile**

Line 5 — change:

```make
	build build-pipeline-api build-gateway build-iceberg-job build-services \
```

to:

```make
	build build-pipeline-api build-gateway \
```

Delete lines 43-47 entirely:

```make
# Build the iceberg job image
build-iceberg-job: ## Build iceberg-job Docker image
	docker build -t datuplet/iceberg-job:latest -f utils/docker/Dockerfile.iceberg-job .

build-services: build-iceberg-job ## Build service images (iceberg-job)
```

Line 114 — change ONLY the target's dep list + help text (leave the line-115 `docker build ... query-worker` recipe untouched):

```make
docker-build-k8s: docker-build-operators build-gateway build-iceberg-job docker-build-pipeline-api docker-build-pipeline-observer ## Build all K8s images (operators + gateway + iceberg-job + pipeline-api + pipeline-observer + query-worker)
```

to:

```make
docker-build-k8s: docker-build-operators build-gateway docker-build-pipeline-api docker-build-pipeline-observer ## Build all K8s images (operators + gateway + pipeline-api + pipeline-observer + query-worker)
```

Lines 307-308 — change:

```make
# Rebuild services: gateway, iceberg-job (RFC 007 Slice 9: TG retired)
k8s-rebuild-services: build-gateway build-iceberg-job ## Rebuild gateway + iceberg-job images
```

to:

```make
k8s-rebuild-services: build-gateway ## Rebuild the gateway image
```

- [ ] **Step 4: Verify**

Run: `go build ./cmd/datuplet/ && make -n docker-build-k8s >/dev/null && make -n k8s-rebuild-services >/dev/null && grep -n 'iceberg' Makefile ; true`
Expected: build OK, make dry-runs OK, grep prints nothing.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "chore: delete iceberg-job image, removed-command stubs, Makefile targets

The iceberg-job image's entrypoint has hard-exited 2 since RFC 021;
nothing schedules it (operator commits inline via the DG sidecar). The
workspace-* modes in the Dockerfile comment had no backing code or RFC.
Also drops the RFC 007-era table-gateway removed-command stub — same
courtesy-error purpose, neither command is documented anywhere.

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 5: Remove iceberg-job from CI workflows

**Files:**
- Modify: `.github/workflows/_release-services.yml:3-7` (header comment), `:45-50` (job)
- Modify: `.github/workflows/release.yml:16-19` (comment), `:71-75` (comment + loop)
- Modify: `.github/workflows/e2e.yml:77` (kind-load entry)

- [ ] **Step 1: _release-services.yml**

Header comment — change:

```yaml
# Reusable: builds + manifests the six datuplet service images
# (pipeline-api, pipeline-observer, pipeline-operator, gateway, iceberg-job,
# query-worker). Each is a call to _release-image.yml. Called once from
# release.yml as the `services` job; appears as a collapsible group in the
# run-detail UI containing 18 flat entries (6 images × 3 sub-jobs).
```

to:

```yaml
# Reusable: builds + manifests the five datuplet service images
# (pipeline-api, pipeline-observer, pipeline-operator, gateway,
# query-worker). Each is a call to _release-image.yml. Called once from
# release.yml as the `services` job; appears as a collapsible group in the
# run-detail UI containing 15 flat entries (5 images × 3 sub-jobs).
```

Delete the job block:

```yaml
  iceberg-job:
    uses: ./.github/workflows/_release-image.yml
    with:
      name: iceberg-job
      dockerfile: utils/docker/Dockerfile.iceberg-job
    secrets: inherit
```

- [ ] **Step 2: release.yml**

Comment at lines 16-19 — change `# gateway, iceberg-job. Appears as one top-level "services" row in` to `# gateway, query-worker. Appears as one top-level "services" row in` (leave the "15 flat entries (5 images × 3" wording — it is now correct).

Comment at line 71 — change `# Repoint image.repository to ghcr.io/<owner>/<image>. The six` to `... The five`.

Loop at line 75 — change:

```bash
          for img in pipeline-api pipeline-operator pipeline-observer gateway iceberg-job query-worker; do
```

to:

```bash
          for img in pipeline-api pipeline-operator pipeline-observer gateway query-worker; do
```

- [ ] **Step 3: e2e.yml**

Delete line 77:

```yaml
            datuplet/iceberg-job:latest \
```

(The `for img in \` list must remain valid shell — verify the preceding line still ends with `\` and the list still terminates with `; do`.)

- [ ] **Step 4: Validate YAML syntax**

Run: `python3 -c "import yaml; [yaml.safe_load(open(f)) for f in ['.github/workflows/_release-services.yml','.github/workflows/release.yml','.github/workflows/e2e.yml']]; print('OK')"`
Expected: `OK`.

- [ ] **Step 5: Commit**

```bash
git add .github/workflows/
git commit -m "ci: stop building/releasing the iceberg-job image

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 6: Chart cleanup (operator env + values + stale chart comments)

**Files:**
- Modify: `charts/datuplet-app/values.yaml:13` (comment), `:39-42` (icebergJob block)
- Modify: `charts/datuplet-app/templates/pipeline-operator/deployment.yaml:1-13` (Helm-comment header, NEW step), `:57-77` (env + comments)
- Modify: `charts/datuplet-app/templates/query-worker/deployment.yaml:24-25`, `:74-76`

**NOTE:** chart + operator-env change ⇒ repo rules require `make e2e-k8s` against an OrbStack cluster before merge. That gate runs in Task 9 — do not skip it.

- [ ] **Step 1: values.yaml**

Change the runtimeDefaults comment:

```yaml
# Tolerations applied to per-run Pods spawned by the pipeline-operator
# (PipelineRun component Pods + TableCommit Job Pods). Empty default
```

to:

```yaml
# Tolerations applied to per-run Pods spawned by the pipeline-operator
# (PipelineRun component Pods). Empty default
```

Delete the icebergJob image block:

```yaml
  # iceberg-job image — pipeline-operator passes this to every spawned
  # TableCommit Job via the ICEBERG_JOB_IMAGE env var.
  icebergJob:
    repository: datuplet/iceberg-job
    tag: latest
```

- [ ] **Step 2: pipeline-operator/deployment.yaml — Helm-comment header (lines 1-13)**

This `{{- /* ... */}}` header still describes the deleted commit-Job model. Change:

```
This is the only operator. The PipelineRun controller schedules commit Jobs
directly when a stage's component Pods complete.

Long-lived S3/GCS credentials are NOT forwarded to the operator or to spawned
commit Job Pods. Commit Pods use only the run-token JWT; lakekeeper vends
per-table STS credentials for all data-plane access.

The operator no longer carries warehouse/project knowledge. The PipelineAPIURL
drives the JWKS endpoint DG sidecars + commit Jobs use to validate the mounted
run-token JWT, which carries warehouse + project_id claims.
```

to:

```
This is the only operator. The PipelineRun controller builds one component
Job per stage component; the Data Gateway sidecar commits Iceberg tables
inline (RFC 021), so there are no separate commit Jobs.

Long-lived S3/GCS credentials are NOT forwarded to the operator or to the
spawned component Pods. The DG sidecar uses only the run-token JWT; lakekeeper
vends per-table STS credentials for all data-plane access.

The operator no longer carries warehouse/project knowledge. The PipelineAPIURL
drives the JWKS endpoint DG sidecars use to validate the mounted run-token
JWT, which carries warehouse + project_id claims.
```

- [ ] **Step 3: pipeline-operator/deployment.yaml — env + inline comments**

Change the PIPELINE_API_URL comment:

```yaml
            # Base URL of pipeline-api. The operator derives the JWKS URL
            # ({PIPELINE_API_URL}/api/v1/auth/jwks.json) and injects it into every
            # DG sidecar configMap + commit Job env so they can validate the
            # mounted run-token JWT against pipeline-api's published keys.
```

to:

```yaml
            # Base URL of pipeline-api. The operator derives the JWKS URL
            # ({PIPELINE_API_URL}/api/v1/auth/jwks.json) and injects it into every
            # DG sidecar configMap so it can validate the mounted run-token
            # JWT against pipeline-api's published keys.
```

Change the image-tags comment and delete the env var:

```yaml
            # Image tags for the two pod types the operator spawns:
            # component-pod gateway sidecars and per-table commit Jobs.
            # Plumbed from chart values so release artifacts pin versioned
            # tags while local dev defaults to :latest.
            - name: GATEWAY_IMAGE
              value: "{{ .Values.image.gateway.repository }}:{{ .Values.image.gateway.tag }}"
            - name: ICEBERG_JOB_IMAGE
              value: "{{ .Values.image.icebergJob.repository }}:{{ .Values.image.icebergJob.tag }}"
```

becomes:

```yaml
            # Image tag for the gateway sidecar the operator injects into
            # component Pods. Plumbed from chart values so release artifacts
            # pin versioned tags while local dev defaults to :latest.
            - name: GATEWAY_IMAGE
              value: "{{ .Values.image.gateway.repository }}:{{ .Values.image.gateway.tag }}"
```

Change the pull-policy comment:

```yaml
            # Runtime pull policy applied by the operator to per-run
            # pods (gateway sidecar, component container, commit Job).
```

to:

```yaml
            # Runtime pull policy applied by the operator to per-run
            # pods (gateway sidecar, component container).
```

- [ ] **Step 4: query-worker/deployment.yaml**

Change (template header comment, line ~24-25):

```
JWKS URL: uses the same in-cluster pipeline-api endpoint as DG sidecars
and tablecommit Jobs ({pipelineApiInClusterUrl}/api/v1/auth/jwks.json).
```

to:

```
JWKS URL: uses the same in-cluster pipeline-api endpoint as DG sidecars
({pipelineApiInClusterUrl}/api/v1/auth/jwks.json).
```

Change (env comment, line ~74-76):

```yaml
            # JWKS endpoint: same in-cluster pipeline-api URL used by DG
            # sidecars and tablecommit Jobs for run-token validation.
```

to:

```yaml
            # JWKS endpoint: same in-cluster pipeline-api URL used by DG
            # sidecars for run-token validation.
```

- [ ] **Step 5: Verify rendering**

Run: `make chart-render-check && helm template charts/datuplet-app 2>/dev/null | grep -i iceberg ; true`
Expected: render-check passes; the grep prints nothing.

- [ ] **Step 6: Commit**

```bash
git add charts/datuplet-app/
git commit -m "chore(chart): drop dead ICEBERG_JOB_IMAGE env + icebergJob values

The operator never read ICEBERG_JOB_IMAGE after RFC 021 deleted the
commit-Job state machine. Also fixes stale TableCommit chart comments.

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 7: ~~Delete the `examples/local-dev` module~~ — OBSOLETE (already done on main)

**Post-rebase status:** `examples/local-dev/` was **already deleted** on `public/main` (one of the 102 merged commits removed it), and the CLAUDE.md monorepo-`go mod tidy` bullet **no longer lists it** either. Nothing to do.

- [ ] **Step 1: Confirm it's gone (no action)**

Run: `test ! -e examples/local-dev && echo GONE; grep -c 'local-dev' CLAUDE.md`
Expected: `GONE`, and grep count `0`. If either is non-zero, the merge state changed — reinstate the deletion steps from git history of this plan.

---

### Task 8: Doc + comment sweep for the deleted code, plus CHANGELOG

Only text that references *deleted* things (iceberg-job image, TableCommit Job, `datuplet run`). The broader stale-prose sweep stays in the separate follow-up task.

**Files:**
- Modify: `README.md:91-97`, `docs/secrets.md:29-72`, `CLAUDE.md` (2 spots), `CHANGELOG.md` (new entry at top)
- Modify: `pkg/datagateway/files_manifest.go:1-8`, `:64-66`; `pkg/icebergjob/commit_shared.go:1-6`; `pkg/icebergjob/manifest_loader.go:1-25`
- Modify: `pkg/k8s/api/v1/commit_types.go:31` (stale `pkg/lib/orchestrator` reference — the package it names is deleted in Task 3)
- Modify: `tests/e2e/framework/signer.go:110-112`; `utils/deploy/k8s/minio.yaml:13-15`; `examples/k8s/simple-pipeline.yaml:5-8`

- [ ] **Step 1: README.md** — replace the flow sentence:

```
A user triggers a pipeline via the UI or REST API. `pipeline-api` mints a
per-run RS256 JWT and creates a PipelineRun CRD. `pipeline-operator` schedules
component Pods, each with a Data Gateway sidecar. The sidecar fetches
STS credentials from Lakekeeper, writes parquet to S3/GCS, records a
per-table `files.json` manifest, then the operator schedules a TableCommit Job
that commits the files to the Iceberg table via iceberg-go. OpenFGA enforces
authorization at every step.
```

with:

```
A user triggers a pipeline via the UI or REST API. `pipeline-api` mints a
per-run RS256 JWT and creates a PipelineRun CRD. `pipeline-operator` schedules
component Pods, each with a Data Gateway sidecar. The sidecar fetches
STS credentials from Lakekeeper, writes parquet to S3/GCS, and commits the
files to the Iceberg table inline via its commit pool (iceberg-go against
Lakekeeper), leaving a per-table `files.json` audit breadcrumb. OpenFGA
enforces authorization at every step.
```

- [ ] **Step 2: docs/secrets.md** — delete the entire `## Docker` section (lines 29-72: the one-file-per-secret walkthrough referencing `./bin/datuplet run`, its fail-fast list, and the `### Makefile example`). The doc then flows from the parse-error paragraph straight into `## Kubernetes`. Adjust the doc's intro if it enumerates "Docker and Kubernetes" modes (check the first 25 lines and drop the Docker mention if present).

- [ ] **Step 3: CLAUDE.md** — two edits:

Key-directories row:

```
| `pkg/pipeline/` | Pipeline execution engine. |
```

becomes:

```
| `pkg/pipeline/config/` | Pipeline YAML spec parser (shared by operator + pipeline-api). |
```

Datalake bullet: change `used by the local pipeline engine (\`pkg/pipeline\`) and the pipeline-api storage walker fallback` to `used by the pipeline-api storage walker fallback`.

- [ ] **Step 4: Go/YAML comment fixes** (code behavior untouched):

`pkg/datagateway/files_manifest.go` package comment — replace `TableCommit consumes it via iceberg-go's` (line ~5) context so the sentence reads: `Each manifest lists the parquet files DG produced for that table; since RFC 021 it is an audit breadcrumb (the inline commit pool passes paths in memory), and icebergjob.CommitTable remains the reader for out-of-band consumers.` Replace line 66 `// Consumed by TableCommit via iceberg-go's txn.AddFiles.` with `// Read back by icebergjob.CommitTable (out-of-band consumers).`

`pkg/icebergjob/commit_shared.go` header — replace the sentences claiming `--mode=table-commit` and "one production caller today" with: `CommitTable is the manifest-path variant retained for out-of-band files.json consumers; it has no in-repo production caller since RFC 021 (the DG commit pool calls CommitTableFiles directly).`

`pkg/icebergjob/manifest_loader.go` header — TWO stale lines: replace `This file is the inverse: TableCommit reads the per-table manifest,` (line ~7) with `This file is the inverse: CommitTable reads the per-table manifest,`, and `(rather than importing from pkg/datagateway) keeps TableCommit's` (line ~25) with `(rather than importing from pkg/datagateway) keeps CommitTable's`.

`pkg/k8s/api/v1/commit_types.go:31` — the comment `// the Docker/local orchestrator path (\`pkg/lib/orchestrator\`).` names a package deleted in Task 3. Reword to drop the dead package reference, e.g. `// the now-removed Docker/local execution path (deleted with pkg/lib/orchestrator).` — or, if the surrounding comment's only purpose was to explain that path, trim it to describe just what the CRD field is (a historical status placeholder). Read the full comment block first and keep it truthful.

`tests/e2e/framework/signer.go:111` — change `// FGAHarness.LakekeeperProjectID. DG / TableCommit forward it as the` to `// FGAHarness.LakekeeperProjectID. DG forwards it as the`.

`utils/deploy/k8s/minio.yaml:13-15` — change `# S3 env var names (for table-commit).` to `# S3 env var names.` and `# verbatim to DataGateway + TableCommit pods in other namespaces` to `# verbatim to DataGateway pods in other namespaces`.

`examples/k8s/simple-pipeline.yaml:7-8` — change `# Slice 9). DG fetches per-table STS credentials at write time;` / `# TableCommit talks directly to lakekeeper for the metadata commit.` to `# Slice 9). DG fetches per-table STS credentials at write time and` / `# commits the tables inline via its commit pool (RFC 021).`

- [ ] **Step 5: CHANGELOG.md** — add at the top of the Unreleased/next section (create `## Unreleased` heading if absent):

```markdown
### Removed

- **`datuplet run --remote`, `datuplet sample`, `datuplet test-component`.**
  All local-Docker execution is gone; `datuplet trigger` (cluster-side runs)
  is the supported path. The local runner had been broken since the run-token
  hardening + RFC 021 (gateway boot validation + inline commits).
- **iceberg-job image and `datuplet iceberg-job` subcommand.** Dead since
  RFC 021 moved commits into the Data Gateway sidecar; the operator chart no
  longer sets `ICEBERG_JOB_IMAGE` and `image.icebergJob` values are removed.
- **`pkg/lib/orchestrator` and `pkg/pipeline` runner** (internal packages);
  `pkg/pipeline/config` (the YAML spec parser) is unchanged.
```

- [ ] **Step 6: Build + test + commit**

Run: `go build ./... && go test ./... && (cd tests/e2e && go vet ./...)`
Expected: all pass.

```bash
git add -A
git commit -m "docs: purge TableCommit + local-run references tied to deleted code

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 9: Final verification + draft PR

- [ ] **Step 1: Zero-reference sweeps**

Run each; ALL must print nothing (excluding `docs/superpowers/plans/`, `CHANGELOG.md`, and `.git`):

```bash
grep -rn 'ExecuteTableCommit\|TableCommitSpec' --include='*.go' .
grep -rni 'iceberg-job\|icebergJob\|ICEBERG_JOB_IMAGE' --exclude-dir=.git --exclude-dir=docs -r . | grep -v CHANGELOG.md
grep -rn 'runRemote\|generateRunID\|sampleComponent\|testComponent\|table-gateway' --include='*.go' .
grep -rn 'lib/orchestrator\|datuplet/pkg/pipeline"' --include='*.go' .
```

Note: `pb.TableCommitResult` (the gRPC session-commit response type in `sdk/go` + `pkg/datagateway/proto`) is a DIFFERENT thing and must still exist — the first grep is scoped to the two orchestrator symbols only.

- [ ] **Step 2: Full builds + tests across modules**

Run: `go build ./... && go test ./... && make tidy && make chart-render-check && (cd tests/e2e && go vet ./...) && (cd sdk/go && go build ./...)`
Expected: all pass; `git status` clean after tidy.

- [ ] **Step 3: e2e against OrbStack (required — chart/operator change)**

Run: `kubectl config current-context` — if an OrbStack cluster is available, run `make e2e-k8s`. Expected: suite passes (proves the operator Deployment renders/behaves without ICEBERG_JOB_IMAGE and pipelines still commit via the DG inline pool). If no cluster is available in the session, STOP here and hand off: the maintainer runs `make e2e-k8s` before merging.

- [ ] **Step 4: Push branch + draft PR**

```bash
# NOTE: this worktree's remote is named `public` (git@github.com:kacurez/datuplet),
# not `origin`. gh must target that repo explicitly.
git push -u public claude/exciting-aryabhata-64fd3d
gh pr create --draft --repo kacurez/datuplet --base main --head claude/exciting-aryabhata-64fd3d --title "chore: delete TableCommit remnants + local CLI execution" --body "$(cat <<'EOF'
## Summary
- Deletes every remnant of the pre-RFC-021 TableCommit Job architecture: iceberg-job image + Dockerfile, CLI stub, CI release/e2e wiring, dead ICEBERG_JOB_IMAGE chart env, stale comments.
- Deletes all local-Docker CLI execution: `datuplet run --remote`, `sample`, `test-component`, the `pkg/pipeline` runner, and the entire `pkg/lib/orchestrator` package. The local run path had been triple-broken (gateway boot validation, run-token model, exit-2 commit container) and is unused/undocumented; `datuplet trigger` is the supported path.
- Keeps: `pkg/icebergjob` (DG commit-pool dependency), `pkg/pipeline/config` (shared YAML parser), CRD `tableCommits` status field, `loadRemoteArgs` + friends (used by trigger/storage/query/pipeline).

## Verification
- `go build ./... && go test ./...`, `make tidy`, `make chart-render-check`, e2e-module vet — all green.
- `make e2e-k8s` (OrbStack): [state result here — required before merge, chart/operator change]

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

Report the PR number back to the maintainer.
