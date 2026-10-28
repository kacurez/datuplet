# Review guide

What a reviewer — human, local `/code-review`, or Codex — should look for
in this repo. Conventions live in [CLAUDE.md](CLAUDE.md); this file says
what *failure* looks like, what to skip, and how much scrutiny a change
needs. Keep it short; add a line when an incident teaches something.

## What failure looks like, by area

Each line is a bug class that has shipped or nearly shipped here.

- **Storage paths** (`pkg/datagateway/`, `pkg/lib/datalake/`): a
  `path.Join` / `filepath.Join` on a URL — `s3://b` silently becomes
  `s3:/b`. Only `joinStoragePath()` is allowed.
- **Credentials** (`pkg/catalogwriter/`, `pkg/datupleticeio/`): a secret
  reaching a log. `%v`/`%s` are redacted by Stringers; `%#v` is not. Any
  new struct holding a token or key needs a redacting `String()`.
- **Storage config**: `UseSSL` derived from `UsePathStyle` or vice versa.
  They are independent.
- **CRDs** (`pkg/k8s/api/v1/`, `charts/*/crds/`): a type change without
  the matching manifest edit and `DeepCopyInto` update. Remember helm
  never upgrades CRDs on an existing cluster — `make k8s-reload-crds`.
- **NetworkPolicies** (`charts/*/templates/`): a new port or caller not
  added to the ingress/egress rules. This crashlooped pipeline-observer
  and left runs stuck Pending; e2e on OrbStack is the only thing that
  catches it.
- **FGA model** (`charts/datuplet-app/files/fga/`): `fgaModel.version`
  bumped in one chart but not `platform.fgaModelVersion` in
  `datuplet-lakekeeper`. CI checks within `datuplet-app` only.
- **e2e** (`tests/e2e/`): a scenario that SKIPs instead of failing. The
  suite once went fake-green for weeks because the `duckdb` CLI was
  missing on the runner. A green e2e with zero assertions is red.
- **Committed artifacts**: a QuickJS host/guest change without a rebuilt
  `pkg/appengine/embed/engine.wasm`; a `components/*/schema.json` edit
  without `make sync-component-schemas`; a vegaspec change without
  `make sync-appshell-schema`.
- **Go modules**: a root-only `go mod tidy`. The components, SDK and e2e
  modules drift and their Docker builds fail CI. Always `make tidy`.
- **Component schemas** (`components/*/schema.json`): anything outside the
  Form Subset (`oneOf`, `$ref`, `if`, …) or a missing `description`. The
  UI then cannot render the form; `pkg/pipeline/schemalint` is the gate.
- **Run tokens / JWTs** (`pkg/pipelineapi/tokens/`, gateway boot): a
  validator check relaxed (audience, `token_kind`, `run_id == $RUN_ID`).
  Each one is a named defence; removing it needs a reason in the PR.
- **Release plumbing** (`Makefile`, `scripts/`, `.github/workflows/`):
  a version string edited by hand instead of `make bump-version`; the
  release workflow fails on tag/chart mismatch, and a wrong chart
  publishes exactly as committed.

## Skip

Generated or synced; review the source instead.

- `**/*.pb.go`, `**/*_pb2.py`
- `charts/datuplet-lakekeeper/_render/*.golden.yaml`
- `charts/datuplet-app/files/component-schemas/` (copies of
  `components/*/schema.json`)
- `ui/appshell/vegaspec.schema.json` (copy of `pkg/appengine/vegaspec/`)
- `pkg/appengine/embed/engine.wasm`
- `docs/superpowers/` (design history, not code)

## How to report

- Every finding cites `file:line` and says what input makes it fail.
  No "this might" without a scenario.
- Label each finding **Important**, **Nit**, or **Pre-existing**.
  Pre-existing means the PR did not introduce it; it is not the author's
  to fix in this PR unless trivial.
- At most 5 Nits. Style-only and cleanup-only findings are Nits.
- The summary starts with a count ("3 Important, 1 Nit") and says
  plainly when nothing needs fixing.
- Do not repeat a finding already posted on the PR. After the first full
  pass, re-review the new diff only.
- After three review-and-fix rounds, stop patching and ask whether the
  approach is wrong.

## Risk tiers

The tier follows the paths touched, not the author.

| Tier | Paths | Before merge |
|------|-------|--------------|
| **Low** | `docs/`, `examples/`, `README.md`, `CHANGELOG.md`, UI copy, an additive chart value that defaults off | Green PR CI. |
| **Medium** | Everything else | Green PR CI, one local `/code-review` pass, Codex review, maintainer reads the diff. |
| **High** | Any path in [`.github/CODEOWNERS`](.github/CODEOWNERS), plus `pkg/k8s/api/v1/`, `charts/*/crds/`, `charts/datuplet-app/files/fga/`, NetworkPolicy templates | Medium, plus `/code-review max`, `/security-review` for auth and token paths, `make lint`, and `make e2e-k8s` on OrbStack. |

A PR that mixes tiers is reviewed at the highest one. Split it if that
is disproportionate.
