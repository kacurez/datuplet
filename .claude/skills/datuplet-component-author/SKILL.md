---
name: datuplet-component-author
description: >-
  Author a NEW Datuplet component (extractor/source, transform, or writer/sink)
  when no existing built-in can do the job — scaffold the component program
  against the thin Datuplet SDK, write its Form-Subset config schema, containerize
  it, register it as a ComponentDefinition, sync the schema into the chart, build
  and publish the image, and prove it in a pipeline. Use this skill whenever the
  user (or the datuplet-operator skill) determines a data source, transformation,
  or destination isn't covered by the existing component catalog and a new
  component must be built; whenever someone asks how to add, create, write, or
  extend a Datuplet component, connector, extractor, or writer; or when
  onboarding a new source/sink to the Datuplet platform.
---

# Authoring a Datuplet component

A **component** is a small containerized program that does one job in a pipeline
stage — pull from a source, transform, or write to a sink. It never touches
object storage directly: it reads inputs and writes outputs through the **Data
Gateway sidecar** via a thin SDK (~200–300 LOC per component), so the platform
owns all the storage, format, and credential concerns. Your job is to write that
small program, describe its config, register it, and prove it runs.

You reach for this skill when the `datuplet-operator` loop hits a wall:
`datuplet components list` has nothing that can read the source (or do the
transform / write the sink) the goal needs. **Don't hack around a missing
component** (e.g. abusing `sql-transform` to fetch HTTP, or asking the user to
pre-load data by hand) — the right move is to build the component. It's a
bounded, well-patterned task.

## Before you build: confirm it's really needed

1. `datuplet components list --json` — is there truly nothing? Check adjacent
   components: `http-json-extractor` already handles most REST/JSON sources
   (incl. pagination); `sql-transform` covers most transforms/joins/aggregations
   via DuckDB SQL. A new component is warranted for a genuinely different
   protocol/auth/format (a database, a SOAP/gRPC API, a binary format, a
   bespoke SaaS API) or a transform SQL can't express.
2. State the case to the user: what source/sink, why existing components don't
   fit, and the shape of the new component (its `io`, its config). Get a nod
   before writing code — a new component is real, reviewable surface area.

## The authoring workflow

Copy the closest existing component as your starting point — they are the
canonical templates:

- **Source** (pull data in, no inputs): `components/http-json-extractor/`
- **Transform** (read tables → write tables): `components/sql-transform/`
- **Sink** (read tables → external, no storage output): `components/stdout-writer/`

Then, in order:

1. **Scaffold** `components/<name>/` — program file (`main.go` or Python), `go.mod`
   (Go; keep the `replace` directive to the in-repo SDK), `Dockerfile`,
   `schema.json`. See `references/build-a-component.md`.
2. **Implement** against the SDK: parse config, read inputs and/or write outputs
   through the gateway, and signal status via the exit-code contract. The SDK
   contract and a minimal skeleton are in `references/build-a-component.md`.
3. **Write `components/<name>/schema.json`** — the config contract, in the
   **Form Subset** of JSON Schema (so the UI can render it as a form) with the
   `x-datuplet-*` annotations. Rules + the linter in
   `references/register-a-component.md`.
4. **Register** it as a ComponentDefinition CR in the chart and declare its `io`
   capability; **sync** the schema into the chart. See
   `references/register-a-component.md`.
5. **Build + publish** the image, then **exercise** it: author a pipeline that
   uses the component, `datuplet pipeline validate`, `put`, `trigger --wait`, and
   verify the output (this is the `datuplet-operator` loop — that skill covers
   it). A component isn't done until it's run green in a pipeline on K8s.

## Definition of done (checklist)

- [ ] `components/<name>/` has the program, `schema.json`, `Dockerfile` (non-root,
      multi-stage), and (Go) `go.mod`/`go.sum`.
- [ ] The program parses config via the SDK, does its one job through gateway
      reads/writes, and uses the exit-code + `DUPLET_STATUS_MESSAGE:` contract.
- [ ] `schema.json` passes the Form-Subset lint (`go test ./pkg/pipeline/schemalint/...`
      or the CI drift check) — every property typed + described, no forbidden
      keywords, only known `x-datuplet-*` annotations.
- [ ] A `ComponentDefinition` template exists at
      `charts/datuplet-app/templates/components/<name>.yaml` with the right `io`,
      a **semver** version (`vX.Y.Z`), the image ref, and the synced `configSchema`.
- [ ] `make sync-component-schemas` run (schema copied into
      `charts/datuplet-app/files/component-schemas/<name>.json`); no CI drift.
- [ ] Image builds (`make build-components` or the component's Dockerfile) and is
      published to the registry the chart's `components.registry`/`tag` point at.
- [ ] Exercised end-to-end: a pipeline referencing the component runs `Succeeded`
      and produces the expected data.

## Conventions that aren't obvious

- **Exit-code contract:** `0` = Succeeded, `1` = FailedUser (bad config/input —
  the user's fault), `>=20` = FailedApplication (infra/your-bug). Emit a
  human-readable reason with the `DUPLET_STATUS_MESSAGE:` stdout prefix (Python:
  `sdk/python/status.py` has `fail_user`/`fail_application`; Go: print the prefix
  then `os.Exit(code)`). Getting this right is how runs surface the correct
  phase and message to the operator.
- **Stay credentials-clean when you can.** Components read/write through the
  gateway and should not hold storage/catalog credentials. `sql-transform` is the
  model: no S3/GCS/Lakekeeper creds ever touch it. Only hold a credential when
  the *external* system requires one (e.g. an API key) — and take it as a
  `x-datuplet-secret` config field resolved from `$[key]`, never bake it in.
- **Pod hardening is automatic** — the platform runs every component with dropped
  Linux capabilities, `seccompProfile: RuntimeDefault`, and no service-account
  token. Your Dockerfile should still run as a **non-root** user (see the
  template Dockerfiles).
- **`io` is the component's contract with the planner.** Declare it honestly:
  `inputs: none` for a source, `inputs: required`/`outputs: required` for a
  transform, `outputs: none` for a sink. The validator and UI gate on it.
- **Versions are semver.** A registered stable version must be `vMAJOR.MINOR.PATCH`
  (the controller marks the definition Invalid otherwise); use `prerelease: true`
  + a mutable tag like `dev` only for iteration.

## References

- `references/build-a-component.md` — the `components/<name>/` anatomy, the SDK
  read/write/config/status contract with a minimal skeleton, and the Dockerfile
  pattern.
- `references/register-a-component.md` — the `schema.json` Form-Subset rules and
  `x-datuplet-*` annotations, the `ComponentDefinition` CR shape, `io`,
  versioning, `make sync-component-schemas`, and how to build + exercise it.
- Repo docs: `docs/components.md` is the authoritative schema-authoring guide;
  the linter lives in `pkg/pipeline/schemalint/`.
