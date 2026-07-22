---
name: datuplet-operator
description: >-
  Operate the Datuplet streaming-ETL platform end-to-end from the `datuplet`
  CLI to solve real data problems — discover components, author an envelope-free
  PipelineDoc, validate and fix it against the component schemas, save it,
  trigger a run, and observe and verify the result. Use this skill whenever the
  user wants to build, configure, run, debug, or monitor a Datuplet pipeline;
  move, ingest, transform, aggregate, join, or summarize data with Datuplet;
  pull an external API or source into a table; or asks about Datuplet
  components, runs, pipelines, storage, or the `datuplet` command — even if they
  don't say the word "pipeline" explicitly.
---

# Operating Datuplet

Datuplet is a streaming ETL platform. You describe work as a **PipelineDoc** —
an envelope-free YAML/JSON document — hand it to the server, and it runs each
stage on Kubernetes, reading and writing Apache Iceberg tables on object
storage. Your job as the operator is to turn a business goal ("pull our orders
API into a table and produce a daily revenue summary") into a validated,
running pipeline and confirm it actually produced the right data.

You do all of this through the `datuplet` CLI, which is built for exactly this:
every step is scriptable, every schema is discoverable, and the validator gives
you machine-readable findings so you can fix your own mistakes without a human
in the loop.

## The golden rule

**Never guess a component's config. Discover the schema, conform to it, and
validate before you save.** The platform is designed so you can be sure a
pipeline is well-formed *before* you ever run it — use that. The most common
failure mode for an agent here is authoring config from memory and skipping
validation; the second is not confirming the run actually succeeded. This skill
exists to keep you from both.

## Prerequisites: authentication

Every command below talks to pipeline-api and needs a bearer token, a remote
URL, and a project. Resolve them once at the start (precedence for each:
explicit flag > env var > `~/.datuplet/`):

- **Logged in already?** If `datuplet login --remote <url>` was run, the CLI
  reads `~/.datuplet/` automatically and you need no flags.
- **Headless (the usual agent case):** export three env vars and skip login
  entirely —
  - `DATUPLET_REMOTE` — the pipeline-api URL
  - `DATUPLET_API_TOKEN` — the pipeline-api bearer token (a `cli-api` JWT; it's
    the `api_token` a `login` produces)
  - `DATUPLET_PROJECT` — the **project UUID** (on the headless path this is used
    directly as the project id; there is no name→id lookup without `~/.datuplet`)

If a command returns `401`, the token is missing/expired; if it returns
`404 project not found`, `DATUPLET_PROJECT` is wrong. Do not print the token.

## The operator loop

Follow these steps in order. Each is a real `datuplet` subcommand; run them,
read their output, and react to it.

### 1. Understand the goal, then discover what's available

List the component catalog and read the schema of each component you intend to
use. The schema is the source of truth for the `config` block — field names,
types, which are required, and which are secrets.

```bash
datuplet components list --json
datuplet components get <component-name> --schema
```

`--schema` prints the JSON Schema for that component's `config`. Honor it:
supply every `required` field, match types, and for any field marked
`x-datuplet-secret` pass a secret reference (see guardrails), never a literal.

See `references/components.md` for the built-in components and what they're for.

### 2. Author the PipelineDoc

Write an envelope-free doc: top-level `name`, optional `gateway`, and `stages`.
Each stage runs its components; a later stage's `inputs` read tables an earlier
stage wrote. Full field reference and rules: `references/pipeline-doc-format.md`.
Worked, copy-adaptable business scenarios (HTTP ingest, SQL aggregation,
multi-source join): `references/scenarios.md` — start there when the goal
resembles one of them.

### 3. Validate — and fix your own findings

This is the tight loop that lets you get it right before running anything:

```bash
datuplet pipeline validate -f pipeline.yaml --json
```

Exit code `0` = clean. Exit `1` = there are `error`-severity findings; the JSON
is `{"findings":[{"path","message","severity"}]}`. Read each finding — `path`
points at the exact offending field (e.g. `stages[0].components[0].config.url`)
— fix the doc, and re-validate. Repeat until it exits `0`. `warning` findings
don't block a run but are worth reading (e.g. a missing secret key). Treat a
transport/HTTP failure (exit ≥ 2) as an environment problem, not a bad pipeline.

### 4. Save it

```bash
datuplet pipeline put -f pipeline.yaml
```

The pipeline name comes from the doc's top-level `name`; `put` is create-or-
replace (last write wins).

### 5. Trigger a run and wait

```bash
datuplet trigger --wait --json --timeout 15m <pipeline-name>
```

Flags MUST come before the positional pipeline name — the CLI's flag parser
stops at the first non-flag argument, so `trigger <name> --wait` would silently
ignore `--wait` and return before the run finishes. `--wait` blocks until the
run is terminal and exits non-zero if it didn't
succeed; `--json` gives you `{"id","phase","message",...}`. Terminal phases:
`Succeeded`, `FailedUser` (your config/data — e.g. a missing secret, bad SQL),
`FailedApplication` (platform/infra), `Cancelled`, `Expired`. On `FailedUser`,
the `message` usually tells you what to fix; go back to step 2.

### 6. Observe the run

```bash
datuplet runs get <run-id> --json          # detail + per-stage timeline
datuplet runs list --pipeline <name>        # history for one pipeline
datuplet runs list --phase Running          # what's in flight
```

Use `runs get` to see which stage failed and why; `runs list --pipeline` to
review history (it pages — a next-cursor hint prints when there's more).

### 7. Verify the output data

A run that says `Succeeded` isn't done until you've confirmed it produced the
right data. Inspect the tables it wrote:

```bash
datuplet storage tables                                  # what tables exist
datuplet storage sample <bucket>.<table>                 # peek at rows
datuplet query --sql 'SELECT count(*) FROM "<bucket>"."<table>"'   # ad-hoc SQL (SQL via --sql/-f/stdin, not positional)
```

Check row counts and shape against the goal. If the numbers are wrong, the fix
is almost always in the transform SQL or the input selection — back to step 2.

## When no existing component fits

Sometimes the goal needs a data source, transform, or destination that **no
catalog component can do** — a database, a SOAP/gRPC API, a proprietary format, a
transform SQL can't express. When you hit this, first make sure it's real:
re-check `datuplet components list --json` and the near-matches (`http-json-
extractor` handles most REST/JSON sources incl. pagination; `sql-transform`
covers most transforms/joins/aggregations).

If nothing genuinely fits, **do not hack around it** — don't abuse
`sql-transform` to make network calls, don't ask the user to hand-load data,
don't silently drop the requirement. The correct move is to **propose creating a
new component** and say so plainly: name the missing capability, why the existing
components don't cover it, and the shape of the component you'd build (its `io`
and config). Then hand off to the **`datuplet-component-author`** skill, which
covers building, registering, and proving a new component. Building a component
is a bounded, well-patterned task — treat a missing connector as "build the
connector," not "give up" or "improvise."

## Guardrails and principles

- **Validate before you save; confirm `Succeeded` after you run.** These two
  checks catch the vast majority of problems and cost seconds.
- **Secrets are references, never literals.** For any config field the schema
  marks `x-datuplet-secret`, write `$[key_name]` (e.g. `token: $[api_token]`).
  The real value lives in the project secret store; a missing key surfaces as a
  warning at validate and a `FailedUser` at run — set it (or ask the user to)
  rather than inlining the secret.
- **Prefer `--json` for anything you'll parse.** `components get`, `validate`,
  `trigger`, `runs` all support it. Human tables are for the user; JSON is for
  you.
- **Legacy format is rejected.** A PipelineDoc has NO `apiVersion`, `kind`,
  `metadata`, or `spec` — those are the old Kubernetes-CR envelope and the
  server rejects them with a pointed finding. Write top-level fields only.
- **Idempotency / safety.** `put` overwrites the same-named pipeline. Triggering
  starts real work that reads/writes storage. When acting for a user on shared
  data, say what you're about to run before you run it.
- **When stuck, re-discover.** If validation keeps failing on a `config` field,
  re-read `components get <name> --schema` — you're likely conforming to a
  remembered shape, not the deployed one.

## References

- `references/pipeline-doc-format.md` — the PipelineDoc shape: `name`,
  `gateway`, `stages[].components[]`, `inputs`, `outputs`, write modes,
  cross-stage table wiring, secret refs.
- `references/components.md` — the built-in components, what each is for, and
  how to read their schemas.
- `references/scenarios.md` — three worked business problems end-to-end
  (HTTP-JSON ingest, SQL daily-summary aggregation, multi-source join), each a
  real, validated doc you can adapt.
