# Config schema, registration, build, and proof

## 1. `components/<name>/schema.json` — the config contract

A JSON Schema (draft 2020-12) describing the component's `config` block. It must
stay inside the **Form Subset** so the UI renders it as a form (never a raw JSON
editor). The linter `pkg/pipeline/schemalint` enforces these rules — run
`go test ./pkg/pipeline/schemalint/...` after writing it, and see
`docs/components.md` for the authoritative guide.

Rules:

- **Every property has a `type` and a `description`.** Descriptions aren't
  optional polish — they're the form labels/help text and the agent's guidance.
- **No composition/indirection keywords.** These 11 are forbidden anywhere in
  the schema: `oneOf`, `anyOf`, `allOf`, `not`, `$ref`, `$defs`, `if`, `then`,
  `else`, `patternProperties`, `const`. Model config as plain typed
  properties/objects/arrays instead.
- **No `required` + `default` on the same field** (contradictory: required means
  the user must supply it; a default means they needn't).
- **Only the known `x-datuplet-*` annotations:**
  - `x-datuplet-secret: true` — the value is a credential; the UI shows a secret
    picker and the user supplies `$[key]`. Use this for API keys/tokens.
  - `x-datuplet-multiline: true` — render a textarea (e.g. a SQL body).
  - `x-datuplet-advanced: true` — hide behind an "advanced" disclosure.
  - `x-datuplet-doc: "<url>"` — link to external docs.
  - `x-datuplet-produces: "<path-expr>"` — (schema root only) declares which
    config path names the output table(s), so the UI can offer them downstream.

Minimal example:

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "additionalProperties": false,
  "required": ["url"],
  "properties": {
    "url":    { "type": "string", "description": "HTTP(S) endpoint to fetch JSON from." },
    "apiKey": { "type": "string", "description": "Bearer token for the API.", "x-datuplet-secret": true },
    "table_name": { "type": "string", "description": "Output table name.", "default": "data" }
  }
}
```

Keep it aligned with the struct your `ParseConfig` decodes into — the schema is
what validates the user's config before a run, so a field the program reads must
appear here, and vice versa.

## 2. Register a ComponentDefinition

Add `charts/datuplet-app/templates/components/<name>.yaml`. Copy an existing one
(e.g. `http-json-extractor.yaml`) and change the name, description, `io`, and
schema file reference. The shape:

```yaml
{{- if .Values.components.builtins }}
---
apiVersion: datuplet.io/v1
kind: ComponentDefinition
metadata:
  name: <name>                    # the registry reference pipelines use
  labels:
    {{- include "datuplet-app.labels" . | nindent 4 }}
    app.kubernetes.io/name: <name>
spec:
  displayName: <Human Name>
  description: <one line — what it does>
  maintainer: datuplet
  io:
    inputs: none | optional | required      # see below
    outputs: none | optional | required
  defaultVersion: {{ .Values.components.tag }}
  versions:
    - version: {{ .Values.components.tag }}  # a SEMVER vX.Y.Z for stable versions
      image: "{{ .Values.components.registry }}/<name>:{{ .Values.components.tag }}"
      configSchema: |
{{ .Files.Get (printf "files/component-schemas/%s.json" "<name>") | indent 8 }}
      resources:
        default:
          requests: { cpu: 100m, memory: 128Mi }
          limits:   { cpu: 500m, memory: 512Mi }
        max:
          cpu: "2"
          memory: 2Gi
          ephemeral-storage: 10Gi
{{- end }}
```

- **`io` must match reality:** a source is `inputs: none` / `outputs: required`;
  a transform is `inputs: required` / `outputs: required`; a sink is
  `inputs: required` / `outputs: none`. The validator and UI gate stages on this.
- **Versioning:** a registered *stable* version must be `vMAJOR.MINOR.PATCH` —
  the componentdefinition controller marks the whole definition **Invalid**
  otherwise (and then no pipeline can use it). For iteration, add a version with
  `prerelease: true` and a mutable tag like `dev`. Built-ins ride the chart's
  `components.tag` (kept in lockstep by `make bump-version`); don't hardcode a
  tag.
- **`configSchema` is not authored here** — it's pulled from the synced file
  (next step). `components/<name>/schema.json` is the single source of truth.

## 3. Sync the schema into the chart

CI enforces that the chart's copy matches `components/<name>/schema.json`. After
any schema edit:

```bash
make sync-component-schemas   # copies components/*/schema.json → charts/datuplet-app/files/component-schemas/<name>.json
```

Commit both the source `schema.json` and the synced copy; a drift fails CI.

## 4. Build and publish the image

```bash
make build-components          # builds every component image (incl. yours)
# or just yours:
docker build -t <registry>/<name>:<tag> -f components/<name>/Dockerfile .
```

The image must be published to the registry the chart's `components.registry` +
`components.tag` resolve to (production: `ghcr.io/kacurez` + the release version;
local OrbStack dev: `datuplet/<name>:<tag>` via `make build-components-local`,
pulled with `IfNotPresent`). The image tag and the ComponentDefinition `version`
must line up so resolution finds it.

## 5. Prove it in a pipeline

A component isn't done until it's run green. Hand off to the `datuplet-operator`
loop:

1. `datuplet components get <name> --schema` — confirm it registered and the
   schema is what you expect.
2. Author a small pipeline that uses `<name>`, `datuplet pipeline validate -f`,
   fix findings, `put`.
3. `datuplet trigger --wait --json <pipeline>` → expect `Succeeded`.
4. Verify the output data (`datuplet storage sample …` / `datuplet query --sql …`).

For a repeatable guard, add an e2e scenario under `tests/e2e/` following the
existing component scenarios (a fixture pipeline + assertions on the output).

## Deployment note

Datuplet is K8s-only: a new component is "done" when its ComponentDefinition is
in the chart, its schema is synced, its image is published, and it has run
`Succeeded` in a pipeline on a cluster — not when it merely compiles locally.
