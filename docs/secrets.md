# Secrets

Pipeline configs can reference secret values without embedding them in YAML.
The DataGateway sidecar resolves every reference at boot and delivers a
plain, resolved config to the component. Components never see the marker,
and the Docker/Kubernetes paths are identical at the pipeline-YAML level.

## Syntax

Use `$[name]` as a **whole scalar** inside `spec.stages[*].components[*].config`:

```yaml
config:
  password: $[db_password]
  api_key:  $[api_token]
```

Rules:

- Whole-value only. `url: "postgres://user:$[pw]@host"` is rejected at parse time.
- Names match `[A-Za-z0-9_-]+`.
- To write a literal `$[x]`, escape it as `$$[x]`.
- Multiple refs in one scalar (`"$[a] $[b]"`) are rejected.
- Only `component.config` is scanned; other fields (`image`, `inputs`, …) never carry secrets.

Syntax errors are caught at pipeline parse / CRD admission time with a
path-aware message, e.g. `stages[0].components[0].config.password`.

## Docker

Put one file per secret in a directory. The file name is the `$[name]`;
the file contents are the value. A single trailing `\n` or `\r\n` is
stripped; other whitespace is preserved.

```bash
mkdir -p ./secrets
printf '%s' 'hunter2'    > ./secrets/db_password
printf '%s' 'abc-123'    > ./secrets/api_token
chmod 400 ./secrets/*

./bin/datuplet run my-pipeline.yaml --secrets-dir ./secrets
```

The CLI resolves the path to absolute, verifies the directory exists, and
bind-mounts it read-only at `/var/run/secrets/datuplet` on the gateway
sidecar — never on the component container.

**Fail-fast checks (in order)**:

1. **Parser** — rejects malformed `$[...]` markers.
2. **Runner** — if any `$[...]` is referenced and `--secrets-dir` was not
   provided, exits before any container starts and lists the missing names.
3. **Gateway boot** — if a referenced secret's file is missing or
   unreadable, the gateway crashes with a clear message and the pipeline
   fails with `FailedApplication`.

### Makefile example

The common pattern: read a secret from an env var, materialise it as a file,
pass the directory to `datuplet run --secrets-dir`.

```makefile
run-my-pipeline: build
	@if [ -z "$(MY_API_KEY)" ]; then \
		echo "Error: MY_API_KEY is not set." >&2; exit 1; \
	fi
	@mkdir -p $(PWD)/tmp/secrets
	@printf '%s' "$(MY_API_KEY)" > $(PWD)/tmp/secrets/my_api_key
	@chmod 400 $(PWD)/tmp/secrets/my_api_key
	./bin/datuplet run my-pipeline.yaml \
		--secrets-dir $(PWD)/tmp/secrets
```

## Kubernetes

Add `spec.secretsRef.name` to your `Pipeline` and create a matching
Kubernetes `Secret` in the same namespace. The keys of the Secret become
your `$[name]` references.

```yaml
apiVersion: datuplet.io/v1
kind: Pipeline
metadata:
  name: my-pipeline
  namespace: datuplet
spec:
  secretsRef:
    name: my-pipeline-secrets
  stages:
    - name: extract
      components:
        - name: extractor
          image: my-registry/my-extractor:latest
          config:
            api_key: $[api_token]
          outputs:
            defaultBucket: raw
```

```bash
kubectl create secret generic my-pipeline-secrets \
  --from-literal=api_token=abc-123 \
  -n datuplet
```

A sample manifest lives at
[`utils/deploy/k8s/rbac/sample-pipeline-secret.yaml`](../utils/deploy/k8s/rbac/sample-pipeline-secret.yaml).

### What the operator does

When `secretsRef` is set, the PipelineRun Pod is built with:

- a `Secret` volume backed by `secretsRef.name` (`Optional: false`), mounted
  at `/var/run/secrets/datuplet` on the **gateway sidecar only**;
- `DefaultMode: 0440` + pod `fsGroup: 65532`, gateway running as non-root
  (`RunAsUser/RunAsGroup: 65532`) so only the gateway process can read the files;
- `AutomountServiceAccountToken: false` — nothing in the Pod needs the K8s
  API, so the SA token is stripped to remove an unused exfiltration path.

### Observing resolution status

The PipelineRun's `status.conditions` reports a `SecretsResolved` condition:

| Status | Reason              | Meaning                                                     |
|--------|---------------------|-------------------------------------------------------------|
| True   | `Resolved`          | Gateway booted and served its first `GetConfig`.            |
| False  | `SecretsRefMissing` | `FailedMount` on the `datuplet-secrets` volume — the Secret object does not exist or `secretsRef.name` is wrong. |
| False  | `SecretNotFound`    | Gateway crashed at boot; a key referenced by `$[name]` is not present in the mounted Secret. |

```bash
kubectl get pipelinerun <name> -n datuplet \
  -o jsonpath='{.status.conditions}' | jq
```

## Limits (v1)

No mid-string substitution, no external providers (Vault, SOPS, cloud
secret managers), no rotation/reload (gateway reads once at boot), no
structured/JSON values.
