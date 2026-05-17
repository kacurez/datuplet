// Secrets placeholder. The pipeline-api has no secrets endpoints yet;
// see docs/secrets.md for the resolution model. Today secrets are
// managed via `kubectl create secret` in the project's K8s namespace
// (datuplet-<project-uuid>) and referenced from a pipeline YAML via
// spec.secretsRef.name + $[key] substitutions.
//
// This page renders the relevant commands with the user's project ID
// filled in so they can copy-paste without looking it up. RFC 005 B-8
// restyles it with the new component vocabulary; no behaviour change.

import { esc } from '/ui/api.js';

export function renderSecrets() {
  const app = document.getElementById('app');
  const head = document.getElementById('page-head');
  const pid = window.__datupletActiveProjectID || '<project-uuid>';
  const ns = `datuplet-${pid}`;
  const yaml = `apiVersion: datuplet.io/v1
kind: Pipeline
metadata:
  name: my-pipeline
spec:
  secretsRef:
    name: my-creds
  stages:
    - name: extract
      components:
        - name: c1
          image: datuplet/csv-extractor:latest
          config:
            db_password: $[db_password]
            api_token: $[api_token]`;

  if (head) {
    head.innerHTML = `<h1>Secrets</h1>`;
  }

  app.innerHTML = `
    <p>A user-facing secrets API is not shipped yet. Until it lands, an
    admin manages project secrets via <code class="inline">kubectl</code>
    in the project's Kubernetes namespace.</p>

    <h3 style="margin-top: var(--s-5); margin-bottom: var(--s-2); font-size: var(--text-lg);">Create or update secrets for this project</h3>
    <pre class="code">kubectl create secret generic my-creds \\
  -n ${esc(ns)} \\
  --from-literal=db_password='...' \\
  --from-literal=api_token='...' \\
  --dry-run=client -o yaml | kubectl apply -f -</pre>

    <h3 style="margin-top: var(--s-5); margin-bottom: var(--s-2); font-size: var(--text-lg);">Reference them from a pipeline YAML</h3>
    <pre class="code">${esc(yaml)}</pre>

    <p style="margin-top: var(--s-4); color: var(--fg-1); font-size: var(--text-sm);">
      The gateway sidecar mounts <code class="inline">secretsRef.name</code> at
      <code class="inline">/var/run/secrets/datuplet/</code> and resolves
      <code class="inline">$[key]</code> refs at boot. Values reach the component
      as part of its config only — no secret is ever visible to the component's
      filesystem. See <code class="inline">docs/secrets.md</code>.
    </p>
  `;
}
