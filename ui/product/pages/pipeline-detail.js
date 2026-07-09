// Pipeline detail: view + edit the stored YAML, or create a new one.
//
// Route params:
//   /ui/pipelines/_new        → empty editor; name field is editable.
//                                Underscore is invalid in DNS-1123 so
//                                this sentinel can never collide with
//                                a real pipeline name.
//   /ui/pipelines/:name       → loaded editor; name field is locked
//                                (server enforces YAML metadata.name
//                                == URL name; changing the name here
//                                means creating a new pipeline)
//
// PUT is idempotent — the same endpoint both creates and replaces.
// Delete is a separate button with a confirm prompt.

import { api, putPipelineYAML, esc, getComponents, getComponent, listSecrets, getStorageCatalog } from '/ui/api.js';
import { timeTag, phaseToPillClass, formatDuration, durationFrom } from '/ui/format.js';
import { renderFindings } from '/ui/lib/findings.js';
import { buildSchemaForm } from '/ui/lib/schema-form.js';
import { schemaDocs } from '/ui/pages/components.js';

const STARTER_YAML = `apiVersion: datuplet.io/v1
kind: Pipeline
metadata:
  name: my-pipeline
spec:
  stages:
    - name: extract
      components:
        - name: c1
          component: http-json-extractor
          config:
            url: "https://api.example.com/items"
          outputs:
            defaultBucket: raw
            defaultWriteMode: FULL_LOAD
`;

export async function renderPipelineDetail(ctx) {
  const app = document.getElementById('app');
  const head = document.getElementById('page-head');
  const pid = window.__datupletActiveProjectID;
  if (!pid) {
    if (head) head.innerHTML = '';
    app.innerHTML = `<p>No active project.</p>`;
    return;
  }
  const name = ctx.params[0];
  const isNew = name === '_new';

  // Abort-on-navigation: bail out of any post-await DOM write if the user
  // has navigated elsewhere while a fetch was in flight.
  const path = window.location.pathname;
  const aborted = () => window.location.pathname !== path;

  let yaml = STARTER_YAML;
  let updatedAt = '';
  let pipelineID = '';
  if (!isNew) {
    const pipe = await api(`/api/v1/projects/${encodeURIComponent(pid)}/pipelines/${encodeURIComponent(name)}`);
    if (aborted()) return;
    yaml = pipe.yaml;
    updatedAt = pipe.updated_at;
    pipelineID = pipe.id;
  }

  if (head) {
    const titleHTML = isNew
      ? `<h1>New pipeline</h1>`
      : `<h1><code class="inline">${esc(name)}</code></h1>`;
    const actions = isNew
      ? ''
      : `
        <a class="btn btn--primary" href="/ui/pipelines/${encodeURIComponent(name)}/trigger">Trigger run</a>
        <button type="button" id="delete-btn" class="btn btn--secondary">Delete</button>
      `;
    head.innerHTML = `
      ${titleHTML}
      <div class="actions">${actions}</div>
    `;
  }

  app.innerHTML = `
    ${updatedAt ? `<p style="color:var(--fg-2);"><small>Updated ${timeTag(updatedAt)}</small></p>` : ''}
    <div class="builder-layout">
      <div>
        <div class="builder-toolbar">
          <label class="field builder-add">Add component
            <select class="input" id="add-component"><option value="">Loading…</option></select>
          </label>
          <button type="button" class="btn btn--secondary" id="insert-component" disabled>Insert snippet</button>
        </div>
        <form id="pipeline-form">
          ${isNew ? `
            <label class="field">Name
              <input class="input" type="text" name="name" placeholder="my-pipeline" required
                pattern="[a-z0-9]([-a-z0-9.]*[a-z0-9])?"
                title="Lowercase DNS-1123 subdomain; must match metadata.name in the YAML.">
            </label>` : ''}
          <label class="field">YAML
            <textarea class="textarea input--mono" name="yaml" spellcheck="false" required>${esc(yaml)}</textarea>
          </label>
          <div id="pipeline-msg"></div>
          <div class="actions" style="margin-top:var(--s-3);">
            <button type="submit" class="btn btn--primary">${isNew ? 'Create' : 'Save'}</button>
          </div>
        </form>
      </div>
      <aside class="builder-docs" id="builder-docs">
        <p style="color:var(--fg-2);">Pick a component to see its config schema.</p>
      </aside>
    </div>
    ${isNew ? '' : `
    <section style="margin-top:var(--s-4);">
      <h2>Recent runs</h2>
      <div id="pipeline-runs"><p><small>Loading…</small></p></div>
    </section>`}
  `;

  const form = document.getElementById('pipeline-form');
  form.addEventListener('submit', async (e) => {
    e.preventDefault();
    const msg = document.getElementById('pipeline-msg');
    msg.innerHTML = '';
    const btn = form.querySelector('button[type=submit]');
    btn.disabled = true;
    try {
      // Read form inputs via FormData rather than `form.name` — on
      // HTMLFormElement, `.name` is the form's own (empty) name
      // attribute, not the <input name="name"> child, so accessing
      // .value would throw. FormData avoids that shadowing and also
      // normalizes string conversion.
      const fd = new FormData(form);
      const targetName = isNew ? String(fd.get('name') || '').trim() : name;
      if (!targetName) throw new Error('Name is required');
      const yamlText = String(fd.get('yaml') || '');
      const res = await putPipelineYAML(pid, targetName, yamlText);
      if (res.ok && (!res.findings || res.findings.length === 0)) {
        // 204 — clean save.
        msg.innerHTML = `<div class="callout">Saved.</div>`;
      } else if (res.ok) {
        // 200 — saved with warnings.
        msg.innerHTML = `<div class="callout">Saved with warnings.</div>` + renderFindings(res.findings);
      } else {
        // 400 — rejected; findings inline, nothing saved.
        msg.innerHTML = renderFindings(res.findings);
      }
      if (res.ok && isNew) {
        // For a fresh create, jump to the detail view for that name.
        window.history.replaceState({}, '', `/ui/pipelines/${encodeURIComponent(targetName)}`);
        if (typeof window.renderRoute === 'function') window.renderRoute();
      }
    } catch (err) {
      if (String(err.message) !== 'not authenticated') {
        msg.innerHTML = `<div class="callout callout--warn">${esc(err.message)}</div>`;
      }
    } finally {
      btn.disabled = false;
    }
  });

  if (!isNew) {
    const delBtn = document.getElementById('delete-btn');
    if (delBtn) {
      delBtn.addEventListener('click', async () => {
        if (!confirm(`Delete pipeline "${name}"? Runs that reference it by ID stay in history, but you won't be able to trigger new ones.`)) return;
        try {
          await api(`/api/v1/projects/${encodeURIComponent(pid)}/pipelines/${encodeURIComponent(name)}`, { method: 'DELETE' });
          window.history.replaceState({}, '', '/ui/pipelines');
          if (typeof window.renderRoute === 'function') window.renderRoute();
        } catch (err) {
          if (String(err.message) !== 'not authenticated') {
            document.getElementById('pipeline-msg').innerHTML = `<div class="callout callout--warn">${esc(err.message)}</div>`;
          }
        }
      });
    }
  }

  if (!isNew && pipelineID) {
    // Best-effort: a slow or failing runs fetch must not block the editor
    // above, which has already been painted and is usable.
    loadRecentRuns(pid, pipelineID).catch((err) => {
      if (String(err.message) === 'not authenticated') return;
      const container = document.getElementById('pipeline-runs');
      if (container) container.innerHTML = `<p><small>Couldn't load recent runs: ${esc(err.message)}</small></p>`;
    });
  }

  // ---- Builder: catalog picker + docs/form panel (RFC 026 Phase 4) --------
  // Fetch the component catalog once, populate the "Add component" select,
  // and wire selection → schema docs + a schema-driven config form / insert →
  // snippet at the cursor. The textarea remains the source of truth — the
  // form is a one-way builder that EMITS a component block into the textarea;
  // it never parses the textarea back (two-way sync is an explicit non-goal,
  // §4.7).
  const sel = document.getElementById('add-component');
  const insertBtn = document.getElementById('insert-component');
  const docs = document.getElementById('builder-docs');

  // Whoami + secret keys, fetched once at page load (both optional). The
  // secret keys populate the form's $[...] secret pickers. `isSuperadmin`
  // is computed for a future resources affordance only: RESOURCE HIDING
  // (§4.4) — `resources` is a SIBLING of `config` in the component spec, not
  // a property of the config JSON Schema, so the schema form never renders a
  // resources control and componentBlockYAML never emits a `resources` key.
  // Resources stay YAML-only and are superadmin-gated server-side (diff-gate).
  // On any /auth/me failure we treat the session as NOT superadmin.
  let me = null;
  let secretKeys = [];
  try { me = await api('/api/v1/auth/me'); if (aborted()) return; } catch { /* swallow */ }
  try {
    const s = await listSecrets(pid);
    if (aborted()) return;
    secretKeys = (s || []).map((x) => x.key);
  } catch { /* secrets optional */ }
  const isSuperadmin = !!(me && me.is_superadmin); // T1-verified key name

  // Storage catalog (RFC 005) for the inputs picker — existing tables the
  // component can read. Optional: on any failure we degrade to an empty
  // catalog (the inputs picker then shows a usable empty state, no crash).
  // Grouped by namespace up front (namespace == bucket in Datuplet's model),
  // mirroring query.js's buildSchemaPane idiom.
  let storageTables = [];
  try {
    const sc = await getStorageCatalog(pid);
    if (aborted()) return;
    storageTables = (sc && sc.tables) || [];
  } catch { /* catalog optional */ }
  const storageByNs = new Map();
  for (const t of storageTables) {
    if (!storageByNs.has(t.namespace)) storageByNs.set(t.namespace, []);
    storageByNs.get(t.namespace).push(t);
  }

  let comps = [];
  try {
    comps = await getComponents();
  } catch (e) {
    // Swallow the centralized 'not authenticated' redirect; on any other
    // fetch error just leave the dropdown empty (builder stays inert).
  }
  if (aborted()) return;
  sel.innerHTML = `<option value="">— choose a component —</option>` +
    comps.map((c) => `<option value="${esc(c.name)}">${esc(c.displayName || c.name)}${c.deprecated ? ' (deprecated)' : ''}</option>`).join('');

  // Current live schema-form handle for the picked component (rebuilt on each
  // pick, torn down before the next). Used by the Insert/Edit-as-YAML buttons.
  let formHandle = null;

  sel.addEventListener('change', async () => {
    const cname = sel.value;
    insertBtn.disabled = !cname;
    if (formHandle) { formHandle.destroy(); formHandle = null; }
    if (!cname) {
      docs.innerHTML = `<p style="color:var(--fg-2);">Pick a component to see its config schema.</p>`;
      return;
    }
    docs.innerHTML = `<p aria-busy="true">Loading…</p>`;
    try {
      const c = await getComponent(cname);
      if (aborted()) return;
      const v = pickDocVersion(c);
      // Stash the resolved default version for the inserted snippet / block.
      sel.dataset.version = v ? v.version : '';
      const schemaStr = (v && v.configSchema) || '{}';

      // Build panel: a schema-driven form (one-way emitter into the textarea)
      // plus the read-only schema reference underneath a Form/YAML toggle.
      docs.innerHTML = `
        <h3 class="section-h" style="margin-top:0;"><code class="inline">${esc(cname)}</code></h3>
        <div class="builder-form-wrap">
          <div class="builder-mode">
            <button type="button" class="btn btn--ghost" id="toggle-docs" aria-pressed="true">Form</button>
            <button type="button" class="btn btn--ghost" id="edit-as-yaml">Edit as YAML…</button>
          </div>
          <div id="builder-form"></div>
          <details class="builder-io"><summary>Inputs (read existing tables)</summary>
            <div id="io-inputs"></div>
            <button type="button" class="btn btn--ghost" id="add-input">+ add input table</button>
          </details>
          <details class="builder-io"><summary>Outputs (tables this component writes)</summary>
            <label class="field">Default bucket <input class="input" id="out-bucket" type="text" placeholder="raw"></label>
            <label class="field">Table name (optional) <input class="input" id="out-table" type="text"></label>
            <label class="field">Write mode
              <select class="input" id="out-writemode"><option value="">— default —</option><option>APPEND</option><option>FULL_LOAD</option></select>
            </label>
            <span class="builder-io-chips" id="out-preview"></span>
          </details>
          <button type="button" class="btn btn--primary" id="insert-form" disabled>Insert as YAML</button>
        </div>
        <div id="builder-schema-docs" style="display:none;">${v && v.configSchema ? schemaDocs(v.configSchema) : `<p style="color:var(--fg-2);">No schema.</p>`}</div>`;

      formHandle = buildSchemaForm(
        document.getElementById('builder-form'),
        schemaStr,
        {}, // fresh component → empty initial config
        { secretKeys },
      );
      document.getElementById('insert-form').disabled = false;

      // ---- Inputs/outputs pickers (RFC 026 T7) ---------------------------
      // Inputs reference EXISTING tables (picked from the storage catalog);
      // outputs are NEW tables the run creates (free-text bucket/name +
      // writeMode). Rows live in this per-pick closure array, read at emit
      // time. All emitted bucket/table/writeMode strings route through the
      // T6 scalar quoter in componentBlockYAML (no raw interpolation); every
      // value shown in the DOM is esc()'d.
      const inputRows = [];
      const nsNames = [...storageByNs.keys()];

      // tableChips renders bucket/table labels with the shared chip classes
      // (runs-UX PR #23). Both operands are esc()'d before interpolation.
      const tableChips = (bucket, table) =>
        (bucket ? `<span class="chip chip--bucket"><span class="mono">${esc(bucket)}</span></span>` : '') +
        (table ? `<span class="chip chip--table"><span class="mono">${esc(table)}</span></span>` : '');

      const ioInputs = document.getElementById('io-inputs');
      const addInputBtn = document.getElementById('add-input');

      function addInputRow() {
        const row = document.createElement('div');
        row.className = 'builder-io-row';
        const nsOpts = `<option value="">— bucket —</option>` +
          nsNames.map((n) => `<option value="${esc(n)}">${esc(n)}</option>`).join('');
        row.innerHTML = `
          <label class="field">Bucket <select class="input io-ns">${nsOpts}</select></label>
          <label class="field">Table <select class="input io-tbl"><option value="">— table —</option></select></label>
          <span class="builder-io-chips io-chips"></span>
          <button type="button" class="btn btn--ghost io-remove">Remove</button>`;
        const nsSel = row.querySelector('.io-ns');
        const tblSel = row.querySelector('.io-tbl');
        const chips = row.querySelector('.io-chips');
        nsSel.addEventListener('change', () => {
          const tbls = nsSel.value ? (storageByNs.get(nsSel.value) || []) : [];
          tblSel.innerHTML = `<option value="">— table —</option>` +
            tbls.map((t) => `<option value="${esc(t.name)}">${esc(t.name)}</option>`).join('');
          chips.innerHTML = tableChips(nsSel.value, '');
        });
        tblSel.addEventListener('change', () => { chips.innerHTML = tableChips(nsSel.value, tblSel.value); });
        const entry = { nsSel, tblSel };
        row.querySelector('.io-remove').addEventListener('click', () => {
          const i = inputRows.indexOf(entry);
          if (i !== -1) inputRows.splice(i, 1);
          row.remove();
        });
        inputRows.push(entry);
        ioInputs.appendChild(row);
      }

      if (nsNames.length === 0) {
        // Empty-but-usable state: nothing to read, so disable add + explain.
        addInputBtn.disabled = true;
        ioInputs.innerHTML = `<p style="color:var(--fg-2);font-size:var(--text-sm);">No existing tables in this project yet.</p>`;
      } else {
        addInputBtn.addEventListener('click', addInputRow);
      }

      // Outputs live preview chips (user-entered → esc()'d in tableChips).
      const outPreview = document.getElementById('out-preview');
      const refreshOutPreview = () => {
        const b = (document.getElementById('out-bucket').value || '').trim();
        const t = (document.getElementById('out-table').value || '').trim();
        outPreview.innerHTML = tableChips(b, t);
      };
      document.getElementById('out-bucket').addEventListener('input', refreshOutPreview);
      document.getElementById('out-table').addEventListener('input', refreshOutPreview);

      // collectInputs → { tables:[{bucket, table}] } | null. The catalog's
      // namespace maps to the CRD `bucket`, its name to `table`. Rows missing
      // either field are skipped.
      function collectInputs() {
        const tables = [];
        for (const r of inputRows) {
          const bucket = r.nsSel.value;
          const table = r.tblSel.value;
          if (bucket && table) tables.push({ bucket, table });
        }
        return tables.length ? { tables } : null;
      }

      // collectOutputs → { outputs, ioErrs }. A table name selects the
      // OutputTableSpec shape (bucket REQUIRED — never emit an entry without
      // it); otherwise the bucket/writeMode become defaultBucket/
      // defaultWriteMode. Only fields the user filled are emitted.
      function collectOutputs() {
        const bucket = (document.getElementById('out-bucket').value || '').trim();
        const table = (document.getElementById('out-table').value || '').trim();
        const writeMode = document.getElementById('out-writemode').value || '';
        const ioErrs = [];
        let outputs = null;
        if (table) {
          if (!bucket) {
            ioErrs.push({ path: 'outputs.tables[0].bucket', message: 'Bucket is required when an output table name is given.', severity: 'error' });
          } else {
            const entry = { name: table, bucket };
            if (writeMode) entry.writeMode = writeMode;
            outputs = { tables: [entry] };
          }
        } else if (bucket || writeMode) {
          outputs = {};
          if (bucket) outputs.defaultBucket = bucket;
          if (writeMode) outputs.defaultWriteMode = writeMode;
        }
        return { outputs, ioErrs };
      }

      // "Insert as YAML": client-side pre-check via getErrors() + the outputs
      // bucket guard, then splice a serialized component block at the caret.
      // The server re-parses and validates authoritatively; this is only a
      // fast local guard.
      document.getElementById('insert-form').addEventListener('click', () => {
        const errs = formHandle.getErrors()
          .map((e) => ({ path: `config.${e.path}`, message: e.message, severity: 'error' }));
        const { outputs, ioErrs } = collectOutputs();
        const all = errs.concat(ioErrs);
        if (all.length) {
          document.getElementById('pipeline-msg').innerHTML = renderFindings(all);
          return;
        }
        const block = componentBlockYAML(sel.value, sel.dataset.version, formHandle.getValue(), collectInputs(), outputs);
        insertAtCursor(document.querySelector('textarea[name=yaml]'), block);
      });

      // "Edit as YAML" — the one-way toggle (confirm guards the irreversible
      // direction). On OK, preserve the current form entries (incl. the
      // collected inputs/outputs) by inserting them into the textarea, then
      // collapse the form; the textarea is now the surface. There is no
      // reverse sync back into the form. collectOutputs never yields a
      // bucket-less table entry, so the emitted block stays valid here too.
      document.getElementById('edit-as-yaml').addEventListener('click', () => {
        if (!confirm('Switch to YAML editing? Your current form entries for this component will be inserted into the YAML and the form cleared. You can’t convert a hand-edited block back into the form.')) return;
        const { outputs } = collectOutputs();
        const block = componentBlockYAML(sel.value, sel.dataset.version, formHandle.getValue(), collectInputs(), outputs);
        insertAtCursor(document.querySelector('textarea[name=yaml]'), block);
        document.querySelector('.builder-form-wrap')?.remove();
        document.querySelector('textarea[name=yaml]')?.focus();
      });

      // Form ⟷ schema-reference view toggle (aria-pressed conveys state).
      const toggleDocs = document.getElementById('toggle-docs');
      toggleDocs.addEventListener('click', () => {
        const showForm = toggleDocs.getAttribute('aria-pressed') !== 'true';
        toggleDocs.setAttribute('aria-pressed', String(showForm));
        const formEl = document.getElementById('builder-form');
        const insertEl = document.getElementById('insert-form');
        const docsEl = document.getElementById('builder-schema-docs');
        if (formEl) formEl.style.display = showForm ? '' : 'none';
        if (insertEl) insertEl.style.display = showForm ? '' : 'none';
        if (docsEl) docsEl.style.display = showForm ? 'none' : '';
        // The inputs/outputs pickers belong to the form surface, not the
        // read-only schema reference — hide them alongside the form.
        document.querySelectorAll('.builder-io').forEach((el) => { el.style.display = showForm ? '' : 'none'; });
      });
    } catch (e) {
      if (!aborted()) docs.innerHTML = `<p style="color:var(--status-fail-fg);">${esc(e.message)}</p>`;
    }
  });

  insertBtn.addEventListener('click', () => {
    const cname = sel.value;
    if (!cname) return;
    insertAtCursor(document.querySelector('textarea[name=yaml]'), componentSnippet(cname));
  });
}

// componentSnippet returns a stand-alone, correctly-indented `components:`
// list entry for the given component name (plus the resolved default version,
// if any). Indentation is fixed at the stage.components level; users paste it
// under a `components:` block. Name is sanitized to a DNS-1123-ish token.
function componentSnippet(name) {
  const ver = document.getElementById('add-component').dataset.version || '';
  return `        - name: ${name.replace(/[^a-z0-9-]/g, '-')}
          component: ${name}${ver ? `\n          version: ${ver}` : ''}
          config: {}
`;
}

// pickDocVersion selects the version to document / build a form for: the
// declared default, else the first non-prerelease (stable), else the first
// listed. Shared by the docs panel and the schema-form builder so they always
// agree on which version's configSchema is shown.
function pickDocVersion(c) {
  const versions = (c && c.versions) || [];
  return versions.find((x) => x.version === c.defaultVersion)
    || versions.find((x) => !x.prerelease)
    || versions[0];
}

// componentBlockYAML serializes one component into a `components:`-level YAML
// entry (8-space `- name:` indent, matching componentSnippet). It emits ONLY
// name / component / version / config — never `resources` (RESOURCE HIDING
// §4.4: resources are YAML-only + superadmin-gated server-side, never authored
// through the form). The serializer is intentionally minimal: it covers the
// constrained shapes the schema form produces (scalars, arrays of scalars, and
// JSON-object subvalues) and only needs to emit VALID, correctly-shaped YAML —
// the server re-parses and validates authoritatively (§4.3).
function componentBlockYAML(name, version, cfg, inputs, outputs) {
  // Identifier fields (name / component / version) may carry registry-supplied
  // strings, so they route through yamlIdentScalar — a value with YAML-special
  // chars or a newline becomes a single double-quoted scalar and can never
  // inject a sibling key (e.g. `resources:`) into the emitted block.
  const nm = yamlIdentScalar(name == null ? '' : name);
  const lines = [];
  lines.push(`        - name: ${nm}`);
  lines.push(`          component: ${nm}`);
  if (version) lines.push(`          version: ${yamlIdentScalar(version)}`);
  const keys = cfg && typeof cfg === 'object' ? Object.keys(cfg) : [];
  if (keys.length === 0) {
    lines.push(`          config: {}`);
  } else {
    lines.push(`          config:`);
    for (const k of keys) yamlKeyValue(lines, k, cfg[k], 12);
  }
  emitInputs(lines, inputs);
  emitOutputs(lines, outputs);
  return lines.join('\n') + '\n';
}

// emitInputs appends an `inputs:` sub-block (sibling of `config:`, 10-space
// indent) when there are input tables. Each table maps the storage catalog's
// (namespace, name) onto the CRD `InputTableSpec` fields (bucket, table). All
// bucket/table strings route through yamlScalar so user/catalog values can't
// inject sibling keys.
function emitInputs(lines, inputs) {
  const tables = inputs && Array.isArray(inputs.tables) ? inputs.tables : [];
  if (tables.length === 0) return;
  lines.push(`          inputs:`);
  lines.push(`            tables:`);
  for (const t of tables) {
    lines.push(`              - bucket: ${yamlScalar(t.bucket)}`);
    lines.push(`                table: ${yamlScalar(t.table)}`);
  }
}

// emitOutputs appends an `outputs:` sub-block when the user filled anything.
// A table name selects the `OutputTableSpec` shape — `bucket` is REQUIRED, so
// entries without one are dropped (never emitted). Otherwise the bucket /
// writeMode become defaultBucket / defaultWriteMode. Values route through
// yamlScalar.
function emitOutputs(lines, outputs) {
  if (!outputs) return;
  const tables = Array.isArray(outputs.tables)
    ? outputs.tables.filter((t) => t && t.bucket)
    : [];
  if (tables.length > 0) {
    lines.push(`          outputs:`);
    lines.push(`            tables:`);
    for (const t of tables) {
      lines.push(`              - name: ${yamlScalar(t.name)}`);
      lines.push(`                bucket: ${yamlScalar(t.bucket)}`);
      if (t.writeMode) lines.push(`                writeMode: ${yamlScalar(t.writeMode)}`);
    }
    return;
  }
  const hasBucket = outputs.defaultBucket != null && String(outputs.defaultBucket) !== '';
  const hasWriteMode = outputs.defaultWriteMode != null && String(outputs.defaultWriteMode) !== '';
  if (!hasBucket && !hasWriteMode) return;
  lines.push(`          outputs:`);
  if (hasBucket) lines.push(`            defaultBucket: ${yamlScalar(outputs.defaultBucket)}`);
  if (hasWriteMode) lines.push(`            defaultWriteMode: ${yamlScalar(outputs.defaultWriteMode)}`);
}

// yamlKeyValue appends one config entry (`<pad><key>: <value>`, or a block
// scalar / block list) to `lines`. `indent` is the column of the key.
function yamlKeyValue(lines, key, value, indent) {
  const pad = ' '.repeat(indent);
  const yk = yamlScalar(key);
  if (typeof value === 'string' && value.indexOf('\n') !== -1) {
    // multi-line string → literal block scalar `|` (content indented +2).
    lines.push(`${pad}${yk}: |`);
    const cpad = ' '.repeat(indent + 2);
    for (const ln of value.split('\n')) lines.push(`${cpad}${ln}`);
    return;
  }
  if (Array.isArray(value) && value.length > 0 && value.every(isYamlScalar)) {
    // array of scalars → block list.
    lines.push(`${pad}${yk}:`);
    const ipad = ' '.repeat(indent + 2);
    for (const item of value) lines.push(`${ipad}- ${yamlScalar(item)}`);
    return;
  }
  lines.push(`${pad}${yk}: ${yamlInline(value)}`);
}

// yamlInline renders a value on one line: scalars via yamlScalar; objects and
// non-scalar arrays as inline JSON (which is valid YAML flow syntax).
function yamlInline(value) {
  if (isYamlScalar(value)) return yamlScalar(value);
  return JSON.stringify(value);
}

function isYamlScalar(v) {
  return v === null || typeof v === 'string' || typeof v === 'number' || typeof v === 'boolean';
}

// yamlScalar renders a single scalar (also used for keys): numbers/booleans →
// literal; null → `null`; strings → plain unless they need quoting, in which
// case JSON.stringify yields a valid YAML double-quoted scalar.
function yamlScalar(v) {
  if (v === null) return 'null';
  if (typeof v === 'number' || typeof v === 'boolean') return String(v);
  const s = String(v);
  return needsYamlQuote(s) ? JSON.stringify(s) : s;
}

// needsYamlQuote is conservative — it quotes any string that could be misread
// as something other than a plain string in a flow/block context.
function needsYamlQuote(s) {
  if (s === '') return true;
  if (/^\s|\s$/.test(s)) return true;                  // leading / trailing space
  if (/^[-?:,[\]{}#&*!|>'"%@`]/.test(s)) return true;  // reserved leading indicator
  if (/:(\s|$)/.test(s) || /\s#/.test(s)) return true; // ": " / trailing ":" / " #"
  if (/^(true|false|null|yes|no|on|off|~)$/i.test(s)) return true; // bool / null-ish
  if (/^[+-]?(\d+\.?\d*|\.\d+)([eE][+-]?\d+)?$/.test(s)) return true; // number-ish
  return false;
}

// yamlIdentScalar renders an identifier field (name / component / version) as a
// SINGLE inline `key: value` scalar. Unlike the config path (yamlKeyValue) it
// NEVER emits a block scalar: a value with any YAML-special char OR a newline /
// control char is double-quoted via JSON.stringify (which escapes `\` `"` and
// newline as `\n`), so it can never break onto a new line as a sibling key.
// Clean DNS-1123 / semver values pass needsYamlQuote and stay unquoted, so
// well-formed output is byte-identical to the previous raw interpolation.
function yamlIdentScalar(v) {
  const s = String(v);
  if (needsYamlQuote(s) || /[\u0000-\u001f]/.test(s)) return JSON.stringify(s);
  return s;
}

// insertAtCursor splices text into a textarea at the caret (or over the
// current selection), then restores focus with the caret after the insert.
// Mirrors the proven idiom in query.js.
function insertAtCursor(textarea, text) {
  if (!textarea) return;
  const start = textarea.selectionStart;
  const end = textarea.selectionEnd;
  const before = textarea.value.slice(0, start);
  const after = textarea.value.slice(end);
  textarea.value = before + text + after;
  textarea.selectionStart = textarea.selectionEnd = start + text.length;
  textarea.focus();
}

// loadRecentRuns fetches the pipeline's most recent runs via the paged
// runs API (filtered by pipeline_id) and renders a compact table into
// #pipeline-runs. Fired once on page load — no live poll here (unlike
// the runs list page).
async function loadRecentRuns(pid, pipelineID) {
  const resp = await api(`/api/v1/projects/${encodeURIComponent(pid)}/runs?pipeline_id=${encodeURIComponent(pipelineID)}&limit=10`);
  const container = document.getElementById('pipeline-runs');
  if (!container) return; // navigated away
  const runs = resp.runs || [];
  if (runs.length === 0) {
    container.innerHTML = '<p><small>No runs yet.</small></p>';
    return;
  }
  const rows = runs.map((r) => {
    const id = String(r.id || '');
    const href = `/ui/runs/${encodeURIComponent(id)}`;
    const pill = phaseToPillClass(r.phase);
    const started = r.started_at ? timeTag(r.started_at) : '<span class="muted">—</span>';
    const dur = r.completed_at || r.started_at
      ? formatDuration(durationFrom(r.started_at, r.completed_at))
      : '—';
    return `
      <tr>
        <td><a href="${href}"><code class="mono">${esc(id.slice(0, 8))}</code></a></td>
        <td><span class="pill ${pill}">${esc(r.phase)}</span></td>
        <td>${started}</td>
        <td>${dur}</td>
      </tr>`;
  }).join('');
  const viewAll = resp.next_cursor
    ? `<p><a href="/ui/runs">View all &rarr;</a></p>`
    : '';
  container.innerHTML = `
    <table class="table">
      <thead>
        <tr>
          <th>Run ID</th>
          <th>Phase</th>
          <th>Started</th>
          <th>Duration</th>
        </tr>
      </thead>
      <tbody>${rows}</tbody>
    </table>
    ${viewAll}
  `;
}
