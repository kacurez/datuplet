"use strict";
(() => {
  const q = globalThis.__dtp_host_query, l = globalThis.__dtp_host_log;
  delete globalThis.__dtp_host_query; delete globalThis.__dtp_host_log;
  const cap = [];
  globalThis.console = { log: (...a) => cap.push(a.map(String).join(" ")) };
  globalThis.datuplet = {
    query(sql, params, opts) {
      const resp = JSON.parse(q(JSON.stringify({ sql, params: params ?? null, opts: opts ?? null })));
      if (resp.error) { const e = new Error(resp.error.message); e.kind = resp.error.kind; return Promise.reject(e); }
      return Promise.resolve(resp.result);
    },
  };
  globalThis.__dtp_settled = false;
  globalThis.__dtp_run = (ctxJson) => {
    const ctx = JSON.parse(ctxJson);
    Promise.resolve()
      .then(() => globalThis.__dtp_app.render(ctx))
      .then((doc) => { globalThis.__dtp_result = JSON.stringify({ ok: true, doc, log: cap.join("\n") }); })
      .catch((e) => { globalThis.__dtp_result = JSON.stringify({ ok: false, error: String(e && e.message || e), kind: e && e.kind || "", stack: String(e && e.stack || ""), log: cap.join("\n") }); })
      .finally(() => { globalThis.__dtp_settled = true; });
  };
})();
