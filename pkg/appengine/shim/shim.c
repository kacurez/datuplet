// shim.c — QuickJS-in-WASI shim for the Datuplet app-worker (RFC 028 Part 0/3).
//
// Exports: dtp_alloc, dtp_render.
// Imports (module "dtp_host"): query, log.
//
// ABI (contract-and-constraints.md "Engine ABI"):
//   dtp_alloc(size u32) -> u32                       guest pointer
//   dtp_render(script_ptr, script_len,
//              ctx_ptr, ctx_len u32) -> u64           guest ptr<<32|len of
//                                                      result JSON
//   host_query(req_ptr, req_len u32) -> u64           guest ptr<<32|len;
//                                                      host writes the
//                                                      response into GUEST
//                                                      memory via dtp_alloc,
//                                                      guest frees it
//   host_log(ptr, len u32)
//
// dtp_render evaluates `script` (Go-side prelude.js + ";\n" + app bundle) as
// a single global-scope script (JS_EVAL_TYPE_GLOBAL — the bundle is an
// esbuild --format=iife --global-name=__dtp_app IIFE, so no module loader is
// needed), then calls the prelude-defined globalThis.__dtp_run(ctxJson).
// __dtp_run does not return the render Promise: the prelude stores the
// settled outcome in globalThis.__dtp_result and sets
// globalThis.__dtp_settled = true in a .finally(). After JS_Call returns,
// this shim drains the microtask queue with JS_ExecutePendingJob until it
// empties, then reads __dtp_settled: if the queue emptied with the render
// Promise still pending (e.g. a never-resolving await), it packs an
// ok:false "render did not settle" result rather than reading a stale/
// missing __dtp_result. (The wazero wall-clock deadline on the host side is
// the backstop for a guest that never yields at all.)
//
// Result JSON is always one of:
//   {"ok":true,"doc":<OutputDoc>}
//   {"ok":false,"error":"<msg>","stack":"<js stack>"}
//
// Leak discipline: every JS_ToCString is paired with JS_FreeCString; every
// JSValue obtained via JS_GetPropertyStr / JS_NewString* / JS_JSONStringify
// is freed on both the success and the exception path; JS_SetPropertyStr
// consumes its value argument per the QuickJS *_str convention, so those are
// not double-freed. JS_FreeContext / JS_FreeRuntime run exactly once, after
// every live JSValue derived from ctx has been freed. All exception-path
// string building funnels through pack_error() so success and failure share
// one set of frees.

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "quickjs.h"

__attribute__((import_module("dtp_host"), import_name("query")))
extern unsigned long long host_query(const char *ptr, unsigned int len);
__attribute__((import_module("dtp_host"), import_name("log")))
extern void host_log(const char *ptr, unsigned int len);

__attribute__((export_name("dtp_alloc")))
void *dtp_alloc(unsigned int size) {
    return malloc(size);
}

// Zero-length sentinel: pack_bytes() returns this address (never
// dereferenced by the caller, since it always travels with len==0) instead
// of relying on the implementation-defined result of malloc(0).
static char zero_len_sentinel;

// Copies `slen` bytes of `s` into a freshly malloc'd guest buffer. Returns
// NULL only on real allocation failure (slen > 0 and malloc fails) — the
// caller must distinguish that from a legitimate empty string.
static char *pack_bytes(const char *s, size_t slen, unsigned long long *out_len) {
    if (slen == 0) {
        *out_len = 0;
        return &zero_len_sentinel;
    }
    char *buf = malloc(slen);
    if (!buf) {
        *out_len = 0;
        return NULL;
    }
    memcpy(buf, s, slen);
    *out_len = slen;
    return buf;
}

static unsigned long long pack_ptr_len(void *ptr, unsigned long long len) {
    return ((unsigned long long)(unsigned int)(uintptr_t)ptr << 32) | (unsigned int)len;
}

// Builds the {"ok":false,"error":...,"stack":...} result for an exception.
// Consumes `exc` (frees it before returning) regardless of outcome. Never
// fails outwardly: any internal failure (OOM, a JSON encode that itself
// throws) falls back to a static literal so dtp_render always returns a
// well-formed result.
static char *pack_error(JSContext *ctx, JSValue exc, unsigned long long *out_len) {
    static const char fallback[] =
        "{\"ok\":false,\"error\":\"internal error\",\"stack\":\"\"}";

    JSValue stackv = JS_GetPropertyStr(ctx, exc, "stack");
    const char *emsg = JS_ToCString(ctx, exc);
    const char *stk = NULL;
    if (!JS_IsException(stackv) && !JS_IsUndefined(stackv))
        stk = JS_ToCString(ctx, stackv);

    char *result = NULL;
    JSValue eobj = JS_NewObject(ctx);
    if (!JS_IsException(eobj)) {
        JS_SetPropertyStr(ctx, eobj, "ok", JS_NewBool(ctx, 0));
        JS_SetPropertyStr(ctx, eobj, "error", JS_NewString(ctx, emsg ? emsg : "unknown"));
        JS_SetPropertyStr(ctx, eobj, "stack", JS_NewString(ctx, stk ? stk : ""));

        JSValue ejson = JS_JSONStringify(ctx, eobj, JS_UNDEFINED, JS_UNDEFINED);
        if (!JS_IsException(ejson) && !JS_IsUndefined(ejson)) {
            const char *es = JS_ToCString(ctx, ejson);
            if (es)
                result = pack_bytes(es, strlen(es), out_len);
            JS_FreeCString(ctx, es);
        }
        JS_FreeValue(ctx, ejson);
    }
    JS_FreeValue(ctx, eobj);

    JS_FreeCString(ctx, stk);
    JS_FreeCString(ctx, emsg);
    JS_FreeValue(ctx, stackv);
    JS_FreeValue(ctx, exc);

    if (!result)
        result = pack_bytes(fallback, sizeof(fallback) - 1, out_len);
    return result;
}

static JSValue js_host_query(JSContext *ctx, JSValueConst this_val,
                              int argc, JSValueConst *argv) {
    (void)this_val;
    if (argc < 1)
        return JS_ThrowTypeError(ctx, "query requires 1 argument");

    size_t len = 0;
    const char *req = JS_ToCStringLen(ctx, &len, argv[0]);
    if (!req)
        return JS_EXCEPTION;

    unsigned long long packed = host_query(req, (unsigned int)len);
    JS_FreeCString(ctx, req);

    const char *resp = (const char *)(uintptr_t)(unsigned int)(packed >> 32);
    unsigned int rlen = (unsigned int)packed;
    JSValue out = JS_NewStringLen(ctx, resp, rlen);
    free((void *)resp);
    return out;
}

static JSValue js_host_log(JSContext *ctx, JSValueConst this_val,
                            int argc, JSValueConst *argv) {
    (void)this_val;
    if (argc < 1)
        return JS_ThrowTypeError(ctx, "log requires 1 argument");

    size_t len = 0;
    const char *msg = JS_ToCStringLen(ctx, &len, argv[0]);
    if (!msg)
        return JS_EXCEPTION;

    host_log(msg, (unsigned int)len);
    JS_FreeCString(ctx, msg);
    return JS_UNDEFINED;
}

__attribute__((export_name("dtp_render")))
unsigned long long dtp_render(const char *script, unsigned int script_len,
                               const char *ctx_json, unsigned int ctx_len) {
    char *result;
    unsigned long long out_len;

    JSRuntime *rt = JS_NewRuntime();
    if (!rt) {
        static const char msg[] = "{\"ok\":false,\"error\":\"failed to create JS runtime\",\"stack\":\"\"}";
        result = pack_bytes(msg, sizeof(msg) - 1, &out_len);
        return pack_ptr_len(result, out_len);
    }
    // Real cap is the wazero linear-memory page limit (engine-level, set at
    // NewEngine time); disable QuickJS's own limit so it never fights that.
    JS_SetMemoryLimit(rt, (size_t)-1);

    JSContext *ctx = JS_NewContext(rt);
    if (!ctx) {
        JS_FreeRuntime(rt);
        static const char msg[] = "{\"ok\":false,\"error\":\"failed to create JS context\",\"stack\":\"\"}";
        result = pack_bytes(msg, sizeof(msg) - 1, &out_len);
        return pack_ptr_len(result, out_len);
    }

    JSValue global = JS_GetGlobalObject(ctx);
    // JS_SetPropertyStr consumes (frees) its value argument.
    JS_SetPropertyStr(ctx, global, "__dtp_host_query",
        JS_NewCFunction(ctx, js_host_query, "__dtp_host_query", 1));
    JS_SetPropertyStr(ctx, global, "__dtp_host_log",
        JS_NewCFunction(ctx, js_host_log, "__dtp_host_log", 1));

    // JS_Eval requires a zero-terminated buffer: "'input' must be zero
    // terminated i.e. input[input_len] = '\0'" (quickjs.h / quickjs.c). The
    // caller (Go host) only guarantees script_len valid bytes at `script`,
    // so make our own NUL-terminated copy rather than reading one byte past
    // a buffer we don't own the allocation size of. QuickJS copies whatever
    // source text it needs for Function#toString() internally (js_strndup),
    // so it's safe to free this the moment JS_Eval returns.
    {
        char *nulscript = malloc((size_t)script_len + 1);
        if (!nulscript) {
            JS_FreeValue(ctx, global);
            JS_FreeContext(ctx);
            JS_FreeRuntime(rt);
            static const char msg[] = "{\"ok\":false,\"error\":\"out of memory\",\"stack\":\"\"}";
            result = pack_bytes(msg, sizeof(msg) - 1, &out_len);
            return pack_ptr_len(result, out_len);
        }
        memcpy(nulscript, script, script_len);
        nulscript[script_len] = '\0';
        JSValue v = JS_Eval(ctx, nulscript, script_len, "app.js", JS_EVAL_TYPE_GLOBAL);
        free(nulscript);
        if (JS_IsException(v)) {
            JS_FreeValue(ctx, v);
            goto exception;
        }
        JS_FreeValue(ctx, v);
    }

    {
        JSValue run = JS_GetPropertyStr(ctx, global, "__dtp_run");
        if (JS_IsException(run)) {
            JS_FreeValue(ctx, run);
            goto exception;
        }
        JSValue arg = JS_NewStringLen(ctx, ctx_json, ctx_len);
        if (JS_IsException(arg)) {
            JS_FreeValue(ctx, run);
            JS_FreeValue(ctx, arg);
            goto exception;
        }
        JSValue v = JS_Call(ctx, run, JS_UNDEFINED, 1, &arg);
        JS_FreeValue(ctx, run);
        JS_FreeValue(ctx, arg);
        if (JS_IsException(v)) {
            JS_FreeValue(ctx, v);
            goto exception;
        }
        JS_FreeValue(ctx, v);
    }

    // Drain microtasks (promise reactions, .finally callbacks, etc.) until
    // the queue empties.
    for (;;) {
        JSContext *pctx;
        int r = JS_ExecutePendingJob(rt, &pctx);
        if (r < 0)
            goto exception; // the failing job's exception is pending on pctx == ctx
        if (r == 0)
            break;
    }

    {
        JSValue settledv = JS_GetPropertyStr(ctx, global, "__dtp_settled");
        if (JS_IsException(settledv)) {
            JS_FreeValue(ctx, settledv);
            goto exception;
        }
        int settled = JS_ToBool(ctx, settledv);
        JS_FreeValue(ctx, settledv);
        if (!settled) {
            static const char msg[] = "{\"ok\":false,\"error\":\"render did not settle\",\"stack\":\"\"}";
            result = pack_bytes(msg, sizeof(msg) - 1, &out_len);
            goto done;
        }
    }

    {
        JSValue res = JS_GetPropertyStr(ctx, global, "__dtp_result");
        if (JS_IsException(res)) {
            JS_FreeValue(ctx, res);
            goto exception;
        }
        const char *s = JS_ToCString(ctx, res);
        JS_FreeValue(ctx, res);
        if (!s)
            goto exception;
        result = pack_bytes(s, strlen(s), &out_len);
        JS_FreeCString(ctx, s);
        if (!result) {
            static const char msg[] = "{\"ok\":false,\"error\":\"out of memory\",\"stack\":\"\"}";
            result = pack_bytes(msg, sizeof(msg) - 1, &out_len);
        }
    }

done:
    JS_FreeValue(ctx, global);
    JS_FreeContext(ctx);
    JS_FreeRuntime(rt);
    return pack_ptr_len(result, out_len);

exception:
    {
        JSValue exc = JS_GetException(ctx);
        result = pack_error(ctx, exc, &out_len); // consumes exc
    }
    goto done;
}
