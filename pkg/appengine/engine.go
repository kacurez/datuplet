// Package appengine renders user dashboard apps (RFC 028) by running the
// committed QuickJS engine.wasm (quickjs-ng + C shim, see pkg/appengine/shim)
// on wazero.
//
// Lifecycle: NewEngine creates ONE wazero runtime (with the engine-level
// linear-memory page limit), instantiates WASI and the dtp_host host module
// once, and compiles the embedded engine module once. Every Render
// instantiates a FRESH anonymous module instance — fresh linear memory,
// reclaimed on Close — so renders are isolated from each other and the
// Engine is safe for concurrent Render calls. Per-render host state (query
// func, remaining query budget, log capture) travels via the call
// context.Context, which is how the single shared dtp_host module serves
// concurrent renders without shared mutable state.
package appengine

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
	"github.com/tetratelabs/wazero/imports/wasi_snapshot_preview1"

	engineembed "github.com/datuplet/datuplet/pkg/appengine/embed"
)

//go:embed prelude.js
var preludeJS []byte

// QueryFunc executes one datuplet.query() call on behalf of the guest.
// reqJSON is the guest's request ({"sql":...,"params":...,"opts":...}); the
// returned bytes are handed back to the guest verbatim and must be a
// response envelope ({"result":...} or {"error":{"message":...,"kind":...}}).
type QueryFunc func(ctx context.Context, reqJSON []byte) ([]byte, error)

// Limits bounds one render. Memory is engine-level (NewEngine), not here.
type Limits struct {
	WallClock      time.Duration
	MaxQueries     int
	MaxOutputBytes int
	MaxLogBytes    int
}

// RenderInput is one render request.
type RenderInput struct {
	Bundle []byte            // esbuild IIFE bundle defining globalThis.__dtp_app
	Path   string            // request path, surfaced as ctx.path
	Params map[string]string // query params, surfaced as ctx.params
	Now    time.Time         // render start, surfaced as ctx.now (ms since epoch)
	Query  QueryFunc
	Limits Limits
}

// Result is a successful render.
type Result struct {
	Doc json.RawMessage // the OutputDoc returned by the app's render()
	Log []byte          // captured console.log output, capped at MaxLogBytes
}

// RenderError is a failed render. Kind is one of
// "render_error" | "timeout" | "bad_request".
type RenderError struct {
	Kind  string
	Msg   string
	Stack string
	Log   []byte
}

func (e *RenderError) Error() string { return e.Kind + ": " + e.Msg }

// Engine wraps one wazero runtime + one compiled engine module. Safe for
// concurrent Render calls.
type Engine struct {
	rt       wazero.Runtime
	compiled wazero.CompiledModule
}

// stateKey keys the per-render *renderState in the call context.
type stateKey struct{}

// renderState is the per-render host-side state read by the dtp_host
// functions. mu serializes host calls: a single guest instance is
// single-threaded, so the mutex is a defensive invariant, not a hot lock.
type renderState struct {
	mu          sync.Mutex
	q           QueryFunc
	queriesLeft int
	log         []byte
	maxLog      int
}

func (rs *renderState) snapshotLog() []byte {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	if len(rs.log) == 0 {
		return nil
	}
	return append([]byte(nil), rs.log...)
}

// NewEngine compiles the embedded engine module once. memoryPages caps each
// render instance's linear memory (64 KiB pages; e.g. 2048 = 128 MiB). The
// returned Engine is safe for concurrent Render calls.
func NewEngine(ctx context.Context, memoryPages uint32) (*Engine, error) {
	rt := wazero.NewRuntimeWithConfig(ctx, wazero.NewRuntimeConfig().
		WithCloseOnContextDone(true).
		WithMemoryLimitPages(memoryPages))

	wasi_snapshot_preview1.MustInstantiate(ctx, rt)

	_, err := rt.NewHostModuleBuilder("dtp_host").
		NewFunctionBuilder().
		WithGoModuleFunction(api.GoModuleFunc(hostQuery),
			[]api.ValueType{api.ValueTypeI32, api.ValueTypeI32},
			[]api.ValueType{api.ValueTypeI64}).
		Export("query").
		NewFunctionBuilder().
		WithGoModuleFunction(api.GoModuleFunc(hostLog),
			[]api.ValueType{api.ValueTypeI32, api.ValueTypeI32},
			nil).
		Export("log").
		Instantiate(ctx)
	if err != nil {
		_ = rt.Close(ctx)
		return nil, fmt.Errorf("instantiate dtp_host module: %w", err)
	}

	compiled, err := rt.CompileModule(ctx, engineembed.EngineWasm)
	if err != nil {
		_ = rt.Close(ctx)
		return nil, fmt.Errorf("compile engine.wasm: %w", err)
	}
	return &Engine{rt: rt, compiled: compiled}, nil
}

// Close releases the runtime and every resource it owns. Renders in flight
// are interrupted.
func (e *Engine) Close(ctx context.Context) error {
	return e.rt.Close(ctx)
}

// renderCtx is the JSON handed to the prelude's __dtp_run.
type renderCtx struct {
	Path   string            `json:"path"`
	Params map[string]string `json:"params"`
	Now    int64             `json:"now"` // ms since epoch (explicit, spec §6.2)
}

// guestResult is the result JSON produced by the shim/prelude.
type guestResult struct {
	OK    bool            `json:"ok"`
	Doc   json.RawMessage `json:"doc"`
	Error string          `json:"error"`
	Kind  string          `json:"kind"`
	Stack string          `json:"stack"`
	Log   string          `json:"log"`
}

// Render runs one app render in a fresh module instance.
func (e *Engine) Render(ctx context.Context, in RenderInput) (*Result, *RenderError) {
	if in.Limits.WallClock <= 0 {
		return nil, &RenderError{Kind: "bad_request", Msg: "limits: WallClock must be positive"}
	}

	ctx, cancel := context.WithTimeout(ctx, in.Limits.WallClock)
	defer cancel()

	rs := &renderState{
		q:           in.Query,
		queriesLeft: in.Limits.MaxQueries,
		maxLog:      in.Limits.MaxLogBytes,
	}
	ctx = context.WithValue(ctx, stateKey{}, rs)

	// Fresh anonymous instance per render. engine.wasm is a WASI *reactor*:
	// its exported _initialize must run once post-instantiation, before
	// dtp_alloc/dtp_render (E0 report). Real wall/nano clocks back the
	// guest's Date (spec §6.2: real clock, no determinism guarantee).
	mod, err := e.rt.InstantiateModule(ctx, e.compiled, wazero.NewModuleConfig().
		WithName("").
		WithStartFunctions("_initialize").
		WithSysWalltime().
		WithSysNanotime())
	if err != nil {
		return nil, hostFailure(ctx, "instantiate render module", err, rs)
	}
	defer mod.Close(context.WithoutCancel(ctx))

	script := make([]byte, 0, len(preludeJS)+2+len(in.Bundle))
	script = append(script, preludeJS...)
	script = append(script, ";\n"...)
	script = append(script, in.Bundle...)

	params := in.Params
	if params == nil {
		params = map[string]string{}
	}
	ctxJSON, err := json.Marshal(renderCtx{Path: in.Path, Params: params, Now: in.Now.UnixMilli()})
	if err != nil {
		return nil, &RenderError{Kind: "render_error", Msg: "marshal render ctx: " + err.Error()}
	}

	scriptPtr, err := writeGuestBytes(ctx, mod, script)
	if err != nil {
		return nil, hostFailure(ctx, "write script", err, rs)
	}
	ctxPtr, err := writeGuestBytes(ctx, mod, ctxJSON)
	if err != nil {
		return nil, hostFailure(ctx, "write render ctx", err, rs)
	}

	ret, err := mod.ExportedFunction("dtp_render").Call(ctx,
		uint64(scriptPtr), uint64(uint32(len(script))),
		uint64(ctxPtr), uint64(uint32(len(ctxJSON))))
	if err != nil {
		return nil, hostFailure(ctx, "dtp_render", err, rs)
	}

	resPtr, resLen := uint32(ret[0]>>32), uint32(ret[0])
	raw, ok := mod.Memory().Read(resPtr, resLen)
	if !ok {
		return nil, &RenderError{Kind: "render_error",
			Msg: fmt.Sprintf("result (ptr=%d len=%d) out of guest memory bounds", resPtr, resLen),
			Log: rs.snapshotLog()}
	}
	// Copy out before mod.Close reclaims the linear memory backing `raw`.
	raw = append([]byte(nil), raw...)

	var gr guestResult
	if err := json.Unmarshal(raw, &gr); err != nil {
		return nil, &RenderError{Kind: "render_error",
			Msg: "malformed engine result: " + err.Error(),
			Log: rs.snapshotLog()}
	}

	logBytes := capBytes([]byte(gr.Log), in.Limits.MaxLogBytes)
	if !gr.OK {
		kind := "render_error"
		if ctx.Err() != nil {
			kind = "timeout"
		}
		return nil, &RenderError{Kind: kind, Msg: gr.Error, Stack: gr.Stack, Log: logBytes}
	}
	if in.Limits.MaxOutputBytes > 0 && len(gr.Doc) > in.Limits.MaxOutputBytes {
		return nil, &RenderError{Kind: "render_error",
			Msg: fmt.Sprintf("output doc is %d bytes, exceeds limit %d", len(gr.Doc), in.Limits.MaxOutputBytes),
			Log: logBytes}
	}
	return &Result{Doc: gr.Doc, Log: logBytes}, nil
}

// hostFailure maps a wazero-level failure (instantiation error, trap, module
// closed) to a RenderError: deadline exceeded → timeout (the wall-clock
// backstop for a guest that never yields); anything else — including the
// memory-limit trap — → render_error.
func hostFailure(ctx context.Context, op string, err error, rs *renderState) *RenderError {
	if ctx.Err() != nil {
		return &RenderError{Kind: "timeout",
			Msg: "render exceeded wall clock: " + ctx.Err().Error(),
			Log: rs.snapshotLog()}
	}
	return &RenderError{Kind: "render_error", Msg: op + ": " + err.Error(), Log: rs.snapshotLog()}
}

// writeGuestBytes allocates a guest buffer via dtp_alloc and copies b into it.
func writeGuestBytes(ctx context.Context, mod api.Module, b []byte) (uint32, error) {
	ret, err := mod.ExportedFunction("dtp_alloc").Call(ctx, uint64(uint32(len(b))))
	if err != nil {
		return 0, err
	}
	ptr := uint32(ret[0])
	if ptr == 0 {
		return 0, errors.New("dtp_alloc returned NULL")
	}
	if !mod.Memory().Write(ptr, b) {
		return 0, fmt.Errorf("guest write (ptr=%d len=%d) out of bounds", ptr, len(b))
	}
	return ptr, nil
}

func capBytes(b []byte, max int) []byte {
	if max >= 0 && len(b) > max {
		b = b[:max]
	}
	if len(b) == 0 {
		return nil
	}
	return b
}

// queryErrEnvelope builds the {"error":{...}} response the prelude turns
// into a rejected datuplet.query promise carrying e.kind.
func queryErrEnvelope(kind, msg string) []byte {
	env := struct {
		Error struct {
			Message string `json:"message"`
			Kind    string `json:"kind"`
		} `json:"error"`
	}{}
	env.Error.Message = msg
	env.Error.Kind = kind
	b, err := json.Marshal(env)
	if err != nil { // unreachable: fixed shape, string fields
		return []byte(`{"error":{"message":"internal error","kind":"render_error"}}`)
	}
	return b
}

// hostQuery implements dtp_host.query(req_ptr, req_len u32) -> u64
// (guest ptr<<32|len). The response buffer is allocated in guest memory via
// dtp_alloc; the shim frees it after copying it into a JS string.
func hostQuery(ctx context.Context, mod api.Module, stack []uint64) {
	reqPtr, reqLen := uint32(stack[0]), uint32(stack[1])

	var resp []byte
	rs, _ := ctx.Value(stateKey{}).(*renderState)
	switch {
	case rs == nil:
		resp = queryErrEnvelope("render_error", "no render state in context")
	default:
		rs.mu.Lock()
		switch {
		case rs.queriesLeft <= 0:
			resp = queryErrEnvelope("bad_request", "query limit exceeded")
		case rs.q == nil:
			resp = queryErrEnvelope("bad_request", "no query function configured")
		default:
			rs.queriesLeft--
			req, ok := mod.Memory().Read(reqPtr, reqLen)
			if !ok {
				resp = queryErrEnvelope("render_error",
					fmt.Sprintf("query request (ptr=%d len=%d) out of guest memory bounds", reqPtr, reqLen))
				break
			}
			// Copy: the Read view is invalidated if guest memory grows
			// (e.g. by the dtp_alloc below) and must not outlive this call.
			req = append([]byte(nil), req...)
			out, err := rs.q(ctx, req)
			switch {
			case err != nil:
				resp = queryErrEnvelope("query_error", err.Error())
			case len(out) == 0:
				resp = []byte(`{"result":null}`)
			default:
				resp = out
			}
		}
		rs.mu.Unlock()
	}

	ptr, err := writeGuestBytes(ctx, mod, resp)
	if err != nil {
		// Module is closing (timeout) or guest allocator failed. ptr=0/len=0
		// yields an empty JS string; JSON.parse("") throws in the guest and
		// surfaces through the normal error path.
		stack[0] = 0
		return
	}
	stack[0] = uint64(ptr)<<32 | uint64(uint32(len(resp)))
}

// hostLog implements dtp_host.log(ptr, len u32). The prelude captures
// console.log itself (returned in the result JSON), but the import is part
// of the module ABI and must behave sensibly if a guest calls it: entries
// accumulate in the per-render state, capped at MaxLogBytes.
func hostLog(ctx context.Context, mod api.Module, stack []uint64) {
	ptr, length := uint32(stack[0]), uint32(stack[1])
	rs, _ := ctx.Value(stateKey{}).(*renderState)
	if rs == nil {
		return
	}
	b, ok := mod.Memory().Read(ptr, length)
	if !ok {
		return
	}
	rs.mu.Lock()
	defer rs.mu.Unlock()
	if len(rs.log) > 0 && len(rs.log) < rs.maxLog {
		rs.log = append(rs.log, '\n')
	}
	if room := rs.maxLog - len(rs.log); room > 0 {
		if len(b) > room {
			b = b[:room]
		}
		rs.log = append(rs.log, b...)
	}
}
