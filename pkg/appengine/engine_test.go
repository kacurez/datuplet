package appengine

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestRenderRoundTrip(t *testing.T) {
	e, err := NewEngine(context.Background(), 2048)
	if err != nil {
		t.Fatal(err)
	}
	bundle := []byte(`var __dtp_app = { render: async (ctx) => {
		const r = await datuplet.query("SELECT $x", {x: ctx.params.x});
		console.log("got rows");
		return { outputDoc: 1, title: "t", blocks: [{ id: "b1", type: "markdown", text: "rows: " + r.rows.length }] };
	}};`)
	q := func(_ context.Context, req []byte) ([]byte, error) {
		return []byte(`{"result":{"schema":[],"rows":[[1]],"truncated":false,"stats":{}}}`), nil
	}
	res, rerr := e.Render(context.Background(), RenderInput{
		Bundle: bundle, Path: "/", Params: map[string]string{"x": "1"},
		Now: time.Unix(1753228800, 0), Query: q,
		Limits: Limits{WallClock: 5 * time.Second, MaxQueries: 10, MaxOutputBytes: 2 << 20, MaxLogBytes: 64 << 10},
	})
	if rerr != nil {
		t.Fatalf("render error: %+v", rerr)
	}
	if !strings.Contains(string(res.Doc), `"rows: 1"`) {
		t.Fatalf("doc: %s", res.Doc)
	}
	if !strings.Contains(string(res.Log), "got rows") {
		t.Fatalf("log: %s", res.Log)
	}
}

func TestRenderInfiniteLoopIsKilled(t *testing.T) {
	e, _ := NewEngine(context.Background(), 2048)
	_, rerr := e.Render(context.Background(), RenderInput{
		Bundle: []byte(`var __dtp_app = { render: () => { for(;;){} } };`),
		Query:  func(context.Context, []byte) ([]byte, error) { return nil, nil },
		Limits: Limits{WallClock: 300 * time.Millisecond, MaxQueries: 1, MaxOutputBytes: 1 << 20, MaxLogBytes: 1 << 10},
	})
	if rerr == nil || rerr.Kind != "timeout" {
		t.Fatalf("want timeout, got %+v", rerr)
	}
}

func TestRenderOOMIsTrapped(t *testing.T) {
	e, _ := NewEngine(context.Background(), 64) // 4 MiB engine memory limit
	_, rerr := e.Render(context.Background(), RenderInput{
		Bundle: []byte(`var __dtp_app = { render: () => { let a=[]; for(;;){ a.push(new Array(100000).fill(0)); } } };`),
		Query:  func(context.Context, []byte) ([]byte, error) { return nil, nil },
		Limits: Limits{WallClock: 5 * time.Second, MaxQueries: 1, MaxOutputBytes: 1 << 20, MaxLogBytes: 1 << 10},
	})
	if rerr == nil || rerr.Kind != "render_error" {
		t.Fatalf("want render_error (memory trap), got %+v", rerr)
	}
}

func TestGuestGlobals(t *testing.T) {
	e, _ := NewEngine(context.Background(), 2048)
	// Real clock (WASI), Math.random present, ctx.now explicit, no fetch/timers.
	bundle := []byte(`var __dtp_app = { render: (ctx) => {
		const facts = { nowNum: (typeof ctx.now === "number"),
			dateOK: (typeof new Date().getUTCFullYear() === "number"),
			rnd: (typeof Math.random() === "number"),
			nofetch: (typeof fetch === "undefined"),
			notimer: (typeof setTimeout === "undefined") };
		return { outputDoc:1, title:"g", blocks:[{id:"b",type:"markdown",text: JSON.stringify(facts)}] };
	}};`)
	res, rerr := e.Render(context.Background(), RenderInput{
		Bundle: bundle, Now: time.Unix(1753228800, 0),
		Query:  func(context.Context, []byte) ([]byte, error) { return nil, nil },
		Limits: Limits{WallClock: 5 * time.Second, MaxQueries: 1, MaxOutputBytes: 1 << 20, MaxLogBytes: 1 << 10},
	})
	if rerr != nil {
		t.Fatal(rerr)
	}
	// The facts ride inside the block's text as a nested JSON string (its
	// quotes are escaped in the raw doc), so decode down to the text first.
	var doc struct {
		Blocks []struct {
			Text string `json:"text"`
		} `json:"blocks"`
	}
	if err := json.Unmarshal(res.Doc, &doc); err != nil || len(doc.Blocks) != 1 {
		t.Fatalf("doc: %s (err=%v)", res.Doc, err)
	}
	for _, want := range []string{`"nowNum":true`, `"dateOK":true`, `"rnd":true`, `"nofetch":true`, `"notimer":true`} {
		if !strings.Contains(doc.Blocks[0].Text, want) {
			t.Fatalf("missing %s in %s", want, res.Doc)
		}
	}
}

func TestExceptionPathStable(t *testing.T) {
	e, _ := NewEngine(context.Background(), 2048)
	// Exercise the shim's exception serialization repeatedly; a leak or
	// double-free in the C error path shows up as a crash/among-runs growth.
	for i := 0; i < 200; i++ {
		_, rerr := e.Render(context.Background(), RenderInput{
			Bundle: []byte(`var __dtp_app = { render: () => { throw new Error("boom") } };`),
			Query:  func(context.Context, []byte) ([]byte, error) { return nil, nil },
			Limits: Limits{WallClock: 2 * time.Second, MaxQueries: 1, MaxOutputBytes: 1 << 20, MaxLogBytes: 1 << 10},
		})
		if rerr == nil || rerr.Kind != "render_error" || !strings.Contains(rerr.Msg, "boom") {
			t.Fatalf("iter %d: want render_error boom, got %+v", i, rerr)
		}
	}
}

// TestRenderQueryFuncPanicReleasesLock is the regression test for the
// hostQuery lock-without-defer bug: a panicking QueryFunc is recovered by
// wazero into a call error, and Render's failure path takes rs.mu again in
// snapshotLog — if the panic escaped with the mutex held, Render would
// deadlock there instead of returning. The watchdog is far below WallClock
// so a hang is reported as a failure, not masked as a timeout.
func TestRenderQueryFuncPanicReleasesLock(t *testing.T) {
	e, err := NewEngine(context.Background(), 2048)
	if err != nil {
		t.Fatal(err)
	}
	bundle := []byte(`var __dtp_app = { render: async (ctx) => {
		await datuplet.query("SELECT 1", null);
		return { outputDoc: 1, title: "p", blocks: [] };
	}};`)
	q := func(context.Context, []byte) ([]byte, error) { panic("boom in QueryFunc") }

	var rerr *RenderError
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, rerr = e.Render(context.Background(), RenderInput{
			Bundle: bundle, Path: "/", Now: time.Unix(1753228800, 0), Query: q,
			Limits: Limits{WallClock: 30 * time.Second, MaxQueries: 1, MaxOutputBytes: 1 << 20, MaxLogBytes: 1 << 10},
		})
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Render did not return after QueryFunc panic (render-state mutex leaked)")
	}
	if rerr == nil || rerr.Kind != "render_error" {
		t.Fatalf("want render_error, got %+v", rerr)
	}
}

// TestRenderConcurrentIsolated proves one shared Engine (one runtime + one
// compiled module) serves parallel Renders with isolated per-render state:
// each render sees only its own params, query func, and log.
func TestRenderConcurrentIsolated(t *testing.T) {
	e, err := NewEngine(context.Background(), 2048)
	if err != nil {
		t.Fatal(err)
	}
	bundle := []byte(`var __dtp_app = { render: async (ctx) => {
		const r = await datuplet.query("SELECT $n", {n: ctx.params.n});
		console.log("render " + ctx.params.n);
		return { outputDoc: 1, title: "c", blocks: [{ id: "b", type: "markdown", text: "n=" + ctx.params.n + " rows=" + r.rows.length }] };
	}};`)
	var wg sync.WaitGroup
	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			n := strconv.Itoa(i)
			q := func(_ context.Context, req []byte) ([]byte, error) {
				if !strings.Contains(string(req), `"n":"`+n+`"`) {
					return nil, fmt.Errorf("render %s got foreign request: %s", n, req)
				}
				return []byte(`{"result":{"schema":[],"rows":[[1]],"truncated":false,"stats":{}}}`), nil
			}
			res, rerr := e.Render(context.Background(), RenderInput{
				Bundle: bundle, Path: "/", Params: map[string]string{"n": n},
				Now: time.Unix(1753228800, 0), Query: q,
				Limits: Limits{WallClock: 10 * time.Second, MaxQueries: 2, MaxOutputBytes: 1 << 20, MaxLogBytes: 1 << 10},
			})
			if rerr != nil {
				t.Errorf("render %s: %+v", n, rerr)
				return
			}
			if want := "n=" + n + " rows=1"; !strings.Contains(string(res.Doc), want) {
				t.Errorf("render %s: doc %s missing %q", n, res.Doc, want)
			}
			if want := "render " + n; !strings.Contains(string(res.Log), want) {
				t.Errorf("render %s: log %q missing %q", n, res.Log, want)
			}
		}(i)
	}
	wg.Wait()
}
