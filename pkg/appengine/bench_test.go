package appengine

// Benchmarks for the RFC 028 engine spike (task E2). Three benchmarks, each
// isolating one cost that matters for §7's render-slot math:
//
//   - BenchmarkInstantiate: fresh module instance + _initialize, no JS eval.
//     This is the per-render fixed cost every render pays regardless of
//     bundle size (E1 report: "each Render pays module instantiation +
//     _initialize ... Compilation is NOT paid per render").
//   - BenchmarkEval300KB: prelude + a deterministically generated ~300 KB
//     bundle, trivial render. Measures QuickJS parse+eval cost for a
//     realistic-sized bundle (spec §7: "realistic bundles 100-500 KB").
//   - BenchmarkJSON10MiB: a render whose QueryFunc returns a ~10 MiB rows
//     payload; the app JS does datuplet.query (guest-side JSON.parse),
//     reshapes rows into chart-ready records (Appendix A pattern), and
//     returns them in the OutputDoc, which __dtp_run JSON.stringifies.
//     Measures the guest-side parse+reshape+stringify path at the query
//     service's max_bytes-cap scale (spec §7: "one buffered query response
//     (<=max_bytes, 10 MiB cap)").
//
// All bundles/payloads are generated deterministically in code (no fixture
// files) so the benchmark is reproducible and self-contained.

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/tetratelabs/wazero"
)

// genPaddedBundle deterministically builds a JS bundle of at least
// targetBytes: a run of trivial numbered function declarations (stand-in for
// a real esbuild IIFE's code volume) followed by a minimal __dtp_app that
// returns a fixed OutputDoc. Padding via real (if pointless) function
// declarations exercises the parser similarly to real code, unlike a single
// giant string/comment literal.
func genPaddedBundle(targetBytes int) []byte {
	var sb strings.Builder
	for i := 0; sb.Len() < targetBytes; i++ {
		fmt.Fprintf(&sb, "function __dtp_pad_%d(x){ return x + %d; }\n", i, i)
	}
	sb.WriteString(`var __dtp_app = { render: (ctx) => {
		return { outputDoc: 1, title: "bench", blocks: [{ id: "b", type: "markdown", text: "ok" }] };
	}};`)
	return []byte(sb.String())
}

// genRowsResponse deterministically builds a query-response envelope
// ({"result":{"schema":...,"rows":[[id,label,value],...],...}}) whose JSON
// encoding is at least targetBytes — a stand-in for a wide DuckDB result set
// at the query service's 10 MiB max_bytes cap.
func genRowsResponse(targetBytes int) []byte {
	var sb strings.Builder
	sb.WriteString(`{"result":{"schema":[{"name":"id","type":"BIGINT"},` +
		`{"name":"label","type":"VARCHAR"},{"name":"value","type":"DOUBLE"}],"rows":[`)
	for i := 0; sb.Len() < targetBytes; i++ {
		if i > 0 {
			sb.WriteByte(',')
		}
		fmt.Fprintf(&sb, `[%d,"label-%d",%d.%d]`, i, i, i%1000, i%100)
	}
	sb.WriteString(`],"truncated":false,"stats":{}}}`)
	return []byte(sb.String())
}

// BenchmarkInstantiate measures the fixed per-render cost every Render pays
// before any JS runs: a fresh anonymous module instance plus the
// _initialize reactor-module handshake (wasi-libc + shim init). This is the
// floor under BenchmarkEval300KB and BenchmarkJSON10MiB, and the number §7's
// render-slot math needs as the per-render baseline.
func BenchmarkInstantiate(b *testing.B) {
	ctx := context.Background()
	e, err := NewEngine(ctx, 2048) // 128 MiB, the spec's default
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = e.Close(ctx) }()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mod, err := e.rt.InstantiateModule(ctx, e.compiled, wazero.NewModuleConfig().
			WithName("").
			WithStartFunctions("_initialize").
			WithSysWalltime().
			WithSysNanotime())
		if err != nil {
			b.Fatal(err)
		}
		if err := mod.Close(ctx); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkEval300KB renders a ~300 KB generated bundle (prelude + padding +
// trivial __dtp_app), no queries. Isolates bundle-size-dependent eval cost
// on top of BenchmarkInstantiate's fixed floor.
func BenchmarkEval300KB(b *testing.B) {
	ctx := context.Background()
	e, err := NewEngine(ctx, 2048)
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = e.Close(ctx) }()

	bundle := genPaddedBundle(300 << 10)
	b.Logf("generated bundle: %d bytes", len(bundle))
	noQuery := func(context.Context, []byte) ([]byte, error) { return nil, nil }
	limits := Limits{WallClock: 10 * time.Second, MaxQueries: 1, MaxOutputBytes: 2 << 20, MaxLogBytes: 4 << 10}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, rerr := e.Render(ctx, RenderInput{Bundle: bundle, Now: time.Now(), Query: noQuery, Limits: limits})
		if rerr != nil {
			b.Fatal(rerr)
		}
	}
}

// BenchmarkJSON10MiB renders an app whose single datuplet.query() call
// returns a ~10 MiB rows payload (the query service's max_bytes cap, spec
// §7). The app reshapes rows into chart-ready records (the Appendix A
// pattern: `rows.map(r => ({...}))`) and returns them in the OutputDoc,
// which __dtp_run's JSON.stringify then serializes — so one render exercises
// guest-side JSON.parse (query response) + a JS reshape pass + JSON.stringify
// (final result) all at ~10 MiB scale.
//
// MaxOutputBytes is set well above the spec's 2 MiB production default: a
// real app would aggregate before returning, but this benchmark deliberately
// keeps the full reshaped payload to measure the engine's raw
// parse+reshape+stringify throughput at the query cap, not production output
// policy (see the spike report's deviations section).
func BenchmarkJSON10MiB(b *testing.B) {
	ctx := context.Background()
	e, err := NewEngine(ctx, 4096) // 256 MiB: the spec's memory cap, not the 128 MiB default
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = e.Close(ctx) }()

	resp := genRowsResponse(10 << 20)
	b.Logf("generated query response: %d bytes", len(resp))
	q := func(context.Context, []byte) ([]byte, error) { return resp, nil }
	limits := Limits{WallClock: 30 * time.Second, MaxQueries: 1, MaxOutputBytes: 64 << 20, MaxLogBytes: 4 << 10}

	bundle := []byte(`var __dtp_app = { render: async (ctx) => {
		const r = await datuplet.query("SELECT id, label, value FROM t", null, {maxRows: 1000000});
		const values = r.rows.map((row) => ({ id: row[0], label: row[1], value: row[2] }));
		return { outputDoc: 1, title: "bench", blocks: [
			{ id: "data", type: "chart", library: "vega-lite", spec: { data: { values: values } } }
		]};
	}};`)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, rerr := e.Render(ctx, RenderInput{Bundle: bundle, Now: time.Now(), Query: q, Limits: limits})
		if rerr != nil {
			b.Fatal(rerr)
		}
	}
}
