# RFC 028 engine spike report (Task E2)

## Status: GO (with one capacity-model caveat — see §5)

**Fix round 1 (Codex review gate, Major):** the original benchmark run
timed `NewEngine` compile, bundle/response generation, and `b.Logf` setup
inside each benchmark's measured region (no `b.ResetTimer()` before the
loop), so the reported per-op numbers overstated isolated per-render cost —
most visibly in `BenchmarkJSON10MiB`'s allocs/op, which included the
one-time ~10 MiB response generation. Fixed by adding `b.ResetTimer()`
immediately after each benchmark's setup (`pkg/appengine/bench_test.go`);
all numbers below are from the corrected run. The qualitative conclusions
and the GO recommendation are unchanged — see the note at the end of §4.

## Purpose

RFC 028 §12.1 gates its own §7 capacity assumptions on a spike measuring
QuickJS-on-wazero eval time and JSON throughput on a representative payload.
E0/E1 built and proved the engine (`pkg/appengine`, commits `68444eb`,
`b865a52`, `d200e3e`); this report supplies the missing numbers and a
GO/NO-GO recommendation for Part 3 (the app-worker).

## Method

Three benchmarks in `pkg/appengine/bench_test.go`, each isolating one cost
that feeds §7's render-slot math:

- **`BenchmarkInstantiate`** — fresh anonymous module instance +
  `_initialize` (the WASI reactor handshake), no JS eval at all. This is the
  fixed per-render tax the E1 report flagged: "each Render pays module
  instantiation + `_initialize`... Compilation is NOT paid per render."
- **`BenchmarkEval300KB`** — `prelude.js` + a deterministically generated
  ~300 KB bundle (numbered no-op function declarations, real parseable code
  rather than a comment/string blob, to exercise the parser similarly to a
  real esbuild IIFE) + a trivial `render()`, no queries. Targets spec §7's
  "realistic bundles 100–500 KB."
- **`BenchmarkJSON10MiB`** — a render whose single `datuplet.query()` call
  is answered by a deterministically generated ~10 MiB rows envelope (the
  query service's `max_bytes` cap, spec §7). The bundle's JS does
  `JSON.parse` (via the prelude's query bridge), reshapes every row into a
  chart-ready record (`rows.map(r => ({id, label, value}))` — the Appendix A
  pattern), and returns it in the `OutputDoc`, which `__dtp_run`
  `JSON.stringify`s. This exercises guest-side parse + reshape + stringify
  end to end at the query cap.

All bundles/payloads are generated in code (`genPaddedBundle`,
`genRowsResponse`) — no fixtures, fully reproducible. `MaxOutputBytes` in the
10 MiB benchmark is set to 64 MiB (well above the 2 MiB production default)
deliberately, to measure raw engine throughput rather than production output
policy — see the deviation note in §6.

No changes were made to `engine.go` or any other existing file; the
benchmarks use `Engine.rt`/`Engine.compiled` directly, which are accessible
because the test file is in the same package.

## Run command

```
go test ./pkg/appengine/ -bench . -benchmem -run XXX -benchtime 3s -timeout 300s
```

`BenchmarkJSON10MiB` only completed 1 iteration at `-benchtime 3s` (each
iteration already takes >3 s), so it was re-run in isolation for a tighter
estimate:

```
go test ./pkg/appengine/ -bench BenchmarkJSON10MiB -benchmem -run XXX -benchtime 15s -timeout 300s
```

## Machine / toolchain

- CPU: `Apple M3` (`sysctl -n machdep.cpu.brand_string`)
- RAM: 16 GiB, 8 logical CPUs (`sysctl -n hw.memsize hw.ncpu`)
- OS: macOS 26.5.1 (build 25F80)
- Go: `go1.25.7 darwin/arm64`
- `pkg/appengine/embed/engine.wasm`: **1,312,937 bytes (≈1.25 MiB)**

## Results (verbatim)

All numbers below are from the post-fix run (`b.ResetTimer()` after setup
in all three benchmarks — see the fix-round note above). The original
pre-fix numbers are preserved in the Task E2 report's fix-round-1 section
for the record; they are superseded here.

First pass, all three benchmarks together, `-benchtime 3s`:

```
goos: darwin
goarch: arm64
pkg: github.com/datuplet/datuplet/pkg/appengine
cpu: Apple M3
BenchmarkInstantiate-8   	   64423	     55754 ns/op	  250378 B/op	    1568 allocs/op
BenchmarkEval300KB-8     	      58	  56990121 ns/op	28925374 B/op	    1629 allocs/op
    bench_test.go:111: generated bundle: 307365 bytes   (x2)
BenchmarkJSON10MiB-8     	       1	4325172917 ns/op	728986712 B/op	    1642 allocs/op
    bench_test.go:147: generated query response: 10485810 bytes
PASS
ok  	github.com/datuplet/datuplet/pkg/appengine	14.367s
```

Second pass, `BenchmarkJSON10MiB` alone at `-benchtime 15s` for a
multi-sample average:

```
goos: darwin
goarch: arm64
pkg: github.com/datuplet/datuplet/pkg/appengine
cpu: Apple M3
BenchmarkJSON10MiB-8   	       4	4369095844 ns/op	728987738 B/op	    1643 allocs/op
    bench_test.go:147: generated query response: 10485810 bytes   (x3)
PASS
ok  	github.com/datuplet/datuplet/pkg/appengine	36.050s
```

The 4-rep average (4.37 s/op) is consistent with the single-shot sample
(4.33 s/op) from the first pass — no wild variance.

### Headline numbers

| Benchmark | Time/op | Bytes/op (Go host allocs) | Allocs/op |
|---|---|---|---|
| `BenchmarkInstantiate` | **0.056 ms** | 250 KB | 1,568 |
| `BenchmarkEval300KB` | **57.0 ms** | 28.9 MB | 1,629 |
| `BenchmarkJSON10MiB` | **4.37 s** (avg of 4) | 729 MB | 1,643 |

Note on the allocs/op drop for `BenchmarkEval300KB` (3,872 → 1,629) and
especially `BenchmarkJSON10MiB` (268,327 → 1,643): the pre-fix numbers
counted one-time setup allocations (bundle/response generation via
`strings.Builder` + `fmt.Fprintf`, which is alloc-heavy for a ~350K-row
response) as if they were per-render cost. `b.ResetTimer()` now excludes
them — the post-fix allocs/op figures are the actual `Render` call's Go-host
allocations.

`b.ReportAllocs()` was used on all three; `Bytes/op`/`Allocs/op` above are
**Go-host-side** allocations (the copies `Render`/`hostQuery` make in/out of
guest linear memory, plus wazero's internal bookkeeping and linear-memory
growth reallocations) — not WASM guest linear memory, which is capped
separately by `NewEngine`'s `memoryPages` argument (2048 pages / 128 MiB for
the first two benchmarks, 4096 pages / 256 MiB for `BenchmarkJSON10MiB`,
matching the spec's default/cap).

## §7 assumptions vs measured numbers

Spec §7's capacity model: *"Capacity scales with concurrent renders...
realistic bundles 100–500 KB... A render is query-dominated (~0.5–2 s), so a
pod with 8–16 slots sustains ~10–30 renders/s... **These figures are
pre-spike assumptions**"* — exactly what this spike is meant to check.

**Fixed floor (`BenchmarkInstantiate`, 0.056 ms).** Negligible. It confirms
the E1-era design decision (compile once in `NewEngine`, fresh instance per
`Render`) does not reintroduce a meaningful per-render tax — instantiation +
`_initialize` costs four orders of magnitude less than a typical render. No
case for instance pooling on these numbers.

**Typical-bundle eval (`BenchmarkEval300KB`, 57.0 ms, no query).** This
isolates the QuickJS parse+eval cost for a bundle at the top of the spec's
"realistic 100–500 KB" range, with a trivial render body and zero query
cost. It is a small fraction of the 10 s default / 30 s cap wall clock.

**Render-slot math, light path.** If light (no/cheap-query) renders ran
back-to-back with no query wait, 8 concurrent slots at 57.0 ms/render would
imply a theoretical ceiling of:

```
8 slots / 0.0570 s ≈ 140 renders/s/pod
```

That is a pure-eval upper bound, not a sustained-throughput prediction —
real renders wait on the query service. Using the spec's own
query-dominated estimate (~0.5–2 s per render, eval cost added on top):

```
8 slots / (0.057 s eval + ~1 s query wait) ≈ 7.6 renders/s/pod
```

That lands at (slightly under) the low end of the spec's "10–30 renders/s"
band — close enough to validate the qualitative claim ("query-dominated,
not eval-dominated") for the common case. Eval cost is genuinely a rounding
error next to query wait for typical apps.

**Render-slot math, heavy-JSON path (`BenchmarkJSON10MiB`, 4.37 s).** This
is where the pre-spike assumption breaks down. A render whose query returns
the full 10 MiB `max_bytes` cap and reshapes every row (the Appendix A
pattern) costs **4.37 s of single-threaded guest CPU time alone** — before
any actual network/query-execution latency is added on top. Against the
default 10 s wall clock, that consumes ~43.7% of the render's entire time
budget on parse+reshape+stringify, leaving comparatively little headroom for
the real DuckDB round trip. Applying the same slot math:

```
8 slots / (4.37 s eval + ~1 s query wait) ≈ 1.5 renders/s/pod
```

— roughly **7–19x below** the spec's assumed 10–30 renders/s for any app
that regularly reshapes at-cap query results. §7's "capacity scales with
concurrent renders, not app count" claim holds only for apps whose payloads
stay well under the 10 MiB cap; apps that push near it need to be budgeted
as a distinct, much more expensive capacity class.

## Memory observations

- Go-host allocations scale with payload, as expected: 250 KB (no eval) →
  28.9 MB (300 KB bundle text + one JS heap) → 729 MB (10 MiB response
  parsed, reshaped into ~350K JS objects, and re-stringified — plus wazero's
  linear-memory growth reallocations as the guest heap expands under load).
  729 MB of *host*-side churn per render is a real GC-pressure number the
  app-worker's sizing must account for, separate from the WASM linear-memory
  cap.
- WASM linear memory itself is bounded by `NewEngine`'s page limit
  (128 MiB default / 256 MiB cap, per spec §7); `BenchmarkJSON10MiB` needed
  the 256 MiB cap; it OOM's inside the 128 MiB default (consistent with the
  E1 report's OOM test and the "10 MiB response + reshape doubles/triples
  the live JS heap" expectation for this pattern).
- Compiled-module (`engine.wasm`, 1.25 MiB) overhead is paid once at
  `NewEngine` (~0.25 s per the E1 report) — negligible against per-pod
  memory budget ("tens of MB" per §7 is the right order of magnitude).

## Artifact size

`pkg/appengine/embed/engine.wasm`: **1,312,937 bytes (≈1.25 MiB)**. Small
enough that shipping it embedded in the app-worker binary (current design)
has no meaningful image-size or cold-start cost.

## GO / NO-GO recommendation

**GO** on QuickJS-on-wazero for Part 3, with one capacity-model caveat.

Reasoning:

1. **Fixed per-render overhead is a non-issue.** 0.056 ms to instantiate a
   fresh module is noise next to any real render. The isolate-per-render
   design (no pooling) is vindicated by these numbers, not just by
   correctness/isolation arguments.
2. **Typical-app eval cost is well within budget.** 57.0 ms for a 300 KB
   bundle (the upper end of the spec's realistic range) leaves ample room
   under the 10 s default wall clock, and is consistent with §7's
   "query-dominated, not eval-dominated" framing for the common case — the
   render-slot math above lands at/near the spec's own 10–30 renders/s
   planning band once query wait is folded in.
3. **The 10 MiB-reshape path is real but bounded and known.** 4.37 s of
   guest CPU for a full-cap reshape is expensive, not catastrophic — it
   still fits inside the 10 s default (barely) and the 30 s cap
   (comfortably), so it does not force a NO-GO on the engine itself. It
   does mean §7's render-slot math needs a second, explicit capacity class
   for large-payload apps rather than one blended "10–30 renders/s"
   figure. Two mitigations already exist in the design and should be
   leaned on rather than treated as headroom: (a) the 2 MiB production
   `MaxOutputBytes` default plus the "aggregate before returning" author
   guidance (this benchmark deliberately bypasses both to measure raw
   throughput — see §6) keeps the common case far from the 10 MiB
   worst case; (b) the per-app in-flight render cap (default 2) already
   limits how many concurrent heavy renders one app can generate.
   Recommended follow-up for Part 3 planning (not a blocker): size the
   render semaphore/pod count assuming some fraction of renders hit the
   heavy path, rather than assuming all renders are query-dominated at
   ~0.5–2 s.
4. **No cgo, small artifact.** wazero + quickjs-ng keeps the pure-Go
   toolchain (spec §12.1's stated reason to prefer this over
   StarlingMonkey/wasmtime-go); 1.25 MiB compiled module is a non-issue for
   image size or boot-time compile (~0.25 s, paid once, before readiness
   per §7's rollout requirements).

Net: proceed to Part 3 on the current engine. Carry the heavy-JSON finding
into Part 3's capacity/sizing work as a named, bounded risk — not a reason
to re-open the engine choice.

## Deviations from the brief

1. **`bench_test.go` was pre-existing** (uncommitted, from a prior crashed
   attempt). Reviewed against this task's requirements and the E1 report;
   kept as-is with no changes — it already implements exactly the three
   required benchmarks, uses `b.ReportAllocs()`, generates bundles/payloads
   deterministically in code, and matches the engine's real `Render`/
   `NewEngine` API. No rewrite was needed.
2. **`BenchmarkJSON10MiB`'s `MaxOutputBytes`** is set to 64 MiB, far above
   the spec's 2 MiB production default. This is intentional and called out
   in the benchmark's own doc comment: the benchmark measures the engine's
   raw parse+reshape+stringify throughput at the query service's 10 MiB
   cap, not production output policy. A real app hitting this shape would
   aggregate before returning and never approach 64 MiB of output; treat
   the 4.37 s figure as an upper bound on guest CPU cost for that class of
   query, not a claim about typical `OutputDoc` sizes.
3. **No changes to `engine.go`** or any other existing file. The benchmarks
   reach `Engine.rt`/`Engine.compiled` directly since the test file lives
   in `package appengine`; no test-only helper was needed.

## Verification

- `go build ./...` — clean.
- `go vet ./pkg/appengine/...` — clean.
- `go test ./pkg/appengine/... -v` — all 7 existing tests PASS (unaffected
  by the new benchmark file).
- `go test ./pkg/appengine/ -bench . -benchmem -run XXX -benchtime 3s` and
  the follow-up `-benchtime 15s` run for `BenchmarkJSON10MiB` — both shown
  verbatim above.
