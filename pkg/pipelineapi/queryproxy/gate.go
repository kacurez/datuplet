package queryproxy

import "sync"

// gate is the per-principal in-flight query cap (RFC 022 §5.2): at most
// `cap` concurrent queries per principal (the authenticated subject UUID).
// It is distinct from the query-worker's own per-pod admission semaphore
// (§5.1) — this gate bounds a single caller, the worker bounds the pod.
//
// The map only holds principals with ≥1 in-flight query; Release deletes
// the entry at zero so a long-lived process serving many distinct callers
// does not accumulate unbounded map entries.
type gate struct {
	mu       sync.Mutex
	inflight map[string]int
	cap      int
}

// newGate returns a gate with the given per-principal cap. A non-positive
// cap is coerced to 1 so the gate never admits an unbounded number of
// concurrent queries for a principal.
func newGate(capacity int) *gate {
	if capacity < 1 {
		capacity = 1
	}
	return &gate{
		inflight: make(map[string]int),
		cap:      capacity,
	}
}

// Acquire reserves an in-flight slot for sub. It returns false (without
// reserving) when sub already holds `cap` slots. Each successful Acquire
// must be paired with exactly one Release.
func (g *gate) Acquire(sub string) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.inflight[sub] >= g.cap {
		return false
	}
	g.inflight[sub]++
	return true
}

// Release frees one in-flight slot for sub. When sub's count drops to zero
// the map entry is deleted to bound memory. Release on a principal with no
// outstanding slots is a no-op (defensive — should not happen given the
// Acquire/Release pairing).
func (g *gate) Release(sub string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	n := g.inflight[sub]
	if n <= 1 {
		delete(g.inflight, sub)
		return
	}
	g.inflight[sub] = n - 1
}
