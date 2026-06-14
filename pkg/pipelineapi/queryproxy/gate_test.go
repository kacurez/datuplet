package queryproxy

import "testing"

func TestGate_AcquireUpToCap(t *testing.T) {
	g := newGate(2)
	if !g.Acquire("alice") {
		t.Fatal("first acquire should succeed")
	}
	if !g.Acquire("alice") {
		t.Fatal("second acquire (at cap) should succeed")
	}
	if g.Acquire("alice") {
		t.Fatal("third acquire should fail — cap exhausted")
	}
}

func TestGate_ReleaseFreesSlot(t *testing.T) {
	g := newGate(1)
	if !g.Acquire("alice") {
		t.Fatal("first acquire should succeed")
	}
	if g.Acquire("alice") {
		t.Fatal("second acquire should fail before release")
	}
	g.Release("alice")
	if !g.Acquire("alice") {
		t.Fatal("acquire after release should succeed")
	}
}

func TestGate_IndependentPrincipals(t *testing.T) {
	g := newGate(1)
	if !g.Acquire("alice") {
		t.Fatal("alice acquire should succeed")
	}
	// bob has his own budget; alice's saturation must not block him.
	if !g.Acquire("bob") {
		t.Fatal("bob acquire should succeed — independent principal")
	}
	if g.Acquire("alice") {
		t.Fatal("alice second acquire should fail — her cap is 1")
	}
}

func TestGate_ReleaseDeletesEntryAtZero(t *testing.T) {
	g := newGate(2)
	g.Acquire("alice")
	g.Acquire("alice")
	g.Release("alice")
	g.Release("alice")

	g.mu.Lock()
	_, present := g.inflight["alice"]
	g.mu.Unlock()
	if present {
		t.Fatal("map entry should be deleted once in-flight count reaches zero (no unbounded growth)")
	}
}

func TestGate_NonPositiveCapCoercedToOne(t *testing.T) {
	g := newGate(0)
	if !g.Acquire("alice") {
		t.Fatal("first acquire should succeed even with cap=0 input")
	}
	if g.Acquire("alice") {
		t.Fatal("second acquire should fail — coerced cap is 1")
	}
}

func TestGate_ReleaseUnknownPrincipalIsNoop(t *testing.T) {
	g := newGate(1)
	// Should not panic or corrupt state.
	g.Release("nobody")
	if !g.Acquire("nobody") {
		t.Fatal("acquire after spurious release should still succeed")
	}
}
