package queryproxy

import "testing"

func TestGate(t *testing.T) {
	t.Run("acquire_up_to_cap", func(t *testing.T) {
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
	})

	t.Run("release_frees_slot", func(t *testing.T) {
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
	})

	t.Run("independent_principals", func(t *testing.T) {
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
	})

	t.Run("release_deletes_entry_at_zero", func(t *testing.T) {
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
	})

	t.Run("non_positive_cap_coerced_to_one", func(t *testing.T) {
		g := newGate(0)
		if !g.Acquire("alice") {
			t.Fatal("first acquire should succeed even with cap=0 input")
		}
		if g.Acquire("alice") {
			t.Fatal("second acquire should fail — coerced cap is 1")
		}
	})

	t.Run("release_unknown_principal_is_noop", func(t *testing.T) {
		g := newGate(1)
		// Should not panic or corrupt state.
		g.Release("nobody")
		if !g.Acquire("nobody") {
			t.Fatal("acquire after spurious release should still succeed")
		}
	})
}
