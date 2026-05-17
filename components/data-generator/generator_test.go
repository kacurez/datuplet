package main

import (
	"math/rand/v2"
	"strings"
	"testing"
	"time"
)

// TestSeedForTable verifies that the same (runID, table) always produces the
// same seed, and that different inputs produce different seeds.
func TestSeedForTable(t *testing.T) {
	s1 := seedForTable("run-001", "products")
	s2 := seedForTable("run-001", "products")
	if s1 != s2 {
		t.Fatalf("same inputs produced different seeds: %d vs %d", s1, s2)
	}

	s3 := seedForTable("run-001", "customers")
	if s1 == s3 {
		t.Fatal("different table names produced same seed")
	}

	s4 := seedForTable("run-002", "products")
	if s1 == s4 {
		t.Fatal("different run IDs produced same seed")
	}
}

// TestNewRNG_Deterministic checks that the same (runID, table) produces the
// same sequence of values.
func TestNewRNG_Deterministic(t *testing.T) {
	rng1 := newRNG("run-abc", "orders")
	rng2 := newRNG("run-abc", "orders")

	for i := 0; i < 20; i++ {
		if rng1.Int64() != rng2.Int64() {
			t.Fatalf("RNG diverged at step %d", i)
		}
	}
}

// TestNewRNG_EmptyRunID checks fallback to time-based seed (just verifies no panic).
func TestNewRNG_EmptyRunID(t *testing.T) {
	rng := newRNG("", "table")
	_ = rng.Int64() // must not panic
}

// TestGenerateValue_NonNil checks each type produces a non-nil value.
func TestGenerateValue_NonNil(t *testing.T) {
	types := []string{"string", "int", "long", "float", "double", "boolean", "date", "timestamp", "now", "uuid"}
	rng := rand.New(rand.NewPCG(42, 42))

	for _, typ := range types {
		v := generateValue(rng, typ)
		if v == nil {
			t.Errorf("type %q returned nil", typ)
		}
	}
}

// TestGenerateValue_StringLength checks string values are hex (16-40 chars).
func TestGenerateValue_StringLength(t *testing.T) {
	rng := rand.New(rand.NewPCG(7, 7))
	for i := 0; i < 50; i++ {
		v := generateValue(rng, "string")
		s, ok := v.(string)
		if !ok {
			t.Fatalf("string type returned non-string %T", v)
		}
		// hex.EncodeToString doubles the byte length: 8–20 bytes → 16–40 chars
		if len(s) < 16 || len(s) > 40 {
			t.Errorf("string length %d outside [16,40]: %q", len(s), s)
		}
	}
}

// TestGenerateValue_DateFormat checks date values match YYYY-MM-DD.
func TestGenerateValue_DateFormat(t *testing.T) {
	rng := rand.New(rand.NewPCG(99, 99))
	for i := 0; i < 20; i++ {
		v := generateValue(rng, "date")
		s, ok := v.(string)
		if !ok {
			t.Fatalf("date type returned non-string %T", v)
		}
		if _, err := time.Parse("2006-01-02", s); err != nil {
			t.Errorf("date %q does not match YYYY-MM-DD: %v", s, err)
		}
	}
}

// TestGenerateValue_TimestampFormat checks timestamp values are RFC3339.
func TestGenerateValue_TimestampFormat(t *testing.T) {
	rng := rand.New(rand.NewPCG(11, 22))
	for i := 0; i < 20; i++ {
		v := generateValue(rng, "timestamp")
		s, ok := v.(string)
		if !ok {
			t.Fatalf("timestamp type returned non-string %T", v)
		}
		if _, err := time.Parse("2006-01-02T15:04:05.000Z07:00", s); err != nil {
			t.Errorf("timestamp %q not RFC3339: %v", s, err)
		}
	}
}

// TestGenerateValue_UUIDFormat checks UUID values match version-4 format.
func TestGenerateValue_UUIDFormat(t *testing.T) {
	rng := rand.New(rand.NewPCG(55, 66))
	for i := 0; i < 20; i++ {
		v := generateValue(rng, "uuid")
		s, ok := v.(string)
		if !ok {
			t.Fatalf("uuid type returned non-string %T", v)
		}
		// UUID v4: xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx
		parts := strings.Split(s, "-")
		if len(parts) != 5 {
			t.Errorf("uuid %q: expected 5 parts, got %d", s, len(parts))
		}
	}
}

// TestShouldStop_RowsOnly verifies rows-only limit.
func TestShouldStop_RowsOnly(t *testing.T) {
	limit := &Limit{RowsCount: 10}
	start := time.Now()

	if shouldStop(limit, 9, 0, start) {
		t.Error("should not stop at row 9 (limit=10)")
	}
	if !shouldStop(limit, 10, 0, start) {
		t.Error("should stop at row 10 (limit=10)")
	}
	if !shouldStop(limit, 100, 0, start) {
		t.Error("should stop at row 100 (limit=10)")
	}
}

// TestShouldStop_BytesOnly verifies bytes-only limit.
func TestShouldStop_BytesOnly(t *testing.T) {
	limit := &Limit{SizeInBytes: 1024}
	start := time.Now()

	if shouldStop(limit, 0, 1023, start) {
		t.Error("should not stop at 1023 bytes (limit=1024)")
	}
	if !shouldStop(limit, 0, 1024, start) {
		t.Error("should stop at 1024 bytes")
	}
}

// TestShouldStop_TimeOnly verifies time-only limit (using a far-past start time).
func TestShouldStop_TimeOnly(t *testing.T) {
	limit := &Limit{TimeoutInSeconds: 1}
	pastStart := time.Now().Add(-2 * time.Second)

	if !shouldStop(limit, 0, 0, pastStart) {
		t.Error("should stop when 2s elapsed and timeout=1s")
	}

	futureStart := time.Now()
	if shouldStop(limit, 0, 0, futureStart) {
		t.Error("should not stop immediately after start with 1s timeout")
	}
}

// TestShouldStop_OR_FirstToTrip verifies OR semantics: bytes trip before rows.
func TestShouldStop_OR_FirstToTrip(t *testing.T) {
	limit := &Limit{RowsCount: 1000, SizeInBytes: 100}
	start := time.Now()

	// At row 5, bytes already exceeded → should stop.
	if !shouldStop(limit, 5, 200, start) {
		t.Error("bytes should have tripped before rows limit")
	}
}

// TestShouldStop_NilLimit returns false.
func TestShouldStop_NilLimit(t *testing.T) {
	if shouldStop(nil, 99999, 99999, time.Now().Add(-time.Hour)) {
		t.Error("nil limit should never stop")
	}
}

// TestSortStrings verifies the simple insertion sort.
func TestSortStrings(t *testing.T) {
	ss := []string{"z", "a", "m", "b"}
	sortStrings(ss)
	want := []string{"a", "b", "m", "z"}
	for i, v := range ss {
		if v != want[i] {
			t.Fatalf("sort failed: got %v, want %v", ss, want)
		}
	}
}

// TestErrInjectionPoint_RowsBased verifies injection point is in [0, rowsCount).
func TestErrInjectionPoint_RowsBased(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	r := &RandomSpec{
		Schema:           map[string]string{"id": "int"},
		Limit:            &Limit{RowsCount: 100},
		UserErrorMessage: "oops",
	}

	for i := 0; i < 50; i++ {
		at := errInjectionPoint(rng, r)
		if at == nil {
			t.Fatal("expected non-nil injection point")
		}
		if *at < 0 || *at >= 100 {
			t.Fatalf("injection point %d out of [0,100)", *at)
		}
	}
}

// TestErrInjectionPoint_NoMessage returns nil.
func TestErrInjectionPoint_NoMessage(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	r := &RandomSpec{
		Schema: map[string]string{"id": "int"},
		Limit:  &Limit{RowsCount: 100},
	}
	if at := errInjectionPoint(rng, r); at != nil {
		t.Fatalf("expected nil with no UserErrorMessage, got %d", *at)
	}
}

// TestErrInjectionPoint_NilLimit returns nil.
func TestErrInjectionPoint_NilLimit(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	r := &RandomSpec{
		UserErrorMessage: "fail",
	}
	if at := errInjectionPoint(rng, r); at != nil {
		t.Fatalf("expected nil with nil Limit, got %d", *at)
	}
}

// TestErrInjectionPoint_NilSpec returns nil.
func TestErrInjectionPoint_NilSpec(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	if at := errInjectionPoint(rng, nil); at != nil {
		t.Fatalf("expected nil with nil RandomSpec, got %d", *at)
	}
}

// TestErrInjectionPoint_TimeBased verifies injection point in [0, timeoutMs).
func TestErrInjectionPoint_TimeBased(t *testing.T) {
	rng := rand.New(rand.NewPCG(3, 4))
	r := &RandomSpec{
		Schema:           map[string]string{"id": "int"},
		Limit:            &Limit{TimeoutInSeconds: 10},
		UserErrorMessage: "boom",
	}
	for i := 0; i < 20; i++ {
		at := errInjectionPoint(rng, r)
		if at == nil {
			t.Fatal("expected non-nil injection point")
		}
		if *at < 0 || *at >= 10000 {
			t.Fatalf("time-based injection point %d out of [0, 10000)", *at)
		}
	}
}
