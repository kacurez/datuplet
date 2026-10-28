package main

import (
	"fmt"
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

// TestGenerateValue checks each column type produces a non-nil value, and
// that string/date/timestamp/uuid values have the documented format.
func TestGenerateValue(t *testing.T) {
	asString := func(v any) (string, string) {
		s, ok := v.(string)
		if !ok {
			return "", fmt.Sprintf("returned non-string %T", v)
		}
		return s, ""
	}
	cases := []struct {
		name  string
		typ   string
		seed  [2]uint64
		iters int
		// check returns a failure message ("" when the value is fine); nil
		// means only the non-nil check below applies.
		check func(v any) string
	}{
		{name: "string", typ: "string", seed: [2]uint64{7, 7}, iters: 50, check: func(v any) string {
			s, msg := asString(v)
			if msg != "" {
				return msg
			}
			// hex.EncodeToString doubles the byte length: 8-20 bytes -> 16-40 chars
			if len(s) < 16 || len(s) > 40 {
				return fmt.Sprintf("length %d outside [16,40]: %q", len(s), s)
			}
			return ""
		}},
		{name: "date", typ: "date", seed: [2]uint64{99, 99}, iters: 20, check: func(v any) string {
			s, msg := asString(v)
			if msg != "" {
				return msg
			}
			if _, err := time.Parse("2006-01-02", s); err != nil {
				return fmt.Sprintf("%q does not match YYYY-MM-DD: %v", s, err)
			}
			return ""
		}},
		{name: "timestamp", typ: "timestamp", seed: [2]uint64{11, 22}, iters: 20, check: func(v any) string {
			s, msg := asString(v)
			if msg != "" {
				return msg
			}
			if _, err := time.Parse("2006-01-02T15:04:05.000Z07:00", s); err != nil {
				return fmt.Sprintf("%q not RFC3339: %v", s, err)
			}
			return ""
		}},
		{name: "uuid", typ: "uuid", seed: [2]uint64{55, 66}, iters: 20, check: func(v any) string {
			s, msg := asString(v)
			if msg != "" {
				return msg
			}
			// UUID v4: xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx
			if parts := strings.Split(s, "-"); len(parts) != 5 {
				return fmt.Sprintf("%q: expected 5 parts, got %d", s, len(parts))
			}
			return ""
		}},
		{name: "int", typ: "int", seed: [2]uint64{42, 42}, iters: 1, check: nil},
		{name: "long", typ: "long", seed: [2]uint64{42, 42}, iters: 1, check: nil},
		{name: "float", typ: "float", seed: [2]uint64{42, 42}, iters: 1, check: nil},
		{name: "double", typ: "double", seed: [2]uint64{42, 42}, iters: 1, check: nil},
		{name: "boolean", typ: "boolean", seed: [2]uint64{42, 42}, iters: 1, check: nil},
		{name: "now", typ: "now", seed: [2]uint64{42, 42}, iters: 1, check: nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rng := rand.New(rand.NewPCG(tc.seed[0], tc.seed[1]))
			for i := 0; i < tc.iters; i++ {
				v := generateValue(rng, tc.typ)
				if v == nil {
					t.Fatalf("type %q returned nil", tc.typ)
				}
				if tc.check == nil {
					continue
				}
				if msg := tc.check(v); msg != "" {
					t.Errorf("type %q: %s", tc.typ, msg)
				}
			}
		})
	}
}

// TestShouldStop verifies each limit kind (rows, bytes, time) and the nil limit.
// OR semantics are pinned separately by TestShouldStop_OR_FirstToTrip.
func TestShouldStop(t *testing.T) {
	cases := []struct {
		name    string
		limit   *Limit
		rows    int
		bytes   int
		elapsed time.Duration
		want    bool
	}{
		{"rows: below limit", &Limit{RowsCount: 10}, 9, 0, 0, false},
		{"rows: at limit", &Limit{RowsCount: 10}, 10, 0, 0, true},
		{"rows: above limit", &Limit{RowsCount: 10}, 100, 0, 0, true},
		{"bytes: below limit", &Limit{SizeInBytes: 1024}, 0, 1023, 0, false},
		{"bytes: at limit", &Limit{SizeInBytes: 1024}, 0, 1024, 0, true},
		{"time: elapsed past timeout", &Limit{TimeoutInSeconds: 1}, 0, 0, 2 * time.Second, true},
		{"time: just started", &Limit{TimeoutInSeconds: 1}, 0, 0, 0, false},
		{"nil limit never stops", nil, 99999, 99999, time.Hour, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := shouldStop(tc.limit, tc.rows, tc.bytes, time.Now().Add(-tc.elapsed)); got != tc.want {
				t.Errorf("shouldStop = %v, want %v", got, tc.want)
			}
		})
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

// TestErrInjectionPoint verifies the injection point is in range for rows- and
// time-based limits, and nil when no error is configured or the spec is unusable.
func TestErrInjectionPoint(t *testing.T) {
	cases := []struct {
		name  string
		seed  [2]uint64
		spec  *RandomSpec
		iters int
		// wantNil: expect no injection point; otherwise it must be in [0, max).
		wantNil bool
		max     int
	}{
		{
			name: "rows based in [0, rowsCount)",
			seed: [2]uint64{1, 2},
			spec: &RandomSpec{
				Schema:           map[string]string{"id": "int"},
				Limit:            &Limit{RowsCount: 100},
				UserErrorMessage: "oops",
			},
			iters: 50,
			max:   100,
		},
		{
			name:    "no message",
			seed:    [2]uint64{1, 2},
			spec:    &RandomSpec{Schema: map[string]string{"id": "int"}, Limit: &Limit{RowsCount: 100}},
			iters:   1,
			wantNil: true,
		},
		{
			name:    "nil limit",
			seed:    [2]uint64{1, 2},
			spec:    &RandomSpec{UserErrorMessage: "fail"},
			iters:   1,
			wantNil: true,
		},
		{name: "nil spec", seed: [2]uint64{1, 2}, spec: nil, iters: 1, wantNil: true},
		{
			name: "time based in [0, timeoutMs)",
			seed: [2]uint64{3, 4},
			spec: &RandomSpec{
				Schema:           map[string]string{"id": "int"},
				Limit:            &Limit{TimeoutInSeconds: 10},
				UserErrorMessage: "boom",
			},
			iters: 20,
			max:   10000,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rng := rand.New(rand.NewPCG(tc.seed[0], tc.seed[1]))
			for i := 0; i < tc.iters; i++ {
				at := errInjectionPoint(rng, tc.spec)
				if tc.wantNil {
					if at != nil {
						t.Fatalf("expected nil injection point, got %d", *at)
					}
					continue
				}
				if at == nil {
					t.Fatal("expected non-nil injection point")
				}
				if *at < 0 || *at >= tc.max {
					t.Fatalf("injection point %d out of [0,%d)", *at, tc.max)
				}
			}
		})
	}
}
