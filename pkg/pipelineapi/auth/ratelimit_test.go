package auth_test

import (
	"testing"
	"time"

	"github.com/datuplet/datuplet/pkg/pipelineapi/auth"
)

// TestLimiter_ResetsAfterWindow — windows MUST be sliding/expiring; once
// the window passes, the bucket refills. We use a small window so the
// test is fast and deterministic.
func TestLimiter_ResetsAfterWindow(t *testing.T) {
	const window = 50 * time.Millisecond
	l := auth.NewLimiter(2, window)
	const key = "key"
	if !l.Allow(key) || !l.Allow(key) {
		t.Fatal("first two should be allowed")
	}
	if l.Allow(key) {
		t.Fatal("third should be denied")
	}
	time.Sleep(window + 20*time.Millisecond)
	if !l.Allow(key) {
		t.Error("after window: should be allowed again")
	}
}

// TestLimiter_Allow: the first N requests in a window MUST succeed and
// request N+1 MUST be denied; the limit is per-key (two different IPs MUST
// NOT share a bucket, otherwise NAT'd users would lock each other out); the
// empty string is still a valid bucket key (matters when we fail-open on a
// malformed RemoteAddr); and a misconfigured rate=0 produces a closed bucket,
// better than uncapped since the operator would never realize the limiter is
// broken.
func TestLimiter_Allow(t *testing.T) {
	cases := []struct {
		name string
		rate int
		keys []string
		want []bool
	}{
		{"allows_up_to_rate", 3, []string{"1.2.3.4", "1.2.3.4", "1.2.3.4", "1.2.3.4"}, []bool{true, true, true, false}},
		{"distinct_keys_independent", 1, []string{"a", "b", "a", "b"}, []bool{true, true, false, false}},
		{"empty_key_is_treated_normally", 2, []string{"", "", ""}, []bool{true, true, false}},
		{"zero_rate_always_denies", 0, []string{"any"}, []bool{false}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			l := auth.NewLimiter(tc.rate, time.Minute)
			for i, key := range tc.keys {
				if got := l.Allow(key); got != tc.want[i] {
					t.Errorf("Allow #%d (key %q) = %v, want %v", i+1, key, got, tc.want[i])
				}
			}
		})
	}
}
