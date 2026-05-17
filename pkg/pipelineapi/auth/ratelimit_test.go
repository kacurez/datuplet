package auth_test

import (
	"testing"
	"time"

	"github.com/datuplet/datuplet/pkg/pipelineapi/auth"
)

// TestLimiter_AllowsUpToRate — the canonical positive test: the first N
// requests in a window MUST succeed, request N+1 MUST be denied.
func TestLimiter_AllowsUpToRate(t *testing.T) {
	l := auth.NewLimiter(3, time.Minute)
	const key = "1.2.3.4"
	for i := 0; i < 3; i++ {
		if !l.Allow(key) {
			t.Errorf("Allow #%d: false, want true", i+1)
		}
	}
	if l.Allow(key) {
		t.Error("Allow #4: true, want false (over rate)")
	}
}

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

// TestLimiter_DistinctKeysIndependent — limit is per-key. Two different
// IPs MUST NOT share a bucket; otherwise NAT'd users would lock each
// other out.
func TestLimiter_DistinctKeysIndependent(t *testing.T) {
	l := auth.NewLimiter(1, time.Minute)
	if !l.Allow("a") || !l.Allow("b") {
		t.Error("first request from each key should be allowed")
	}
	if l.Allow("a") {
		t.Error("second from 'a' should be denied")
	}
	if l.Allow("b") {
		t.Error("second from 'b' should be denied")
	}
}

// TestLimiter_EmptyKeyIsTreatedNormally — empty string is still a valid
// bucket key (matters when we fail-open on a malformed RemoteAddr).
func TestLimiter_EmptyKeyIsTreatedNormally(t *testing.T) {
	l := auth.NewLimiter(2, time.Minute)
	if !l.Allow("") || !l.Allow("") {
		t.Error("first two with empty key should be allowed")
	}
	if l.Allow("") {
		t.Error("third with empty key should be denied")
	}
}

// TestLimiter_ZeroRateAlwaysDenies — defensive guard: a misconfigured
// `rate=0` produces a closed bucket. Better than uncapped since the
// alternative (uncapped) means the operator never realizes the limiter
// is broken.
func TestLimiter_ZeroRateAlwaysDenies(t *testing.T) {
	l := auth.NewLimiter(0, time.Minute)
	if l.Allow("any") {
		t.Error("zero rate should always deny")
	}
}
