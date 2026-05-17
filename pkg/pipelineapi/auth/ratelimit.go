package auth

import (
	"sync"
	"time"
)

// Limiter is a naive per-key fixed-window rate limiter. POC-grade; we
// pick fixed-window over a true token bucket for two reasons:
//
//  1. The handler that uses this (POST /api/v1/auth/token)
//     accepts ≤10 req/min/IP — at that rate, the burst-vs-smoothing
//     distinction is meaningless.
//  2. Memory accounting is trivial: each (key) row carries one int + one
//     timestamp. We don't sweep the map; the bucket is reset in-place on
//     the first request after the window expires.
//
// Limitations (documented for the security reviewer):
//
//   - Memory grows with the number of distinct keys seen during a single
//     window. With key=client-IP and a steady-state attacker rotating
//     through a /24 (256 IPs), the map peaks at ~256 entries — bounded.
//     A bot net spraying from /16 (65k IPs) would peak at ~65k entries
//     × ~32 bytes = ~2MB — still fine. We do NOT defend against a
//     deliberate /0 spray; for that, run pipeline-api behind a real
//     reverse proxy with global per-IP limits.
//   - Stale entries are NOT swept until the key is observed again. For
//     a sustained-attack scenario followed by a long quiet period, the
//     map retains the keys until the process restarts. POC-acceptable.
//   - A reverse proxy in front of pipeline-api will collapse all client
//     IPs to the proxy's IP unless the caller forwards the X-Forwarded-For
//     header AND the limiter keys on it. This implementation keys on
//     whatever string the caller passes — the *handler* is responsible
//     for picking the right key (cli_token_handler.go strips the port
//     from r.RemoteAddr).
//
// Concurrency: a single mutex guards the whole map. At 10 req/min/IP
// over hundreds of distinct IPs, contention is negligible. If this
// limiter ever moves to a hot path, switch to a sharded map.
type Limiter struct {
	mu      sync.Mutex
	buckets map[string]*bucket
	rate    int
	window  time.Duration
}

type bucket struct {
	count int
	reset time.Time
}

// NewLimiter constructs a fresh per-key fixed-window limiter. `rate` is
// the maximum number of requests permitted per `window`. A `rate` of 0
// produces a closed bucket — every Allow call returns false; a `window`
// of 0 makes the limiter degenerate to "always allow up to `rate` ever",
// which is rarely what you want, so use a sensible non-zero window.
func NewLimiter(rate int, window time.Duration) *Limiter {
	return &Limiter{
		buckets: make(map[string]*bucket),
		rate:    rate,
		window:  window,
	}
}

// Allow returns true if the request bound to `key` is permitted under
// the configured rate, and false if the bucket is full. Side-effect: a
// returned `true` consumes one permit from the key's bucket.
func (l *Limiter) Allow(key string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.rate <= 0 {
		return false
	}
	now := time.Now()
	b, ok := l.buckets[key]
	if !ok || now.After(b.reset) {
		l.buckets[key] = &bucket{count: 1, reset: now.Add(l.window)}
		return true
	}
	if b.count >= l.rate {
		return false
	}
	b.count++
	return true
}
