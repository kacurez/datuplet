package catalogwriter

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/apache/iceberg-go/catalog/rest"
)

// DefaultMaxRetries is the bounded retry limit for commit-on-409 loops.
// After this many attempts the last error is surfaced to the caller
// (typically TableCommit, which exits non-zero → pipeline-operator
// marks PipelineRun FailedApplication).
const DefaultMaxRetries = 5

// DefaultBaseBackoff is the starting backoff for the exponential
// schedule. Backoffs are 100ms, 200ms, 400ms, 800ms, 1600ms before the
// cap. ±10% jitter is applied per attempt.
const DefaultBaseBackoff = 100 * time.Millisecond

// IsCommitConflict returns true if err is iceberg-go's REST 409
// ErrCommitFailed (or wraps it). Use this to gate retry logic in
// callers that need finer control than RetryOnConflict provides.
func IsCommitConflict(err error) bool {
	return errors.Is(err, rest.ErrCommitFailed)
}

// RetryOpts tunes RetryOnConflict.
type RetryOpts struct {
	// MaxAttempts caps the number of tries. Must be >=1; values <=0
	// fall back to DefaultMaxRetries. The first call is attempt 1.
	MaxAttempts int

	// BaseBackoff is the starting sleep between attempts.
	BaseBackoff time.Duration

	// Now is the time source. Tests inject; production leaves nil.
	// (Currently only used for jitter seeding so tests can produce
	// deterministic schedules; the actual sleep uses real time.)
	Now func() time.Time

	// Sleep is the sleep function. Tests inject a no-op; production
	// leaves nil and the implementation uses time.Sleep with context
	// cancellation honored via select.
	Sleep func(ctx context.Context, d time.Duration) error
}

// RetryOnConflict invokes fn, retrying on iceberg-go's
// `rest.ErrCommitFailed` (HTTP 409) up to opts.MaxAttempts times.
// Backoff schedule: 100ms, 200ms, 400ms, ... with ±10% jitter, capped
// implicitly by MaxAttempts.
//
// Returns nil on first success. On final failure returns the last
// error wrapped with the attempt count so operator logs surface the
// retry burn.
//
// Non-409 errors short-circuit immediately — we don't retry e.g.
// "table not found" or "auth failed", because those won't go away
// with a retry.
//
// ctx cancellation is honoured between attempts.
func RetryOnConflict(ctx context.Context, opts RetryOpts, fn func(ctx context.Context) error) error {
	max := opts.MaxAttempts
	if max <= 0 {
		max = DefaultMaxRetries
	}
	base := opts.BaseBackoff
	if base <= 0 {
		base = DefaultBaseBackoff
	}
	sleep := opts.Sleep
	if sleep == nil {
		sleep = sleepWithContext
	}
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	// Mix PID into the seed so two pods that boot within the same
	// nanosecond don't end up replaying identical jitter schedules.
	rng := rand.New(rand.NewSource(now().UnixNano() ^ int64(os.Getpid()))) //nolint:gosec // jitter, not crypto

	var lastErr error
	for attempt := 1; attempt <= max; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := fn(ctx)
		if err == nil {
			return nil
		}
		lastErr = err
		if !IsCommitConflict(err) {
			return err
		}
		if attempt == max {
			break
		}
		// Exponential: base * 2^(attempt-1), then ±10% jitter. The
		// previous formula `time.Duration(rng.Float64()*0.2-0.1) * backoff`
		// truncated the float-multiplied factor to an int64 nanosecond
		// count *before* multiplying by backoff, which collapsed to 0
		// for any backoff ≥ 1ms. Multiply float math against the
		// nanosecond count first, then convert back to Duration.
		backoff := base << (attempt - 1)
		jitter := time.Duration(float64(backoff) * (rng.Float64()*0.2 - 0.1))
		next := backoff + jitter
		if next < 0 {
			next = backoff
		}
		if err := sleep(ctx, next); err != nil {
			return err
		}
	}
	return fmt.Errorf("commit failed after %d attempts: %w", max, lastErr)
}

func sleepWithContext(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
