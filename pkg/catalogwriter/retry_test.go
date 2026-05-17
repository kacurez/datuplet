package catalogwriter

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/iceberg-go/catalog/rest"
)

func nopSleep(ctx context.Context, d time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}

// TestRetryOnConflict_SucceedsOnFirstTry exercises the trivial happy
// path so we know the retry wrapper isn't accidentally adding a delay
// or extra calls.
func TestRetryOnConflict_SucceedsOnFirstTry(t *testing.T) {
	t.Parallel()
	var calls atomic.Int64
	err := RetryOnConflict(context.Background(), RetryOpts{Sleep: nopSleep}, func(ctx context.Context) error {
		calls.Add(1)
		return nil
	})
	if err != nil {
		t.Fatalf("expected nil err, got %v", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("calls=%d want 1", got)
	}
}

// TestRetryOnConflict_RetriesUpToBudget verifies that 409 conflicts
// retry until success when N < MaxAttempts.
func TestRetryOnConflict_RetriesUpToBudget(t *testing.T) {
	t.Parallel()
	var calls atomic.Int64
	err := RetryOnConflict(context.Background(), RetryOpts{
		MaxAttempts: 5,
		BaseBackoff: time.Microsecond,
		Sleep:       nopSleep,
	}, func(ctx context.Context) error {
		calls.Add(1)
		if calls.Load() < 4 {
			// Wrap to ensure errors.Is matches through wrapping —
			// matches iceberg-go's actual error shape.
			return fmt.Errorf("fake: %w", rest.ErrCommitFailed)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("expected success after retries, got %v", err)
	}
	if got := calls.Load(); got != 4 {
		t.Fatalf("calls=%d want 4 (3 conflicts + 1 success)", got)
	}
}

// TestRetryOnConflict_ExhaustsAtMaxAttempts is the inverse: when
// 409s persist beyond MaxAttempts, the last error is surfaced.
func TestRetryOnConflict_ExhaustsAtMaxAttempts(t *testing.T) {
	t.Parallel()
	var calls atomic.Int64
	err := RetryOnConflict(context.Background(), RetryOpts{
		MaxAttempts: 3,
		BaseBackoff: time.Microsecond,
		Sleep:       nopSleep,
	}, func(ctx context.Context) error {
		calls.Add(1)
		return rest.ErrCommitFailed
	})
	if err == nil {
		t.Fatal("expected exhaustion error")
	}
	if !errors.Is(err, rest.ErrCommitFailed) {
		t.Fatalf("err should wrap ErrCommitFailed, got %v", err)
	}
	if got := calls.Load(); got != 3 {
		t.Fatalf("calls=%d want 3", got)
	}
}

// TestRetryOnConflict_NonConflictShortCircuits verifies that a non-409
// error breaks out of the retry loop on the first attempt — there's
// no point retrying e.g. a 401 or a malformed-request 400.
func TestRetryOnConflict_NonConflictShortCircuits(t *testing.T) {
	t.Parallel()
	var calls atomic.Int64
	want := errors.New("auth failed")
	err := RetryOnConflict(context.Background(), RetryOpts{
		MaxAttempts: 5,
		BaseBackoff: time.Microsecond,
		Sleep:       nopSleep,
	}, func(ctx context.Context) error {
		calls.Add(1)
		return want
	})
	if !errors.Is(err, want) {
		t.Fatalf("err=%v want wrapping %v", err, want)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("calls=%d want 1 (non-conflict must short-circuit)", got)
	}
}

// TestRetryOnConflict_HonoursContextCancel ensures a cancelled context
// propagates rather than burning the full retry budget.
func TestRetryOnConflict_HonoursContextCancel(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	var calls atomic.Int64
	err := RetryOnConflict(ctx, RetryOpts{Sleep: nopSleep}, func(ctx context.Context) error {
		calls.Add(1)
		return rest.ErrCommitFailed
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v want context.Canceled", err)
	}
	if got := calls.Load(); got > 0 {
		t.Fatalf("calls=%d want 0 (cancelled before first attempt)", got)
	}
}

// TestRetryOnConflict_JitterCovers80PercentOfWindow verifies the
// fix to the jitter-truncation bug: previously `time.Duration(rng.Float64()*0.2-0.1) * backoff`
// would truncate the 0..0.2 float to 0 ns whenever backoff >= 1ms,
// producing zero jitter in production. We now multiply float math
// against the nanosecond count first. Run many iterations and assert
// the observed jitter spread covers ≥ 80% of the expected ±10%
// window.
func TestRetryOnConflict_JitterCovers80PercentOfWindow(t *testing.T) {
	t.Parallel()

	const (
		iterations = 1000
		baseSleep  = 100 * time.Millisecond // base << 0 = first attempt
	)

	sleeps := make([]time.Duration, 0, iterations)
	captureSleep := func(_ context.Context, d time.Duration) error {
		sleeps = append(sleeps, d)
		return nil
	}

	// Drive iterations independent retry runs that each fail twice and
	// succeed on the third call. That gives us one captured sleep per
	// iteration — between attempt 1 and attempt 2, with backoff =
	// baseSleep<<0 = baseSleep, which is the formula we care about.
	for i := 0; i < iterations; i++ {
		var calls atomic.Int64
		err := RetryOnConflict(context.Background(), RetryOpts{
			MaxAttempts: 2,
			BaseBackoff: baseSleep,
			Sleep:       captureSleep,
		}, func(ctx context.Context) error {
			calls.Add(1)
			if calls.Load() < 2 {
				return rest.ErrCommitFailed
			}
			return nil
		})
		if err != nil {
			t.Fatalf("iter %d: %v", i, err)
		}
	}
	if got := len(sleeps); got != iterations {
		t.Fatalf("expected %d captured sleeps, got %d", iterations, got)
	}

	// Expected window: backoff ± 10% = [90ms, 110ms].
	baseFloat := float64(baseSleep)
	expectedMin := time.Duration(baseFloat * 0.90)
	expectedMax := time.Duration(baseFloat * 1.10)

	var observedMin, observedMax time.Duration
	observedMin = sleeps[0]
	observedMax = sleeps[0]
	for _, s := range sleeps {
		if s < observedMin {
			observedMin = s
		}
		if s > observedMax {
			observedMax = s
		}
	}

	// All sleeps must land inside the ±10% window.
	if observedMin < expectedMin {
		t.Errorf("observed min %v < expected %v (jitter underflowed window)", observedMin, expectedMin)
	}
	if observedMax > expectedMax {
		t.Errorf("observed max %v > expected %v (jitter overflowed window)", observedMax, expectedMax)
	}

	// Spread across observed values must cover ≥ 80% of the expected
	// 20% window. Catches the truncation regression — under the bug
	// every sleep was exactly baseSleep so spread was 0.
	expectedSpread := expectedMax - expectedMin
	observedSpread := observedMax - observedMin
	threshold := time.Duration(float64(expectedSpread) * 0.80)
	if observedSpread < threshold {
		t.Errorf("jitter spread %v covers <80%% of expected window %v (regressed to truncation bug?)", observedSpread, expectedSpread)
	}
}

// TestIsCommitConflict_DetectsWrapping verifies the helper recognises
// wrapped 409 errors, which is how iceberg-go and our own wrappers
// surface them.
func TestIsCommitConflict_DetectsWrapping(t *testing.T) {
	t.Parallel()
	wrapped := fmt.Errorf("commit table foo.bar: %w", rest.ErrCommitFailed)
	if !IsCommitConflict(wrapped) {
		t.Fatalf("IsCommitConflict should detect wrapped ErrCommitFailed")
	}
	if IsCommitConflict(errors.New("plain")) {
		t.Fatalf("IsCommitConflict should reject plain errors")
	}
}
