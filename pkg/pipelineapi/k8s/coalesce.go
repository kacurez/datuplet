package k8s

import (
	"bytes"
	"context"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/types"

	"github.com/datuplet/datuplet/pkg/pipelineapi/metrics"
)

// coalesceEntry is the cached record of a successful write.
type coalesceEntry struct {
	status    RunStatus
	writtenAt time.Time
}

// CoalescedUpdater is a decorator that drops same-state writes before
// they hit the DB. Cache is keyed by the PipelineRun's NamespacedName
// so the informer's DELETE event (which carries only ns+name on the
// event.TypedDeleteEvent) can evict without a UUID lookup.
type CoalescedUpdater struct {
	inner RunStatusUpdater
	mu    sync.Mutex
	cache map[types.NamespacedName]coalesceEntry
	now   func() time.Time
}

// NewCoalescedUpdater wraps an inner updater.
func NewCoalescedUpdater(inner RunStatusUpdater) *CoalescedUpdater {
	return NewCoalescedUpdaterWithClock(inner, time.Now)
}

// NewCoalescedUpdaterWithClock is NewCoalescedUpdater with an injectable
// clock, used only by tests.
func NewCoalescedUpdaterWithClock(inner RunStatusUpdater, now func() time.Time) *CoalescedUpdater {
	return &CoalescedUpdater{
		inner: inner,
		cache: map[types.NamespacedName]coalesceEntry{},
		now:   now,
	}
}

// Update returns (applied, err). applied mirrors the inner updater's
// decision. On applied=true we cache; on applied=false (stale rv,
// missing row) we skip the cache insert so a subsequent identical
// event still reaches the inner.
//
// Coalesced (same-state) drops return (false, nil) — that's not an
// "applied" write, but it also isn't a failure.
func (c *CoalescedUpdater) Update(ctx context.Context, s RunStatus) (bool, error) {
	key := types.NamespacedName{Namespace: s.Namespace, Name: s.PipelineRunName}

	c.mu.Lock()
	prev, hit := c.cache[key]
	c.mu.Unlock()
	if hit && prev.status.equalsForDB(s) {
		metrics.DBUpdatesTotal.WithLabelValues(metrics.OutcomeCoalesced).Inc()
		return false, nil // same state; coalesced, no DB write
	}

	applied, err := c.inner.Update(ctx, s)
	if err != nil {
		metrics.DBUpdatesTotal.WithLabelValues(metrics.OutcomeError).Inc()
		return false, err
	}
	if !applied {
		// Stale rv or missing row — don't poison the cache.
		metrics.DBUpdatesTotal.WithLabelValues(metrics.OutcomeStale).Inc()
		return false, nil
	}

	metrics.DBUpdatesTotal.WithLabelValues(metrics.OutcomeApplied).Inc()
	c.mu.Lock()
	c.cache[key] = coalesceEntry{status: s, writtenAt: c.now()}
	c.mu.Unlock()
	return true, nil
}

// Forget removes the key's cache entry. Called from the informer's
// DELETE handler BEFORE the reconcile.Request for the same key would
// otherwise be enqueued, so a delete-then-recreate with the same name
// starts from a clean cache entry.
func (c *CoalescedUpdater) Forget(key types.NamespacedName) {
	c.mu.Lock()
	delete(c.cache, key)
	c.mu.Unlock()
}

// Size returns the current cache size. Used by the metrics gauge and
// by tests.
func (c *CoalescedUpdater) Size() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.cache)
}

// SweepOnce drops entries older than maxAge. Called periodically by
// the janitor goroutine and directly by tests.
func (c *CoalescedUpdater) SweepOnce(maxAge time.Duration) {
	cutoff := c.now().Add(-maxAge)
	c.mu.Lock()
	defer c.mu.Unlock()
	for k, v := range c.cache {
		if v.writtenAt.Before(cutoff) {
			delete(c.cache, k)
		}
	}
}

// RunJanitor sweeps every `every` and returns when ctx is cancelled.
// Run as a goroutine from runServe.
func (c *CoalescedUpdater) RunJanitor(ctx context.Context, every, maxAge time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			c.SweepOnce(maxAge)
		}
	}
}

// equalsForDB compares the fields UpdateRunPhase actually writes.
// ResourceVersion intentionally NOT compared — two identical-content
// events with different rvs should still coalesce: the DB content is
// the same and the SQL guard would otherwise bump observed_rv for
// nothing. StartedAt / CompletedAt are compared by time value (nil vs
// non-nil matters).
func (a RunStatus) equalsForDB(b RunStatus) bool {
	return a.Phase == b.Phase &&
		a.CurrentStage == b.CurrentStage &&
		a.Message == b.Message &&
		timePtrEqual(a.StartedAt, b.StartedAt) &&
		timePtrEqual(a.CompletedAt, b.CompletedAt) &&
		bytes.Equal(a.StageStatuses, b.StageStatuses)
}

func timePtrEqual(a, b *time.Time) bool {
	switch {
	case a == nil && b == nil:
		return true
	case a == nil || b == nil:
		return false
	default:
		return a.Equal(*b)
	}
}
