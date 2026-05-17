package k8s_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"k8s.io/apimachinery/pkg/types"

	pkg8s "github.com/datuplet/datuplet/pkg/pipelineapi/k8s"
)

// countingUpdater is a thread-safe RunStatusUpdater that tracks call
// count and always returns the configured applied bool.
type countingUpdater struct {
	mu      sync.Mutex
	calls   int
	applied bool
}

func (c *countingUpdater) Update(_ context.Context, _ pkg8s.RunStatus) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls++
	return c.applied, nil
}

func (c *countingUpdater) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls
}

func keyFor(runID uuid.UUID) types.NamespacedName {
	return types.NamespacedName{Namespace: "ns", Name: "pr-" + runID.String()[:8]}
}

func statusFor(runID uuid.UUID, phase string, rv int64) pkg8s.RunStatus {
	return pkg8s.RunStatus{
		RunID: runID, Namespace: "ns",
		PipelineRunName: "pr-" + runID.String()[:8],
		Phase:           phase, ResourceVersion: rv,
	}
}

func TestCoalesce_DropsIdenticalSecondWrite(t *testing.T) {
	inner := &countingUpdater{applied: true}
	c := pkg8s.NewCoalescedUpdater(inner)
	runID := uuid.New()
	s := statusFor(runID, "Running", 10)
	_, _ = c.Update(context.Background(), s)
	_, _ = c.Update(context.Background(), s)
	if inner.count() != 1 {
		t.Errorf("inner calls = %d, want 1 (identity write should coalesce)", inner.count())
	}
}

func TestCoalesce_WritesOnPhaseChange(t *testing.T) {
	inner := &countingUpdater{applied: true}
	c := pkg8s.NewCoalescedUpdater(inner)
	runID := uuid.New()
	_, _ = c.Update(context.Background(), statusFor(runID, "Running", 10))
	_, _ = c.Update(context.Background(), statusFor(runID, "Succeeded", 20))
	if inner.count() != 2 {
		t.Errorf("inner calls = %d, want 2 (phase change must pass through)", inner.count())
	}
}

func TestCoalesce_WritesOnStageChange(t *testing.T) {
	// Phase identical, stage differs — coalesce must not drop.
	inner := &countingUpdater{applied: true}
	c := pkg8s.NewCoalescedUpdater(inner)
	runID := uuid.New()
	s1 := statusFor(runID, "Running", 10)
	s1.CurrentStage = "extract"
	s2 := s1
	s2.CurrentStage = "transform"
	s2.ResourceVersion = 11
	_, _ = c.Update(context.Background(), s1)
	_, _ = c.Update(context.Background(), s2)
	if inner.count() != 2 {
		t.Errorf("inner calls = %d, want 2 (stage change must pass through)", inner.count())
	}
}

func TestCoalesce_IgnoresResourceVersionForEquality(t *testing.T) {
	// Same content, different rv — must coalesce. If we cached rv, an
	// informer replay of the same object with a bumped rv would bump
	// observed_rv for no DB-visible reason.
	inner := &countingUpdater{applied: true}
	c := pkg8s.NewCoalescedUpdater(inner)
	runID := uuid.New()
	_, _ = c.Update(context.Background(), statusFor(runID, "Running", 10))
	_, _ = c.Update(context.Background(), statusFor(runID, "Running", 25))
	if inner.count() != 1 {
		t.Errorf("inner calls = %d, want 1 (rv bump alone should coalesce)", inner.count())
	}
}

func TestCoalesce_ForgetEvictsEntry(t *testing.T) {
	inner := &countingUpdater{applied: true}
	c := pkg8s.NewCoalescedUpdater(inner)
	runID := uuid.New()
	s := statusFor(runID, "Running", 10)
	_, _ = c.Update(context.Background(), s)
	c.Forget(keyFor(runID))
	// After Forget, the next identical Update must go through.
	_, _ = c.Update(context.Background(), s)
	if inner.count() != 2 {
		t.Errorf("inner calls = %d, want 2 after Forget", inner.count())
	}
}

func TestCoalesce_DoesNotCacheWhenInnerReportsNotApplied(t *testing.T) {
	// Simulates stale-rv or missing-row: inner returns applied=false.
	// Coalesce must NOT insert, so a retry with the same state still
	// reaches inner.
	inner := &countingUpdater{applied: false}
	c := pkg8s.NewCoalescedUpdater(inner)
	runID := uuid.New()
	_, _ = c.Update(context.Background(), statusFor(runID, "Running", 10))
	_, _ = c.Update(context.Background(), statusFor(runID, "Running", 10))
	if inner.count() != 2 {
		t.Errorf("inner calls = %d, want 2 (no caching on applied=false)", inner.count())
	}
}

func TestCoalesce_JanitorDropsOldEntries(t *testing.T) {
	inner := &countingUpdater{applied: true}
	fake := time.Now()
	c := pkg8s.NewCoalescedUpdaterWithClock(inner, func() time.Time { return fake })

	runID := uuid.New()
	s := statusFor(runID, "Running", 10)
	_, _ = c.Update(context.Background(), s)
	if c.Size() != 1 {
		t.Fatalf("size = %d, want 1 after first Update", c.Size())
	}

	fake = fake.Add(49 * time.Hour)
	c.SweepOnce(48 * time.Hour)
	if c.Size() != 0 {
		t.Fatalf("size = %d, want 0 after sweep past TTL", c.Size())
	}

	// Identical write after sweep must reach inner again.
	_, _ = c.Update(context.Background(), s)
	if inner.count() != 2 {
		t.Errorf("inner calls = %d, want 2 (janitor should have evicted)", inner.count())
	}
}

func TestCoalesce_JanitorKeepsFreshEntries(t *testing.T) {
	inner := &countingUpdater{applied: true}
	fake := time.Now()
	c := pkg8s.NewCoalescedUpdaterWithClock(inner, func() time.Time { return fake })

	runID := uuid.New()
	_, _ = c.Update(context.Background(), statusFor(runID, "Running", 10))

	fake = fake.Add(24 * time.Hour)
	c.SweepOnce(48 * time.Hour)
	if c.Size() != 1 {
		t.Errorf("size = %d, want 1 (entry was within TTL)", c.Size())
	}
}
