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

// TestCoalesce_DropsIdenticalWrites: an identical second write coalesces, and
// so does the same content with a bumped rv. If we cached rv, an informer
// replay of the same object with a bumped rv would bump observed_rv for no
// DB-visible reason.
func TestCoalesce_DropsIdenticalWrites(t *testing.T) {
	inner := &countingUpdater{applied: true}
	c := pkg8s.NewCoalescedUpdater(inner)
	runID := uuid.New()
	s := statusFor(runID, "Running", 10)
	_, _ = c.Update(context.Background(), s)
	_, _ = c.Update(context.Background(), s)
	if inner.count() != 1 {
		t.Errorf("inner calls = %d, want 1 (identity write should coalesce)", inner.count())
	}
	_, _ = c.Update(context.Background(), statusFor(runID, "Running", 25))
	if inner.count() != 1 {
		t.Errorf("inner calls = %d, want 1 (rv bump alone should coalesce)", inner.count())
	}
}

// TestCoalesce_WritesOnChange: any content change passes through.
func TestCoalesce_WritesOnChange(t *testing.T) {
	runID := uuid.New()
	cases := []struct {
		name   string
		first  pkg8s.RunStatus
		second func(base pkg8s.RunStatus) pkg8s.RunStatus
	}{
		{
			"phase_change",
			statusFor(runID, "Running", 10),
			func(pkg8s.RunStatus) pkg8s.RunStatus { return statusFor(runID, "Succeeded", 20) },
		},
		{
			// Phase identical, stage differs — coalesce must not drop.
			"stage_change",
			func() pkg8s.RunStatus {
				s := statusFor(runID, "Running", 10)
				s.CurrentStage = "extract"
				return s
			}(),
			func(base pkg8s.RunStatus) pkg8s.RunStatus {
				s := base
				s.CurrentStage = "transform"
				s.ResourceVersion = 11
				return s
			},
		},
		{
			// Same scalars, DIFFERENT snapshot (a component finished) +
			// higher rv.
			"snapshot_only_change",
			func() pkg8s.RunStatus {
				s := statusFor(runID, "Running", 1)
				s.CurrentStage = "extract"
				s.StageStatuses = []byte(`[{"name":"extract","phase":"Running"}]`)
				return s
			}(),
			func(base pkg8s.RunStatus) pkg8s.RunStatus {
				s := base
				s.ResourceVersion = 2
				s.StageStatuses = []byte(`[{"name":"extract","phase":"Succeeded"}]`)
				return s
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			inner := &countingUpdater{applied: true}
			c := pkg8s.NewCoalescedUpdater(inner)
			if _, err := c.Update(context.Background(), tc.first); err != nil {
				t.Fatalf("first: %v", err)
			}
			if _, err := c.Update(context.Background(), tc.second(tc.first)); err != nil {
				t.Fatalf("second: %v", err)
			}
			if inner.count() != 2 {
				t.Errorf("inner calls = %d, want 2 (change must pass through)", inner.count())
			}
		})
	}
}

func TestCoalesce_Janitor(t *testing.T) {
	cases := []struct {
		name      string
		advance   time.Duration
		wantSize  int
		wantCalls int // after an identical write following the sweep
	}{
		{"drops_old_entries", 49 * time.Hour, 0, 2},
		{"keeps_fresh_entries", 24 * time.Hour, 1, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			inner := &countingUpdater{applied: true}
			fake := time.Now()
			c := pkg8s.NewCoalescedUpdaterWithClock(inner, func() time.Time { return fake })

			s := statusFor(uuid.New(), "Running", 10)
			_, _ = c.Update(context.Background(), s)
			if c.Size() != 1 {
				t.Fatalf("size = %d, want 1 after first Update", c.Size())
			}

			fake = fake.Add(tc.advance)
			c.SweepOnce(48 * time.Hour)
			if c.Size() != tc.wantSize {
				t.Fatalf("size = %d, want %d after sweep", c.Size(), tc.wantSize)
			}

			// An identical write after the sweep reaches inner only if the
			// janitor evicted the entry.
			_, _ = c.Update(context.Background(), s)
			if inner.count() != tc.wantCalls {
				t.Errorf("inner calls = %d, want %d", inner.count(), tc.wantCalls)
			}
		})
	}
}
