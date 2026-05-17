package metrics_test

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/datuplet/datuplet/pkg/pipelineapi/metrics"
)

func TestDBUpdatesTotal_IncrementsByOutcome(t *testing.T) {
	// Snapshot + delta — other tests in the same binary could have
	// incremented this counter, so we compare deltas rather than
	// absolute values.
	before := testutil.ToFloat64(metrics.DBUpdatesTotal.WithLabelValues(metrics.OutcomeApplied))
	metrics.DBUpdatesTotal.WithLabelValues(metrics.OutcomeApplied).Inc()
	after := testutil.ToFloat64(metrics.DBUpdatesTotal.WithLabelValues(metrics.OutcomeApplied))
	if after-before != 1 {
		t.Errorf("applied delta = %v, want 1", after-before)
	}
}

func TestReconcileEventsTotal_Increments(t *testing.T) {
	before := testutil.ToFloat64(metrics.ReconcileEventsTotal)
	metrics.ReconcileEventsTotal.Inc()
	after := testutil.ToFloat64(metrics.ReconcileEventsTotal)
	if after-before != 1 {
		t.Errorf("delta = %v, want 1", after-before)
	}
}

func TestInformerCacheSize_Set(t *testing.T) {
	metrics.InformerCacheSize.Set(42)
	if got := testutil.ToFloat64(metrics.InformerCacheSize); got != 42 {
		t.Errorf("got %v, want 42", got)
	}
}

func TestObserverLag_RecordReconcileResetsClock(t *testing.T) {
	// Before any RecordReconcile call, lag may be from a previous test —
	// just confirm RecordReconcile resets it to near-zero.
	metrics.RecordReconcile()
	lag := metrics.ObserverLag()
	if lag > 100*time.Millisecond {
		t.Errorf("lag after RecordReconcile = %v, want < 100ms", lag)
	}
}

func TestObserverLag_GrowsOverTime(t *testing.T) {
	metrics.RecordReconcile()
	time.Sleep(50 * time.Millisecond)
	lag := metrics.ObserverLag()
	if lag < 40*time.Millisecond {
		t.Errorf("lag after 50ms sleep = %v, want >= 40ms", lag)
	}
}
