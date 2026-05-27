package datagateway

import (
	"context"
	"errors"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/datuplet/datuplet/pkg/catalogwriter"
)

// Metrics are registered ONCE here at package-init via promauto against
// the default registry. Do NOT move registration into NewCommitPool —
// constructing two pools in one process would then panic with
// "duplicate metrics collector registration".
//
// N7 (documented assumption): production constructs exactly one
// CommitPool per process, so package-level registration is safe. The
// pool unit tests inject a fake CommitFn and never touch these metrics,
// so they are unaffected. If a future test needs per-pool metrics,
// switch to an injected *prometheus.Registry.
var (
	commitDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "datuplet_dg_commit_duration_seconds",
		Help:    "Inline iceberg commit duration per table.",
		Buckets: []float64{0.05, 0.1, 0.5, 1, 2, 5, 10, 30, 60, 120, 300},
	}, []string{"mode", "result"})
	commitQueueDepth = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "datuplet_dg_commit_queue_depth",
		Help: "Queued+in-flight inline commits.",
	})
	commitIdempotencySkips = promauto.NewCounter(prometheus.CounterOpts{
		Name: "datuplet_dg_commit_idempotency_skips_total",
		Help: "Inline commits skipped via matching commit-key.",
	})
	commitErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "datuplet_dg_commit_errors_total",
		Help: "Inline commit errors by class.",
	}, []string{"class"})
)

// classifyCommitError maps an error to a stable metric label using TYPED
// checks (no substring scanning of message text). Unknown → "other".
func classifyCommitError(err error) string {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, context.Canceled):
		return "cancelled"
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case catalogwriter.IsCommitConflict(err):
		return "conflict"
	default:
		return "other"
	}
}
