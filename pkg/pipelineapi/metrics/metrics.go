// Package metrics exposes the Prometheus instruments pipeline-api's
// observer path populates. Served on /metrics by
// pkg/pipelineapi/http/server.go. controller-runtime's built-in metrics
// server is deliberately disabled (see pkg/pipelineapi/k8s/observer.go
// NewObserver) so everything lives on one port.
package metrics

import (
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Outcome label values for DBUpdatesTotal.
const (
	OutcomeApplied   = "applied"
	OutcomeCoalesced = "coalesced"
	OutcomeStale     = "stale"
	OutcomeError     = "error"
)

var (
	// ReconcileEventsTotal counts every reconcile.Request the observer
	// processes, regardless of outcome.
	ReconcileEventsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pipelineapi_reconcile_events_total",
		Help: "Total reconcile events observed by the PipelineRun observer.",
	})

	// DBUpdatesTotal labels each write attempt with its outcome:
	//   applied   — inner updater wrote the row
	//   coalesced — same state as last successful write, DB skipped
	//   stale     — observed_rv guard / missing row reported by inner
	//   error     — inner updater returned an error
	DBUpdatesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "pipelineapi_db_updates_total",
		Help: "Outcomes of observer-driven runs-table updates.",
	}, []string{"outcome"})

	// InformerCacheSize reports the number of PipelineRun objects
	// currently tracked by the informer. Sampled by a background
	// goroutine every 15s from runServe; not event-driven.
	InformerCacheSize = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "pipelineapi_informer_cache_size",
		Help: "Number of PipelineRun objects in the observer's informer cache.",
	})

	// ReconcileDurationSeconds is wall time for one reconcile.Request.
	// Includes the DB write via the coalesce chain.
	ReconcileDurationSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "pipelineapi_reconcile_duration_seconds",
		Help:    "Wall time per reconcile.Request from dequeue to return.",
		Buckets: prometheus.DefBuckets,
	})

	// ReaperRunTuplesOrphanedTotal counts run_tuples rows the reaper
	// removed because they were orphaned: the row had aged past the
	// crash-recovery deadline (>30m) AND no live runs row / PipelineRun
	// referenced it. Non-zero is normal when pipeline-api crashes
	// mid-trigger; spikes deserve investigation.
	//
	ReaperRunTuplesOrphanedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pipelineapi_reaper_run_tuples_orphaned_total",
		Help: "Total run_tuples rows GC'd because no live runs row referenced them.",
	})

	// ReaperRunTuplesTerminalWithTuplesTotal counts terminal-phase runs
	// the reaper found still carrying live FGA tuples — a state that
	// should never persist, since the primary cancel/complete path
	// always DeleteTuples first. Non-zero means "reaper saved us":
	// alert and investigate the cancel/complete path.
	ReaperRunTuplesTerminalWithTuplesTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pipelineapi_reaper_run_tuples_terminal_with_tuples_total",
		Help: "Terminal-phase runs the reaper found still carrying FGA tuples (should always be 0).",
	})

	// ObserverLagSeconds reports the elapsed time since the
	// pipeline-observer last processed a reconcile event OR was marked
	// alive at cache sync — whichever is more recent.
	//
	// In a healthy cluster this gauge stays small. If pipeline-observer
	// is down or its informer stalls, the value climbs unbounded. The
	// recommended alert threshold is 300s (5 min).
	//
	// Sampled by a background goroutine in cmd/pipeline-observer/main.go;
	// not event-driven (the metric must grow during quiet periods, not
	// just when events arrive).
	ObserverLagSeconds = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "pipelineapi_observer_lag_seconds",
		Help: "Seconds since the observer last processed a reconcile event (or marked alive at cache sync).",
	})
)

// lastReconcileUnixNano tracks the most recent reconcile event (or cache-sync
// liveness mark). Updated atomically so the metric sampler can read it
// without a lock.
var lastReconcileUnixNano atomic.Int64

// RecordReconcile stamps the current time as the most recent reconcile
// event. Called by the observer's reconcileOne and once at cache-sync
// completion (to seed liveness for the idle case).
func RecordReconcile() {
	lastReconcileUnixNano.Store(time.Now().UnixNano())
}

// ObserverLag returns the time elapsed since the most recent RecordReconcile
// call. Returns 0 before the first call (observer not started yet — caller
// should hide the metric or treat as healthy).
func ObserverLag() time.Duration {
	last := lastReconcileUnixNano.Load()
	if last == 0 {
		return 0
	}
	return time.Since(time.Unix(0, last))
}
