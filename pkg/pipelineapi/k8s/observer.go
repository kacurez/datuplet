// Package k8s — Observer is the informer-backed reconciler that
// mirrors PipelineRun status into the Postgres runs table.
package k8s

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/google/uuid"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	datupletv1 "github.com/datuplet/datuplet/pkg/k8s/api/v1"
	"github.com/datuplet/datuplet/pkg/pipelineapi/metrics"
)

// RunStatus is the subset of a PipelineRun's status the observer
// propagates to the DB. ResourceVersion is the parsed
// metadata.resourceVersion — 0 means "skip the monotonic-rv guard in
// UpdateRunPhase" (used by the reaper's Expired transition and by mock
// updaters in tests).
type RunStatus struct {
	RunID     uuid.UUID
	Namespace string // K8s namespace the PipelineRun lived in; DB updater validates this matches runs.project_id's project namespace before applying
	// PipelineRunName is the source PipelineRun's metadata.name. The DB
	// updater recomputes the expected name from the stored pipeline and
	// rejects updates that don't match — otherwise any actor with write
	// access to the project namespace could create a decoy PipelineRun
	// carrying a victim's run-id label and hijack its mirrored status.
	PipelineRunName string
	Phase           string
	CurrentStage    string
	Message         string
	StartedAt       *time.Time
	CompletedAt     *time.Time
	ResourceVersion int64
	// StageStatuses is the marshalled PipelineRun.Status.StageStatuses, or nil
	// when the CRD has no stage statuses yet. Persisted verbatim as runs.stage_statuses.
	StageStatuses []byte
}

// RunStatusUpdater is the seam between observer and DB. The coalesce
// decorator and the production DBRunUpdater both
// implement it. Update returns (true, nil) when the DB row was
// updated, (false, nil) when the observed_rv guard filtered the write
// or the row no longer exists, and (false, err) on infrastructure
// error.
type RunStatusUpdater interface {
	Update(ctx context.Context, s RunStatus) (applied bool, err error)
}

// Observer is a controller-runtime Manager wrapping a PipelineRun
// informer + reconciler. Single-replica; no leader election.
type Observer struct {
	mgr     manager.Manager
	updater RunStatusUpdater
}

// NewObserver builds a Manager with the PipelineRun controller
// registered. controller-runtime's own metrics server is disabled
// (BindAddress "0") — pipeline-api exposes metrics on its main HTTP
// mux via the metrics package.
//
// onDelete, when non-nil, is invoked synchronously from the informer's
// DELETE handler before the reconcile.Request would otherwise be
// enqueued. Pass CoalescedUpdater.Forget so reaper-driven and
// kubectl-driven deletes immediately evict their cache entry. May be
// nil when coalesce isn't in play (smoke tests, Task 2 transitional
// wiring).
func NewObserver(cfg *rest.Config, u RunStatusUpdater, onDelete func(types.NamespacedName)) (*Observer, error) {
	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme:                 scheme,
		LeaderElection:         false,
		HealthProbeBindAddress: "0",
		Metrics:                metricsserver.Options{BindAddress: "0"},
	})
	if err != nil {
		return nil, fmt.Errorf("new manager: %w", err)
	}
	o := &Observer{mgr: mgr, updater: u}

	hasRunID := predicate.NewPredicateFuncs(func(obj client.Object) bool {
		_, ok := obj.GetLabels()["datuplet.io/run-id"]
		return ok
	})

	// controller-runtime v0.20.4: handler.Funcs is an alias for
	// TypedFuncs[client.Object, reconcile.Request], so the workqueue
	// param is workqueue.TypedRateLimitingInterface[reconcile.Request].
	// TypedCreate/Update/DeleteEvent[client.Object] are the matching
	// generic event types.
	handlerFuncs := handler.Funcs{
		CreateFunc: func(_ context.Context, e event.TypedCreateEvent[client.Object], q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
			q.Add(reconcile.Request{NamespacedName: types.NamespacedName{
				Namespace: e.Object.GetNamespace(),
				Name:      e.Object.GetName(),
			}})
		},
		UpdateFunc: func(_ context.Context, e event.TypedUpdateEvent[client.Object], q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
			q.Add(reconcile.Request{NamespacedName: types.NamespacedName{
				Namespace: e.ObjectNew.GetNamespace(),
				Name:      e.ObjectNew.GetName(),
			}})
		},
		DeleteFunc: func(_ context.Context, e event.TypedDeleteEvent[client.Object], _ workqueue.TypedRateLimitingInterface[reconcile.Request]) {
			key := types.NamespacedName{
				Namespace: e.Object.GetNamespace(),
				Name:      e.Object.GetName(),
			}
			if onDelete != nil {
				onDelete(key) // coalesce.Forget, synchronous
			}
			// No enqueue: reconcile for a deleted object is a no-op.
		},
		// GenericFunc left default (drop).
	}

	if err := ctrl.NewControllerManagedBy(mgr).
		Named("pipelinerun-observer").
		Watches(
			&datupletv1.PipelineRun{},
			handlerFuncs,
			builder.WithPredicates(hasRunID),
		).
		Complete(reconcile.Func(o.reconcile)); err != nil {
		return nil, fmt.Errorf("register controller: %w", err)
	}
	return o, nil
}

// Start blocks until ctx is cancelled or the manager exits. Run it as a
// goroutine from runServe.
func (o *Observer) Start(ctx context.Context) error {
	return o.mgr.Start(ctx)
}

// WaitForCacheSync blocks until the informer has completed its initial
// list. Callers MUST NOT accept HTTP traffic until this returns true —
// the cancel path reads the same cache and a half-populated cache
// would produce spurious 404s.
func (o *Observer) WaitForCacheSync(ctx context.Context) bool {
	return o.mgr.GetCache().WaitForCacheSync(ctx)
}

// CacheSize returns the number of PipelineRuns the informer currently
// tracks. Used by the metrics gauge (Task 6) and by tests.
func (o *Observer) CacheSize(ctx context.Context) (int, error) {
	list := &datupletv1.PipelineRunList{}
	sel, err := labels.Parse("datuplet.io/run-id")
	if err != nil {
		return 0, err
	}
	if err := o.mgr.GetCache().List(ctx, list, &client.ListOptions{LabelSelector: sel}); err != nil {
		return 0, err
	}
	return len(list.Items), nil
}

func (o *Observer) reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	return reconcileOne(ctx, o.mgr.GetClient(), req, o.updater)
}

// reconcileOne is exposed for tests that want to drive the logic with a
// fake client.Client without standing up a full manager.
func reconcileOne(ctx context.Context, c client.Client, req reconcile.Request, u RunStatusUpdater) (reconcile.Result, error) {
	start := time.Now()
	defer func() {
		metrics.ReconcileDurationSeconds.Observe(time.Since(start).Seconds())
	}()
	metrics.ReconcileEventsTotal.Inc()
	// Stamp the observer-lag metric so it stays small during normal
	// activity. A background sampler in cmd/pipeline-observer/main.go
	// converts the timestamp to a pipelineapi_observer_lag_seconds
	// gauge value.
	metrics.RecordReconcile()

	pr := &datupletv1.PipelineRun{}
	if err := c.Get(ctx, req.NamespacedName, pr); err != nil {
		if apierrors.IsNotFound(err) {
			// Object is gone. DELETE-event handling for coalesce
			// eviction is done separately in Task 3.
			return reconcile.Result{}, nil
		}
		return reconcile.Result{}, err
	}
	if pr.DeletionTimestamp != nil {
		// Mid-delete. A cancel handler may have just written phase
		// Cancelled; we must not overwrite it with the pre-delete
		// phase still visible on the object.
		return reconcile.Result{}, nil
	}
	rid, ok := pr.Labels["datuplet.io/run-id"]
	if !ok {
		return reconcile.Result{}, nil
	}
	parsed, err := uuid.Parse(rid)
	if err != nil {
		return reconcile.Result{}, nil
	}
	phase := string(pr.Status.Phase)
	if phase == "" {
		// Operator hasn't reconciled yet — preserve the Pending the
		// API wrote at trigger time.
		return reconcile.Result{}, nil
	}
	// Fail closed on non-numeric resourceVersion. metadata.resourceVersion
	// is formally opaque, but on etcd it's a base-10 int64. If parsing
	// fails, a rv=0 fallback would disable the
	// SQL guard and let a stale event overwrite newer state.
	rv, parseErr := strconv.ParseInt(pr.ResourceVersion, 10, 64)
	if parseErr != nil || rv <= 0 {
		log.Printf("pipeline-api observer: skip run=%s ns=%s: non-numeric resourceVersion %q",
			parsed, pr.Namespace, pr.ResourceVersion)
		return reconcile.Result{}, nil
	}
	status := RunStatus{
		RunID:           parsed,
		Namespace:       pr.Namespace,
		PipelineRunName: pr.Name,
		Phase:           phase,
		CurrentStage:    pr.Status.CurrentStage,
		Message:         pr.Status.Message,
		ResourceVersion: rv,
	}
	if pr.Status.StartTime != nil {
		t := pr.Status.StartTime.Time
		status.StartedAt = &t
	}
	if pr.Status.CompletionTime != nil {
		t := pr.Status.CompletionTime.Time
		status.CompletedAt = &t
	}
	if len(pr.Status.StageStatuses) > 0 {
		if b, err := json.Marshal(pr.Status.StageStatuses); err == nil {
			status.StageStatuses = b
		}
	}
	if _, err := u.Update(ctx, status); err != nil {
		// Return the error so controller-runtime's workqueue requeues
		// with exponential backoff. Unlike the old polling design (where
		// a failed sweep was followed by another sweep 2s later), the
		// informer path has no guaranteed next event — for a run that
		// already reached its terminal phase, this reconcile is the
		// last chance to mirror. Swallowing the error here leaves
		// runs.phase stuck on the prior value indefinitely.
		log.Printf("pipeline-api observer: update run=%s ns=%s: %v", status.RunID, status.Namespace, err)
		return reconcile.Result{}, err
	}
	return reconcile.Result{}, nil
}

// ReconcileOneForTest is an exported shim for observer_test.go. Keep
// it at the bottom of the file so it's easy to spot in code review; no
// production code should call it.
func ReconcileOneForTest(ctx context.Context, c client.Client, req reconcile.Request, u RunStatusUpdater) (reconcile.Result, error) {
	return reconcileOne(ctx, c, req, u)
}
