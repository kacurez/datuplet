package preview

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	datupletv1 "github.com/datuplet/datuplet/pkg/k8s/api/v1"
	"github.com/datuplet/datuplet/pkg/pipelineapi/authz"
	pkg8s "github.com/datuplet/datuplet/pkg/pipelineapi/k8s"
	"github.com/datuplet/datuplet/pkg/pipelineapi/store"
	"github.com/datuplet/datuplet/pkg/pipelineapi/tokens"
)

// sweepLockKey is the pg advisory lock ("prev") that elects one sweeping replica.
const sweepLockKey int64 = 0x70726576

const sweepBatch = 100

var sweepFailures = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "pipelineapi_preview_sweep_failures_total",
	Help: "Preview sweep steps that failed and were left for the next tick.",
}, []string{"step"})

// stepError tags a Purge failure with its sweep_failures step label.
type stepError struct {
	step string
	err  error
}

func (e *stepError) Error() string { return e.err.Error() }
func (e *stepError) Unwrap() error { return e.err }

func failedStep(err error) string {
	var se *stepError
	if errors.As(err, &se) {
		return se.step
	}
	return "purge"
}

// RunSweeper sweeps previews every interval until ctx is done. Only the
// replica holding the advisory lock sweeps a given tick.
func (s *Service) RunSweeper(ctx context.Context, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if _, err := s.sweepIfLeader(ctx); err != nil {
				log.Printf("pipeline-api: preview sweep: %v", err)
			}
		}
	}
}

// sweepIfLeader runs SweepOnce under the sweep advisory lock and reports
// whether it got the lock.
func (s *Service) sweepIfLeader(ctx context.Context) (bool, error) {
	conn, err := s.d.DB.Acquire(ctx)
	if err != nil {
		return false, fmt.Errorf("acquire conn: %w", err)
	}
	defer conn.Release()
	var got bool
	if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, sweepLockKey).Scan(&got); err != nil {
		return false, fmt.Errorf("try advisory lock: %w", err)
	}
	if !got {
		return false, nil
	}
	defer func() {
		if _, err := conn.Exec(context.WithoutCancel(ctx), `SELECT pg_advisory_unlock($1)`, sweepLockKey); err != nil {
			log.Printf("pipeline-api: preview sweep: advisory unlock: %v", err)
		}
	}()
	return true, s.SweepOnce(ctx)
}

// SweepOnce deletes the run-identity tuples of terminal previews and purges
// expired ones. A failing row is logged and counted, never stopping the others.
func (s *Service) SweepOnce(ctx context.Context) error {
	failures := 0
	fail := func(step string, id fmt.Stringer, err error) {
		failures++
		sweepFailures.WithLabelValues(step).Inc()
		log.Printf("pipeline-api: preview sweep %s (preview=%s): %v", step, id, err)
	}

	terminal, err := store.ListPreviewsNeedingTupleDelete(ctx, s.d.DB, sweepBatch)
	if err != nil {
		return err
	}
	for i := range terminal {
		if err := s.deleteOwnTuple(ctx, &terminal[i]); err != nil {
			fail("tuple", terminal[i].ID, err)
		}
	}

	expired, err := store.ListExpiredPreviews(ctx, s.d.DB, s.d.Now(), sweepBatch)
	if err != nil {
		return err
	}
	for i := range expired {
		if err := s.Purge(ctx, &expired[i]); err != nil {
			fail(failedStep(err), expired[i].ID, err)
		}
	}
	if failures > 0 {
		return fmt.Errorf("%d preview sweep step(s) failed", failures)
	}
	return nil
}

// Purge stops the preview, waits for its pods to go, then purges and drops
// every scratch namespace under a transient sweeper identity, and marks the
// preview cleaned (Expired when it never finished).
func (s *Service) Purge(ctx context.Context, p *store.Preview) error {
	expired := !isTerminal(p.Phase)
	if expired {
		if err := s.stop(ctx, p); err != nil {
			return &stepError{"cancel", err}
		}
	}
	if err := s.deleteOwnTuple(ctx, p); err != nil {
		return &stepError{"tuple", err}
	}
	if err := s.deletePipelineRun(ctx, p); err != nil {
		return err
	}
	if err := s.deleteTokenSecret(ctx, p); err != nil {
		return err
	}
	if err := s.waitGone(ctx, p); err != nil {
		return err
	}
	if err := s.purgeNamespaces(ctx, p); err != nil {
		return err
	}
	return store.MarkPreviewCleaned(ctx, s.d.DB, p.ID, s.d.Now().UTC(), expired)
}

func (s *Service) deleteTokenSecret(ctx context.Context, p *store.Preview) error {
	sec := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{
		Name: pkg8s.CreateRunOpts{RunID: p.ID}.SecretName(), Namespace: p.K8sNamespace,
	}}
	if err := s.d.K8s.Delete(ctx, sec); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete run-token Secret: %w", err)
	}
	return nil
}

// waitGone polls until the PipelineRun and every pod of the preview are gone:
// purging under a live writer does not stop the writer.
func (s *Service) waitGone(ctx context.Context, p *store.Preview) error {
	deadline := time.Now().Add(s.d.PurgeWait)
	for {
		gone, err := s.gone(ctx, p)
		if err != nil {
			return err
		}
		if gone {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("preview %s still has a PipelineRun or pods after %s", p.ID, s.d.PurgeWait)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(s.d.PollInterval):
		}
	}
}

func (s *Service) gone(ctx context.Context, p *store.Preview) (bool, error) {
	err := s.d.K8s.Get(ctx, types.NamespacedName{
		Name: pkg8s.PreviewPipelineRunName(p.ID), Namespace: p.K8sNamespace,
	}, &datupletv1.PipelineRun{})
	if err == nil {
		return false, nil
	}
	if !apierrors.IsNotFound(err) {
		return false, fmt.Errorf("get PipelineRun: %w", err)
	}
	pods := &corev1.PodList{}
	if err := s.d.K8s.List(ctx, pods, client.InNamespace(p.K8sNamespace),
		client.MatchingLabels{"datuplet.io/run-id": p.ID.String()}); err != nil {
		return false, fmt.Errorf("list pods: %w", err)
	}
	return len(pods.Items) == 0, nil
}

// purgeNamespaces grants the per-preview sweeper identity editor for the
// duration of the purge only, then verifies every namespace is empty.
func (s *Service) purgeNamespaces(ctx context.Context, p *store.Preview) error {
	sw := sweeperSubjectPrefix + p.ID.String()
	lk := p.LakekeeperProjectID
	if s.tuplesEnabled(lk) {
		if err := authz.WriteProjectTuple(ctx, s.d.Authz, sw, authz.RelationEditor, lk); err != nil {
			return &stepError{"tuple", err}
		}
		defer func() {
			bg, cancel := context.WithTimeout(context.WithoutCancel(ctx), detachedTimeout)
			defer cancel()
			if err := authz.DeleteProjectTuple(bg, s.d.Authz, sw, authz.RelationEditor, lk); err != nil {
				sweepFailures.WithLabelValues("tuple").Inc()
				log.Printf("pipeline-api: preview %s: delete sweeper tuple: %v", p.ID, err)
			}
		}()
	}
	tp := func(context.Context) (string, error) {
		return tokens.MintServiceToken(s.d.Signer, tokens.ServiceTokenSpec{
			Subject: sw, Audience: s.d.Audience, Lifetime: sweeperTokenLifetime,
		})
	}
	cat, err := s.d.Catalogs(ctx, tp, lk, p.Warehouse)
	if err != nil {
		return fmt.Errorf("open catalog: %w", err)
	}
	for _, ns := range p.ScratchNamespaces {
		tables, err := cat.ListTables(ctx, ns)
		if err != nil {
			return err
		}
		for _, t := range tables {
			if err := cat.PurgeTable(ctx, ns, t); err != nil {
				return err
			}
		}
		if err := cat.DropNamespace(ctx, ns); err != nil {
			return err
		}
	}
	for _, ns := range p.ScratchNamespaces {
		tables, err := cat.ListTables(ctx, ns)
		if err != nil {
			return err
		}
		if len(tables) > 0 {
			return fmt.Errorf("namespace %s still lists %d table(s) after purge", ns, len(tables))
		}
	}
	return nil
}
