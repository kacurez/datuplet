package preview

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"regexp"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/sync/singleflight"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/datuplet/datuplet/pkg/catalogwriter"
	datupletv1 "github.com/datuplet/datuplet/pkg/k8s/api/v1"
	"github.com/datuplet/datuplet/pkg/pipeline/config"
	"github.com/datuplet/datuplet/pkg/pipeline/validate"
	"github.com/datuplet/datuplet/pkg/pipelineapi/authz"
	pkg8s "github.com/datuplet/datuplet/pkg/pipelineapi/k8s"
	"github.com/datuplet/datuplet/pkg/pipelineapi/runbackend"
	"github.com/datuplet/datuplet/pkg/pipelineapi/store"
	"github.com/datuplet/datuplet/pkg/pipelineapi/tokens"
)

// DefaultWorkspace is the workspace of a preview created without one.
const DefaultWorkspace = "default"

const (
	defaultRowLimit, maxRowLimit           = 1000, 100000
	defaultTimeout, minTimeout, maxTimeout = 300, 30, 3600
	defaultTTL, minTTL, maxTTL             = 3600, 60, 86400
	tokenSlack                             = 10 * time.Minute
	defaultPurgeWait, defaultPollInterval  = 90 * time.Second, 2 * time.Second
	detachedTimeout                        = 10 * time.Second
	tupleDeleteTimeout                     = 5 * time.Second
	sweeperSubjectPrefix                   = "preview-sweeper-"
	sweeperTokenLifetime                   = 5 * time.Minute
	cancelledMessage                       = "cancelled by user"
	phaseCancelled, phaseFailedApplication = "Cancelled", "FailedApplication"
	phasePending, phaseRunning             = "Pending", "Running"
)

var workspaceRe = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,62}$`)

// ErrBadRequest marks a preview request the caller must fix.
var ErrBadRequest = errors.New("bad preview request")

// Deps are the Service's collaborators.
type Deps struct {
	DB                *pgxpool.Pool
	K8s               client.Client
	Authz             authz.Authorizer
	Signer            *tokens.Signer
	ProjectNS         runbackend.ProjectNS
	WarehouseResolver func(ctx context.Context, lkProjectID string) (string, error)
	Catalogs          CatalogFactory
	UserCatalogToken  catalogwriter.TokenProvider // impersonation for the ctx user
	Sampler           Sampler
	Logs              LogTailer
	Caps              store.PreviewCaps
	Now               func() time.Time
	Audience          string
	// PurgeWait bounds the wait for a preview's PipelineRun and pods to
	// disappear before its tables are purged.
	PurgeWait    time.Duration
	PollInterval time.Duration
}

// Service creates, cancels and cleans up previews.
type Service struct {
	d Deps
	// flight serializes result assembly per preview id.
	flight singleflight.Group
}

func New(d Deps) *Service {
	if d.Now == nil {
		d.Now = time.Now
	}
	if d.Audience == "" {
		d.Audience = tokens.TableTokenAudience
	}
	if d.PurgeWait <= 0 {
		d.PurgeWait = defaultPurgeWait
	}
	if d.PollInterval <= 0 {
		d.PollInterval = defaultPollInterval
	}
	return &Service{d: d}
}

// CreateRequest is a preview to start. Zero numeric fields and an empty
// Workspace take their defaults.
type CreateRequest struct {
	ProjectID, UserID                    uuid.UUID
	Doc                                  *config.Pipeline
	Workspace                            string
	RowLimit, TimeoutSeconds, TTLSeconds int
	UntilStage                           string
}

type CreateResponse struct {
	ID                uuid.UUID
	Workspace, Phase  string
	ScratchNamespaces []string
	ExpiresAt         time.Time
}

func withDefaults(r CreateRequest) (CreateRequest, error) {
	if r.Workspace == "" {
		r.Workspace = DefaultWorkspace
	}
	if !workspaceRe.MatchString(r.Workspace) {
		return r, fmt.Errorf("%w: workspace %q must match %s", ErrBadRequest, r.Workspace, workspaceRe)
	}
	bound := func(name string, v *int, def, lo, hi int) error {
		if *v == 0 {
			*v = def
		}
		if *v < lo || *v > hi {
			return fmt.Errorf("%w: %s %d out of range %d..%d", ErrBadRequest, name, *v, lo, hi)
		}
		return nil
	}
	if err := bound("row_limit", &r.RowLimit, defaultRowLimit, 1, maxRowLimit); err != nil {
		return r, err
	}
	if err := bound("timeout_seconds", &r.TimeoutSeconds, defaultTimeout, minTimeout, maxTimeout); err != nil {
		return r, err
	}
	if err := bound("ttl_seconds", &r.TTLSeconds, defaultTTL, minTTL, maxTTL); err != nil {
		return r, err
	}
	return r, nil
}

// Create rewrites the doc onto scratch namespaces, records the preview,
// grants its run identity editor on the lakekeeper project and starts the
// PipelineRun. ctx must carry the authenticated user (the run token's actor).
func (s *Service) Create(ctx context.Context, r CreateRequest) (*CreateResponse, error) {
	if r.Doc == nil {
		return nil, fmt.Errorf("%w: doc is required", ErrBadRequest)
	}
	r, err := withDefaults(r)
	if err != nil {
		return nil, err
	}
	id := uuid.New()
	rw, err := Rewrite(r.Doc, id, r.UntilStage)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrBadRequest, err)
	}
	rewritten, err := json.Marshal(rw.Doc)
	if err != nil {
		return nil, fmt.Errorf("encode rewritten doc: %w", err)
	}
	if _, findings := validate.ValidatePipelineDoc(rewritten, rw.Doc.Name, nil, nil); hasError(findings) {
		return nil, fmt.Errorf("rewritten preview doc is invalid: %s", firstError(findings))
	}
	original, err := json.Marshal(r.Doc)
	if err != nil {
		return nil, fmt.Errorf("encode doc: %w", err)
	}

	ns, err := s.d.ProjectNS.Ensure(ctx, r.ProjectID)
	if err != nil {
		return nil, err
	}
	proj, err := store.GetProjectByID(ctx, s.d.DB, r.ProjectID)
	if err != nil {
		return nil, fmt.Errorf("get project: %w", err)
	}
	lk := proj.LakekeeperProjectID
	var warehouse string
	if s.d.WarehouseResolver != nil {
		if warehouse, err = s.d.WarehouseResolver(ctx, lk); err != nil {
			return nil, fmt.Errorf("resolve warehouse: %w", err)
		}
	}

	p := &store.Preview{
		ID: id, ProjectID: r.ProjectID, TriggeredBy: r.UserID, Workspace: r.Workspace,
		Doc: original, RowLimit: r.RowLimit, TimeoutSeconds: r.TimeoutSeconds, TTLSeconds: r.TTLSeconds,
		UntilStage: r.UntilStage, ScratchNamespaces: rw.Namespaces,
		LakekeeperProjectID: lk, Warehouse: warehouse, K8sNamespace: ns,
	}
	if err := store.InsertPreview(ctx, s.d.DB, p, s.d.Caps); err != nil {
		return nil, err
	}

	if s.tuplesEnabled(lk) {
		if err := authz.WriteProjectTuple(ctx, s.d.Authz, id.String(), authz.RelationEditor, lk); err != nil {
			return nil, s.failCreate(ctx, p, fmt.Errorf("write FGA tuple: %w", err))
		}
	}
	tok, err := tokens.MintRunToken(ctx, s.d.Signer, tokens.RunSpec{
		RunID:        id.String(),
		ProjectID:    lk,
		PipelineName: rw.Doc.Name,
		Warehouse:    warehouse,
		Audience:     s.d.Audience,
		Lifetime:     time.Duration(r.TimeoutSeconds)*time.Second + tokenSlack,
	})
	if err != nil {
		return nil, s.failCreate(ctx, p, fmt.Errorf("mint run token: %w", err))
	}
	if err := pkg8s.CreatePreviewResources(ctx, s.d.K8s, pkg8s.CreatePreviewOpts{
		Namespace: ns,
		PreviewID: id,
		Token:     tok,
		Spec:      config.DocToCR(rw.Doc).Spec,
		Preview: datupletv1.PreviewSpec{
			ID:             id.String(),
			RowLimit:       int64(r.RowLimit),
			TimeoutSeconds: int64(r.TimeoutSeconds),
		},
	}); err != nil {
		return nil, s.failCreate(ctx, p, fmt.Errorf("create K8s resources: %w", err))
	}

	return &CreateResponse{
		ID: id, Workspace: p.Workspace, Phase: p.Phase,
		ScratchNamespaces: rw.Namespaces, ExpiresAt: p.ExpiresAt,
	}, nil
}

// failCreate removes whatever K8s objects a failed create left, marks the
// preview FailedApplication and drops its tuple, on a context detached from
// the request. It returns cause.
func (s *Service) failCreate(ctx context.Context, p *store.Preview, cause error) error {
	bg, cancel := context.WithTimeout(context.WithoutCancel(ctx), detachedTimeout)
	defer cancel()
	if err := s.deletePipelineRun(bg, p); err != nil {
		log.Printf("pipeline-api: preview %s: %v (sweeper retries)", p.ID, err)
	}
	if err := s.deleteTokenSecret(bg, p); err != nil {
		log.Printf("pipeline-api: preview %s: %v (sweeper retries)", p.ID, err)
	}
	now := s.d.Now().UTC()
	if _, err := store.UpdatePreviewPhase(bg, s.d.DB, p.ID, store.UpdatePreviewPhaseOpts{
		Phase: phaseFailedApplication, Message: cause.Error(), CompletedAt: &now,
	}); err != nil {
		log.Printf("pipeline-api: preview %s: mark failed: %v", p.ID, err)
	}
	if err := s.deleteOwnTuple(bg, p); err != nil {
		log.Printf("pipeline-api: preview %s: %v (sweeper retries)", p.ID, err)
	}
	return cause
}

// List returns one page of a project's previews; workspace "*" lists all.
func (s *Service) List(ctx context.Context, projectID uuid.UUID, workspace string, limit int, cursor string) ([]store.Preview, string, error) {
	return store.ListPreviews(ctx, s.d.DB, projectID, workspace, limit, cursor)
}

// Cancel stops a preview. Tables committed before the cancel stay until TTL.
func (s *Service) Cancel(ctx context.Context, projectID, id uuid.UUID) error {
	p, err := store.GetPreview(ctx, s.d.DB, projectID, id)
	if err != nil {
		return err
	}
	return s.cancel(ctx, p)
}

func (s *Service) cancel(ctx context.Context, p *store.Preview) error {
	if err := s.stop(ctx, p); err != nil {
		return err
	}
	if isTerminal(p.Phase) {
		return nil
	}
	bg, cancel := context.WithTimeout(context.WithoutCancel(ctx), detachedTimeout)
	defer cancel()
	now := s.d.Now().UTC()
	if _, err := store.UpdatePreviewPhase(bg, s.d.DB, p.ID, store.UpdatePreviewPhaseOpts{
		Phase: phaseCancelled, Message: cancelledMessage, CompletedAt: &now, GuardTerminal: true,
	}); err != nil {
		return fmt.Errorf("update preview phase: %w", err)
	}
	p.Phase, p.Message, p.CompletedAt = phaseCancelled, cancelledMessage, &now
	return nil
}

// stop revokes the preview's lakekeeper access first (a failure is logged and
// left to the sweeper), then signals its pods and deletes its PipelineRun.
func (s *Service) stop(ctx context.Context, p *store.Preview) error {
	bg, cancel := context.WithTimeout(context.WithoutCancel(ctx), tupleDeleteTimeout)
	err := s.deleteOwnTuple(bg, p)
	cancel()
	if err != nil {
		log.Printf("pipeline-api: preview %s: %v (sweeper retries)", p.ID, err)
	}
	s.annotatePodsCancelled(ctx, p)
	return s.deletePipelineRun(ctx, p)
}

// Delete cancels a running preview and purges it now.
func (s *Service) Delete(ctx context.Context, projectID, id uuid.UUID) error {
	p, err := store.GetPreview(ctx, s.d.DB, projectID, id)
	if err != nil {
		return err
	}
	if p.CleanedAt != nil {
		return nil
	}
	if !isTerminal(p.Phase) {
		if err := s.cancel(ctx, p); err != nil {
			return err
		}
	}
	return s.Purge(ctx, p)
}

// DeleteWorkspace deletes every uncleaned preview of a workspace. A failure
// to list the workspace is reported under uuid.Nil.
func (s *Service) DeleteWorkspace(ctx context.Context, projectID uuid.UUID, ws string) (deleted []uuid.UUID, failed map[uuid.UUID]string) {
	failed = map[uuid.UUID]string{}
	ids, err := store.ListPreviewIDsInWorkspace(ctx, s.d.DB, projectID, ws)
	if err != nil {
		failed[uuid.Nil] = err.Error()
		return nil, failed
	}
	for _, id := range ids {
		if err := s.Delete(ctx, projectID, id); err != nil {
			failed[id] = err.Error()
			continue
		}
		deleted = append(deleted, id)
	}
	return deleted, failed
}

func (s *Service) tuplesEnabled(lk string) bool { return lk != "" && s.d.Authz != nil }

// deleteOwnTuple removes the preview's run-identity editor tuple and records
// that it is gone. A preview without a tuple is recorded as gone too.
func (s *Service) deleteOwnTuple(ctx context.Context, p *store.Preview) error {
	if p.FGATupleDeletedAt != nil {
		return nil
	}
	if s.tuplesEnabled(p.LakekeeperProjectID) {
		if err := authz.DeleteProjectTuple(ctx, s.d.Authz, p.ID.String(), authz.RelationEditor, p.LakekeeperProjectID); err != nil {
			return fmt.Errorf("delete FGA tuple: %w", err)
		}
	}
	now := s.d.Now().UTC()
	if err := store.MarkPreviewTupleDeleted(ctx, s.d.DB, p.ID, now); err != nil {
		return err
	}
	p.FGATupleDeletedAt = &now
	return nil
}

func (s *Service) deletePipelineRun(ctx context.Context, p *store.Preview) error {
	pr := &datupletv1.PipelineRun{ObjectMeta: metav1.ObjectMeta{
		Name: pkg8s.PreviewPipelineRunName(p.ID), Namespace: p.K8sNamespace,
	}}
	err := s.d.K8s.Delete(ctx, pr, client.PropagationPolicy(metav1.DeletePropagationForeground))
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete PipelineRun: %w", err)
	}
	return nil
}

// annotatePodsCancelled is best-effort: the gateway sidecar watches the
// datuplet.io/cancel annotation and exits.
func (s *Service) annotatePodsCancelled(ctx context.Context, p *store.Preview) {
	pods := &corev1.PodList{}
	if err := s.d.K8s.List(ctx, pods, client.InNamespace(p.K8sNamespace),
		client.MatchingLabels{"datuplet.io/run-id": p.ID.String()}); err != nil {
		log.Printf("pipeline-api: preview %s: list pods to cancel: %v (best-effort)", p.ID, err)
		return
	}
	patch := client.RawPatch(types.MergePatchType, []byte(`{"metadata":{"annotations":{"datuplet.io/cancel":"true"}}}`))
	for i := range pods.Items {
		if err := s.d.K8s.Patch(ctx, &pods.Items[i], patch); err != nil {
			log.Printf("pipeline-api: preview %s: annotate pod %s: %v (best-effort)", p.ID, pods.Items[i].Name, err)
		}
	}
}

func isTerminal(phase string) bool { return phase != phasePending && phase != phaseRunning }

func hasError(fs []validate.Finding) bool { return firstError(fs) != "" }

func firstError(fs []validate.Finding) string {
	for _, f := range fs {
		if f.Severity == "error" {
			return f.Path + ": " + f.Message
		}
	}
	return ""
}
