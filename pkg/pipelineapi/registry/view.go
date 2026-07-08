// Package registry provides pipeline-api's live view of the component
// registry: a validate.RegistryView backed by ComponentDefinition CRs listed
// from the cluster via the same controller-runtime client run-trigger uses.
package registry

import (
	"context"
	"fmt"
	"sync"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"

	datupletv1 "github.com/datuplet/datuplet/pkg/k8s/api/v1"
	"github.com/datuplet/datuplet/pkg/pipeline/validate"
)

// defaultTTL is used when NewView is called with ttl <= 0.
const defaultTTL = 10 * time.Second

// listTimeout bounds a single List call against the cluster so a slow/hung
// API server doesn't stall a save or catalog request indefinitely.
const listTimeout = 5 * time.Second

// View is a TTL-cached validate.RegistryView. It lists ComponentDefinition
// CRs via a controller-runtime client and builds a validate.StaticRegistry
// from them, refreshing the snapshot at most once per ttl so concurrent
// Resolve/List calls (pipeline saves, the catalog handlers) share the List
// cost. Resolve DELEGATES to the StaticRegistry snapshot rather than
// hand-constructing a validate.ResolvedComponent: StaticRegistry.Resolve is
// what populates the unexported rawConfigSchema field ValidateConfig needs
// for its x-datuplet-secret walk — a hand-built ResolvedComponent would leave
// that field empty and silently no-op config-schema secret-ref validation.
type View struct {
	c   client.Client
	ttl time.Duration
	now func() time.Time // overridden in tests for deterministic TTL expiry

	mu        sync.Mutex
	items     []datupletv1.ComponentDefinition
	static    validate.StaticRegistry
	fetchedAt time.Time
	primed    bool
}

// NewView constructs a TTL-cached RegistryView over c. ttl <= 0 defaults to
// 10 seconds.
func NewView(c client.Client, ttl time.Duration) *View {
	if ttl <= 0 {
		ttl = defaultTTL
	}
	return &View{c: c, ttl: ttl, now: time.Now}
}

// snapshot returns the cached (items, StaticRegistry) pair, refreshing from
// the cluster first if the cache is stale or has never been populated.
func (v *View) snapshot(ctx context.Context) ([]datupletv1.ComponentDefinition, validate.StaticRegistry, error) {
	v.mu.Lock()
	defer v.mu.Unlock()

	if v.primed && v.now().Sub(v.fetchedAt) < v.ttl {
		return v.items, v.static, nil
	}

	listCtx, cancel := context.WithTimeout(ctx, listTimeout)
	defer cancel()

	var list datupletv1.ComponentDefinitionList
	if err := v.c.List(listCtx, &list); err != nil {
		return nil, nil, err
	}

	static := make(validate.StaticRegistry, len(list.Items))
	for _, item := range list.Items {
		static[item.Name] = item
	}

	v.items = list.Items
	v.static = static
	v.fetchedAt = v.now()
	v.primed = true
	return v.items, v.static, nil
}

// Resolve implements validate.RegistryView by delegating to the cached
// StaticRegistry snapshot (see View doc comment for why delegation, not
// hand-construction, is required).
func (v *View) Resolve(component, version string) (*validate.ResolvedComponent, []validate.Finding) {
	_, static, err := v.snapshot(context.Background())
	if err != nil {
		return nil, []validate.Finding{{
			Path:     "component",
			Message:  fmt.Sprintf("component registry unavailable: %v", err),
			Severity: "error",
		}}
	}
	return static.Resolve(component, version)
}

// List returns the cached ComponentDefinition snapshot, refreshing it first
// if stale. Used by the /api/v1/components catalog handlers.
func (v *View) List(ctx context.Context) ([]datupletv1.ComponentDefinition, error) {
	items, _, err := v.snapshot(ctx)
	return items, err
}
