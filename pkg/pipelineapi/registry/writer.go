package registry

import (
	"context"
	"errors"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"

	datupletv1 "github.com/datuplet/datuplet/pkg/k8s/api/v1"
)

// ErrInvalidDefinition marks a client-side problem with a submitted
// ComponentDefinition: unparseable YAML, a missing metadata.name, or a
// metadata.name that disagrees with the requested name. Writer.Put wraps
// these so the REST handler can map them to 400. A K8s API failure
// (Get/Create/Update) is returned unwrapped so the handler returns 500.
var ErrInvalidDefinition = errors.New("invalid component definition")

// Writer upserts and hard-deletes ComponentDefinition CRs. It backs the
// superadmin-gated REST admin endpoints (RFC 026 P3). The CLI
// `admin component register` shares the same Upsert core so there is a single
// write path. Definition validation is NOT performed here — the
// componentdefinition-controller sets status.phase Valid/Invalid
// asynchronously; Writer only persists the CR.
type Writer struct {
	c client.Client
}

// NewWriter constructs a Writer over c.
func NewWriter(c client.Client) *Writer { return &Writer{c: c} }

// Put unmarshals specYAML into a ComponentDefinition, verifies its
// metadata.name is non-empty and matches name, and upserts it. A parse error,
// empty metadata.name, or name mismatch is wrapped in ErrInvalidDefinition.
func (wr *Writer) Put(ctx context.Context, name string, specYAML []byte) error {
	def := &datupletv1.ComponentDefinition{}
	if err := yaml.Unmarshal(specYAML, def); err != nil {
		return fmt.Errorf("%w: unmarshal ComponentDefinition YAML: %v", ErrInvalidDefinition, err)
	}
	if def.Name == "" {
		return fmt.Errorf("%w: ComponentDefinition YAML has no metadata.name", ErrInvalidDefinition)
	}
	if def.Name != name {
		return fmt.Errorf("%w: metadata.name %q does not match URL name %q", ErrInvalidDefinition, def.Name, name)
	}
	return Upsert(ctx, wr.c, def)
}

// Delete hard-deletes the named ComponentDefinition CR (no CLI equivalent —
// the CLI's `deprecate` is a soft flag). A missing CR surfaces as an
// apierrors.IsNotFound error the REST handler maps to 404.
func (wr *Writer) Delete(ctx context.Context, name string) error {
	def := &datupletv1.ComponentDefinition{ObjectMeta: metav1.ObjectMeta{Name: name}}
	return wr.c.Delete(ctx, def)
}

// Upsert Creates or Updates def in K8s using Get-then-Create/Update rather
// than true server-side apply, mirroring pkg/pipelineapi/k8s.ApplyPipelineCRD's
// documented rationale: SSA needs a FieldManager and behaves awkwardly against
// the fake client used in tests, so Get+Create/Update is the established
// equivalent for this codebase. Shared by Writer.Put and the CLI
// `admin component register` path so the upsert mechanics live in one place.
func Upsert(ctx context.Context, c client.Client, def *datupletv1.ComponentDefinition) error {
	def.TypeMeta = metav1.TypeMeta{APIVersion: "datuplet.io/v1", Kind: "ComponentDefinition"}
	def.ResourceVersion = "" // Client.Create rejects a set RV.

	existing := &datupletv1.ComponentDefinition{}
	err := c.Get(ctx, types.NamespacedName{Name: def.Name}, existing)
	if apierrors.IsNotFound(err) {
		if createErr := c.Create(ctx, def); createErr != nil {
			return fmt.Errorf("create ComponentDefinition: %w", createErr)
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("get ComponentDefinition: %w", err)
	}
	// Replace spec, preserve resourceVersion so the update succeeds.
	def.ResourceVersion = existing.ResourceVersion
	if err := c.Update(ctx, def); err != nil {
		return fmt.Errorf("update ComponentDefinition: %w", err)
	}
	return nil
}
