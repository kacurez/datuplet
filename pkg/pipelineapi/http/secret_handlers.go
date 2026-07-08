package http

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"regexp"
	"sort"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	pkg8s "github.com/datuplet/datuplet/pkg/pipelineapi/k8s"
)

// secretKeyPattern bounds the {key} path segment. Keys become K8s
// Secret.data map keys and the suffix of a "datuplet.io/updated-<key>"
// annotation name, so the charset is kept conservative on both counts.
var secretKeyPattern = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)

// secretUpdatedAnnotationPrefix + key is the annotation pipeline-api
// stamps with the RFC3339 write timestamp every time a key's value
// changes; removed (merge-patch null) when the key is deleted.
const secretUpdatedAnnotationPrefix = "datuplet.io/updated-"

// secretListEntryJSON is one row of GET /secrets: name + last-updated
// timestamp only. The Secret's value is never serialized — this handler
// must never read or expose Secret.Data values.
type secretListEntryJSON struct {
	Key       string `json:"key"`
	UpdatedAt string `json:"updatedAt"`
}

// getManagedSecret fetches the project's managed Secret. A NotFound Secret
// is not an error to callers that treat "no secrets yet" as a valid state
// (list, delete's not-found path) — ok=false with err=nil signals that.
func (s *Server) getManagedSecret(ctx context.Context, namespace string) (sec *corev1.Secret, ok bool, err error) {
	sec = &corev1.Secret{}
	getErr := s.secretsK8s.Get(ctx, types.NamespacedName{Name: pkg8s.ProjectSecretsName, Namespace: namespace}, sec)
	if apierrors.IsNotFound(getErr) {
		return nil, false, nil
	}
	if getErr != nil {
		return nil, false, getErr
	}
	return sec, true, nil
}

// handleListSecrets is GET /api/v1/projects/{pid}/secrets. Any project
// member (datuplet_member — satisfied by viewer, editor, or data_admin)
// may list secret names + their last-updated timestamps. Values are never
// returned.
func (s *Server) handleListSecrets(w http.ResponseWriter, r *http.Request) {
	_, projectID, ok := s.mustHaveRelation(w, r, "datuplet_member")
	if !ok {
		return
	}
	namespace := pkg8s.NamespaceForProject(projectID)
	sec, found, err := s.getManagedSecret(r.Context(), namespace)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "get secrets")
		return
	}
	out := make([]secretListEntryJSON, 0)
	if found {
		for key := range sec.Data {
			out = append(out, secretListEntryJSON{
				Key:       key,
				UpdatedAt: sec.Annotations[secretUpdatedAnnotationPrefix+key],
			})
		}
		sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	}
	writeJSON(w, http.StatusOK, out)
}

// handlePutSecret is PUT /api/v1/projects/{pid}/secrets/{key}, body
// {"value":"..."}. Requires data_admin. Lazily ensures the project
// namespace + managed Secret exist, then merge-PATCHes in the new key —
// a merge-PATCH (not a full update) so a concurrent key from another PUT
// is never clobbered.
func (s *Server) handlePutSecret(w http.ResponseWriter, r *http.Request) {
	_, projectID, ok := s.mustHaveRelation(w, r, "data_admin")
	if !ok {
		return
	}
	key := r.PathValue("key")
	if !secretKeyPattern.MatchString(key) {
		writeError(w, http.StatusBadRequest, "invalid key: must match ^[A-Za-z0-9_-]+$")
		return
	}
	var body struct {
		Value string `json:"value"`
	}
	if err := readJSON(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid body: "+err.Error())
		return
	}

	// Lazy ensure: reuse the single namespace-ensure path
	// (pkg8s.EnsureProjectNamespace) also used by run-trigger, then ensure
	// the managed Secret exists in it.
	if err := pkg8s.EnsureProjectNamespace(r.Context(), s.secretsK8s, projectID); err != nil {
		writeError(w, http.StatusInternalServerError, "ensure namespace: "+err.Error())
		return
	}
	namespace := pkg8s.NamespaceForProject(projectID)
	if err := pkg8s.EnsureProjectSecret(r.Context(), s.secretsK8s, namespace); err != nil {
		writeError(w, http.StatusInternalServerError, "ensure secret: "+err.Error())
		return
	}

	now := s.secretsClock().UTC().Format(time.RFC3339)
	payload, err := json.Marshal(map[string]any{
		"data": map[string]any{
			key: base64.StdEncoding.EncodeToString([]byte(body.Value)),
		},
		"metadata": map[string]any{
			"annotations": map[string]any{
				secretUpdatedAnnotationPrefix + key: now,
			},
		},
	})
	if err != nil {
		writeError(w, http.StatusInternalServerError, "build patch")
		return
	}
	target := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: pkg8s.ProjectSecretsName, Namespace: namespace}}
	if err := s.secretsK8s.Patch(r.Context(), target, client.RawPatch(types.MergePatchType, payload)); err != nil {
		writeError(w, http.StatusInternalServerError, "patch secret: "+err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleDeleteSecret is DELETE /api/v1/projects/{pid}/secrets/{key}.
// Requires data_admin. 404 when the managed Secret doesn't exist yet, or
// exists but lacks the key. Removes only the named key via a merge-PATCH
// with a null value — merge-patch null deletes the map entry — leaving
// every other key untouched.
func (s *Server) handleDeleteSecret(w http.ResponseWriter, r *http.Request) {
	_, projectID, ok := s.mustHaveRelation(w, r, "data_admin")
	if !ok {
		return
	}
	key := r.PathValue("key")
	if !secretKeyPattern.MatchString(key) {
		writeError(w, http.StatusBadRequest, "invalid key: must match ^[A-Za-z0-9_-]+$")
		return
	}
	namespace := pkg8s.NamespaceForProject(projectID)
	sec, found, err := s.getManagedSecret(r.Context(), namespace)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "get secret")
		return
	}
	if !found {
		writeError(w, http.StatusNotFound, "secret key not found")
		return
	}
	if _, exists := sec.Data[key]; !exists {
		writeError(w, http.StatusNotFound, "secret key not found")
		return
	}

	payload, err := json.Marshal(map[string]any{
		"data": map[string]any{key: nil},
		"metadata": map[string]any{
			"annotations": map[string]any{secretUpdatedAnnotationPrefix + key: nil},
		},
	})
	if err != nil {
		writeError(w, http.StatusInternalServerError, "build patch")
		return
	}
	target := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: pkg8s.ProjectSecretsName, Namespace: namespace}}
	if err := s.secretsK8s.Patch(r.Context(), target, client.RawPatch(types.MergePatchType, payload)); err != nil {
		writeError(w, http.StatusInternalServerError, "patch secret: "+err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
