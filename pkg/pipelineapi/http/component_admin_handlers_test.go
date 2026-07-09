package http_test

import (
	"bytes"
	"context"
	stdhttp "net/http"
	"net/http/httptest"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/datuplet/datuplet/pkg/pipelineapi/auth"
	"github.com/datuplet/datuplet/pkg/pipelineapi/authz"
	apihttp "github.com/datuplet/datuplet/pkg/pipelineapi/http"
	"github.com/datuplet/datuplet/pkg/pipelineapi/registry"
)

// stubServerAdminExt is a test double for authz.ServerAdminChecker.
type stubServerAdminExt struct {
	result bool
	err    error
}

func (s stubServerAdminExt) ServerObject(context.Context) (string, error) { return "server:x", s.err }
func (s stubServerAdminExt) IsServerAdmin(context.Context, string) (bool, error) {
	return s.result, s.err
}

// recordingWriter records what the handlers pass to the write seam.
type recordingWriter struct {
	putCalls    int
	putName     string
	putBody     []byte
	putErr      error
	deleteCalls int
	deleteName  string
	deleteErr   error
}

func (rw *recordingWriter) Put(_ context.Context, name string, specYAML []byte) error {
	rw.putCalls++
	rw.putName = name
	rw.putBody = append([]byte(nil), specYAML...)
	return rw.putErr
}

func (rw *recordingWriter) Delete(_ context.Context, name string) error {
	rw.deleteCalls++
	rw.deleteName = name
	return rw.deleteErr
}

func newAdminServer(sa authz.ServerAdminChecker, wr apihttp.ComponentRegistryWriter, resolver auth.UserResolver) (*httptest.Server, func()) {
	srv := apihttp.NewServer(nil).
		WithUserResolver(resolver).
		WithServerAdmin(sa).
		WithComponentAdmin(wr)
	ts := httptest.NewServer(srv.Handler())
	return ts, ts.Close
}

func doReq(t *testing.T, method, url string, body []byte) *stdhttp.Response {
	t.Helper()
	var rdr *bytes.Reader
	if body != nil {
		rdr = bytes.NewReader(body)
	} else {
		rdr = bytes.NewReader(nil)
	}
	req, _ := stdhttp.NewRequest(method, url, rdr)
	resp, err := stdhttp.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	return resp
}

const adminPutYAML = `apiVersion: datuplet.io/v1
kind: ComponentDefinition
metadata:
  name: http-fetch
spec:
  defaultVersion: v1.0.0
  versions:
    - version: v1.0.0
      image: datuplet/http-fetch:v1.0.0
`

func TestPutComponentDefinition_Superadmin204(t *testing.T) {
	wr := &recordingWriter{}
	ts, cleanup := newAdminServer(stubServerAdminExt{result: true}, wr, stubResolver{})
	defer cleanup()

	resp := doReq(t, stdhttp.MethodPut, ts.URL+"/api/v1/admin/components/http-fetch", []byte(adminPutYAML))
	defer resp.Body.Close()

	if resp.StatusCode != stdhttp.StatusNoContent {
		t.Fatalf("status = %d, want 204", resp.StatusCode)
	}
	if wr.putCalls != 1 {
		t.Fatalf("putCalls = %d, want 1", wr.putCalls)
	}
	if wr.putName != "http-fetch" {
		t.Errorf("putName = %q, want http-fetch", wr.putName)
	}
	if !bytes.Equal(wr.putBody, []byte(adminPutYAML)) {
		t.Errorf("putBody = %q, want the request body verbatim", wr.putBody)
	}
}

func TestPutComponentDefinition_NonSuperadmin403(t *testing.T) {
	wr := &recordingWriter{}
	ts, cleanup := newAdminServer(stubServerAdminExt{result: false}, wr, stubResolver{})
	defer cleanup()

	resp := doReq(t, stdhttp.MethodPut, ts.URL+"/api/v1/admin/components/http-fetch", []byte(adminPutYAML))
	defer resp.Body.Close()

	if resp.StatusCode != stdhttp.StatusForbidden {
		t.Fatalf("status = %d, want 403", resp.StatusCode)
	}
	if wr.putCalls != 0 {
		t.Errorf("putCalls = %d, want 0 (writer must not run for a non-superadmin)", wr.putCalls)
	}
}

func TestPutComponentDefinition_InvalidDefinition400(t *testing.T) {
	wr := &recordingWriter{putErr: registry.ErrInvalidDefinition}
	ts, cleanup := newAdminServer(stubServerAdminExt{result: true}, wr, stubResolver{})
	defer cleanup()

	resp := doReq(t, stdhttp.MethodPut, ts.URL+"/api/v1/admin/components/http-fetch", []byte(adminPutYAML))
	defer resp.Body.Close()

	if resp.StatusCode != stdhttp.StatusBadRequest {
		t.Fatalf("status = %d, want 400 (ErrInvalidDefinition)", resp.StatusCode)
	}
}

func TestDeleteComponentDefinition_Superadmin204(t *testing.T) {
	wr := &recordingWriter{}
	ts, cleanup := newAdminServer(stubServerAdminExt{result: true}, wr, stubResolver{})
	defer cleanup()

	resp := doReq(t, stdhttp.MethodDelete, ts.URL+"/api/v1/admin/components/http-fetch", nil)
	defer resp.Body.Close()

	if resp.StatusCode != stdhttp.StatusNoContent {
		t.Fatalf("status = %d, want 204", resp.StatusCode)
	}
	if wr.deleteCalls != 1 || wr.deleteName != "http-fetch" {
		t.Errorf("deleteCalls=%d deleteName=%q, want 1/http-fetch", wr.deleteCalls, wr.deleteName)
	}
}

func TestDeleteComponentDefinition_NotFound404(t *testing.T) {
	wr := &recordingWriter{deleteErr: apierrors.NewNotFound(
		schema.GroupResource{Group: "datuplet.io", Resource: "componentdefinitions"}, "http-fetch")}
	ts, cleanup := newAdminServer(stubServerAdminExt{result: true}, wr, stubResolver{})
	defer cleanup()

	resp := doReq(t, stdhttp.MethodDelete, ts.URL+"/api/v1/admin/components/http-fetch", nil)
	defer resp.Body.Close()

	if resp.StatusCode != stdhttp.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
}

func TestPutComponentDefinition_Unauthenticated401(t *testing.T) {
	wr := &recordingWriter{}
	ts, cleanup := newAdminServer(stubServerAdminExt{result: true}, wr, &selectiveResolver{allow: false})
	defer cleanup()

	resp := doReq(t, stdhttp.MethodPut, ts.URL+"/api/v1/admin/components/http-fetch", []byte(adminPutYAML))
	defer resp.Body.Close()

	if resp.StatusCode != stdhttp.StatusUnauthorized {
		t.Fatalf("status = %d, want 401 (auth.WithUser rejects before the guard)", resp.StatusCode)
	}
	if wr.putCalls != 0 {
		t.Errorf("putCalls = %d, want 0", wr.putCalls)
	}
}
