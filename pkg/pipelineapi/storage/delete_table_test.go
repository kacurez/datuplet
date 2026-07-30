package storage

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/datuplet/datuplet/pkg/pipelineapi/auth"
	"github.com/datuplet/datuplet/pkg/pipelineapi/authz"
	"github.com/datuplet/datuplet/pkg/pipelineapi/authz/authztest"
	"github.com/datuplet/datuplet/pkg/pipelineapi/projectgate"
)

// dataAdminFake grants data_admin (the destructive-route relation) for
// stubUser. The authztest fake is exact-match on relation, so a data_admin
// grant does NOT imply datuplet_member here and vice versa — which is exactly
// what makes these tests able to prove WHICH relation the handler checks.
func dataAdminFake() *authztest.Fake {
	f := authztest.New()
	f.Allow(authz.UserObject(stubUser.ID.String()).String(), "data_admin", authz.ProjectObject(fixtureLakekeeperProjectID))
	return f
}

// newDeleteTestServer wires just the DELETE route the same way server.go does.
func newDeleteTestServer(t *testing.T, svc *Service, resolver auth.UserResolver, authzr *authztest.Fake) *httptest.Server {
	t.Helper()
	h := &HTTPHandlers{
		Svc: svc,
		Gate: &projectgate.Gate{
			LakekeeperProjectIDFor: svc.LakekeeperProjectIDFor,
			Authorizer:             authzr,
		},
	}
	mux := http.NewServeMux()
	mux.Handle("DELETE /api/v1/storage/projects/{pid}/tables/{ns}/{t}",
		auth.WithUser(resolver, http.HandlerFunc(h.DeleteTable)))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func deleteReq(t *testing.T, srv *httptest.Server, pid, ns, table string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodDelete, srv.URL+"/api/v1/storage/projects/"+pid+"/tables/"+ns+"/"+table, nil)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("DELETE: %v", err)
	}
	t.Cleanup(func() { resp.Body.Close() })
	return resp
}

// The destructive route must require data_admin, NOT the datuplet_member that
// every read handler accepts. A user who can browse a table must not be able
// to destroy it.
func TestDeleteTable_MembershipAloneIsForbidden(t *testing.T) {
	svc := makeFixtureServiceWithLK(t)
	// allowedFake grants ONLY datuplet_member — the read-path relation.
	srv := newDeleteTestServer(t, svc, stubResolver{}, allowedFake())

	resp := deleteReq(t, srv, fixtureProjectID, "raw", "simple")
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %d, want 403 — datuplet_member must not authorize a delete", resp.StatusCode)
	}
}

func TestDeleteTable_NonMemberIsForbidden(t *testing.T) {
	svc := makeFixtureServiceWithLK(t)
	srv := newDeleteTestServer(t, svc, stubResolver{}, deniedFake())

	resp := deleteReq(t, srv, fixtureProjectID, "raw", "simple")
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", resp.StatusCode)
	}
}

func TestDeleteTable_Unauthenticated(t *testing.T) {
	svc := makeFixtureServiceWithLK(t)
	srv := newDeleteTestServer(t, svc, unauthResolver{}, dataAdminFake())

	resp := deleteReq(t, srv, fixtureProjectID, "raw", "simple")
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", resp.StatusCode)
	}
}

// With data_admin granted but no lakekeeper configured, the handler must
// refuse with 501 rather than falling back to the directory walker. There is
// no safe walker delete path, and inventing one out of prefix removal is
// precisely the class of bug this codebase avoids.
func TestDeleteTable_WithoutLakekeeperIsNotImplemented(t *testing.T) {
	svc := makeFixtureServiceWithLK(t) // LakekeeperURL == ""
	srv := newDeleteTestServer(t, svc, stubResolver{}, dataAdminFake())

	resp := deleteReq(t, srv, fixtureProjectID, "raw", "simple")
	if resp.StatusCode != http.StatusNotImplemented {
		t.Fatalf("status = %d, want 501 (no lakekeeper => refuse, never walker-delete)", resp.StatusCode)
	}
}

// Identifier validation must run before any catalog work, so a traversal-ish
// or malformed identifier is rejected as a 400 and never reaches lakekeeper.
// Authorization is granted here so a 400 can only come from validation.
func TestDeleteTable_RejectsBadIdentifiers(t *testing.T) {
	svc := makeFixtureServiceWithLK(t)
	srv := newDeleteTestServer(t, svc, stubResolver{}, dataAdminFake())

	for _, tc := range []struct{ name, ns, table string }{
		{"dot-dot namespace", "..", "simple"},
		{"dot-dot table", "raw", ".."},
		{"empty-ish table", "raw", "%20"},
		{"quote in table", "raw", "a'b"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resp := deleteReq(t, srv, fixtureProjectID, tc.ns, tc.table)
			if resp.StatusCode != http.StatusBadRequest && resp.StatusCode != http.StatusNotFound {
				t.Fatalf("status = %d, want 400 (or 404 from the router); a bad identifier must never reach the catalog", resp.StatusCode)
			}
		})
	}
}

func TestDeleteTable_InvalidProjectID(t *testing.T) {
	svc := makeFixtureServiceWithLK(t)
	srv := newDeleteTestServer(t, svc, stubResolver{}, dataAdminFake())

	resp := deleteReq(t, srv, "not-a-uuid", "raw", "simple")
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
}
