package storage

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/datuplet/datuplet/pkg/pipelineapi/auth"
	"github.com/datuplet/datuplet/pkg/pipelineapi/authz"
	"github.com/datuplet/datuplet/pkg/pipelineapi/authz/authztest"
	"github.com/datuplet/datuplet/pkg/pipelineapi/projectgate"
	"github.com/datuplet/datuplet/pkg/pipelineapi/projectgate/projectgatetest"
	"github.com/datuplet/datuplet/pkg/pipelineapi/queryproxy"
	"github.com/datuplet/datuplet/pkg/pipelineapi/storage/testdata"
	"github.com/datuplet/datuplet/pkg/pipelineapi/store"
)

// fixtureProjectID mirrors the UUID testdata.GenerateSimple bakes into
// the warehouse path. Tests hit the same project so the resolver +
// authz stubs can return a fixed user/grant tied to it.
const fixtureProjectID = "00000000-0000-0000-0000-000000000002"

// fixtureLakekeeperProjectID is a synthetic lakekeeper project UUID used
// by the test Service's LakekeeperProjectIDFor resolver. The storage handler
// resolves this from the Datuplet project ID and passes it to the FGA check.
const fixtureLakekeeperProjectID = "aaaaaaaa-0000-0000-0000-000000000002"

// stubUser mirrors the local-mode hard-coded user so the handlers'
// authz check has a stable userID to pair with the fixture project.
var stubUser = &store.User{
	ID:    uuid.MustParse("00000000-0000-0000-0000-000000000001"),
	Email: "test@example.com",
}

// stubResolver always returns stubUser, mirroring auth.LocalResolver.
type stubResolver struct{}

func (stubResolver) UserFor(_ http.ResponseWriter, _ *http.Request) (*store.User, bool, error) {
	return stubUser, true, nil
}
func (stubResolver) Mode() string        { return "test" }
func (stubResolver) SupportsLogin() bool { return false }

// unauthResolver returns (nil, false, nil) — triggers 401 via WithUser.
type unauthResolver struct{}

func (unauthResolver) UserFor(_ http.ResponseWriter, _ *http.Request) (*store.User, bool, error) {
	return nil, false, nil
}
func (unauthResolver) Mode() string        { return "test" }
func (unauthResolver) SupportsLogin() bool { return false }

// allowedFake returns an *authztest.Fake seeded with a datuplet_member grant
// for stubUser on fixtureLakekeeperProjectID. Use for "membership granted" tests.
func allowedFake() *authztest.Fake {
	f := authztest.New()
	userStr := authz.UserObject(stubUser.ID.String()).String()
	f.Allow(userStr, "datuplet_member", authz.ProjectObject(fixtureLakekeeperProjectID))
	return f
}

// deniedFake returns an empty *authztest.Fake (no tuples) — every Check
// returns (false, nil), simulating a non-member. Use for "forbidden" tests.
func deniedFake() *authztest.Fake { return authztest.New() }

// makeFixtureServiceWithLK returns a Service with a LakekeeperProjectIDFor
// resolver that maps any Datuplet project UUID to fixtureLakekeeperProjectID.
// Use when tests need the FGA resolveProject path to succeed.
func makeFixtureServiceWithLK(t *testing.T) *Service {
	t.Helper()
	svc := makeFixtureService(t)
	svc.LakekeeperProjectIDFor = func(_ context.Context, _ uuid.UUID) (string, error) {
		return fixtureLakekeeperProjectID, nil
	}
	return svc
}

// newTestServer wires a fresh HTTPHandlers + httptest.Server on a real
// ServeMux with route patterns matching the production wiring. Each
// caller gets its own mux + server so t.Parallel() would be safe even
// though we don't use it here.
func newTestServer(t *testing.T, svc *Service, resolver auth.UserResolver, authzr *authztest.Fake) *httptest.Server {
	t.Helper()
	// Gate is built from the SAME stubs the fixture Service already carries
	// (svc.LakekeeperProjectIDFor) plus authzr — resolveProject delegates
	// to it instead of calling h.Svc.LakekeeperProjectIDFor + an authorizer
	// directly. WarehouseFor is left nil: every test here uses the
	// fixture-walker path (LakekeeperURL == ""), so resolveWarehouse is
	// never invoked.
	h := &HTTPHandlers{
		Svc: svc,
		Gate: &projectgate.Gate{
			LakekeeperProjectIDFor: svc.LakekeeperProjectIDFor,
			Authorizer:             authzr,
		},
	}
	mux := http.NewServeMux()
	mux.Handle("GET /api/v1/storage/projects/{pid}/tables",
		auth.WithUser(resolver, http.HandlerFunc(h.ListTables)))
	mux.Handle("GET /api/v1/storage/projects/{pid}/tables/{ns}/{t}/info",
		auth.WithUser(resolver, http.HandlerFunc(h.TableInfo)))
	mux.Handle("GET /api/v1/storage/projects/{pid}/tables/{ns}/{t}/schema",
		auth.WithUser(resolver, http.HandlerFunc(h.TableSchema)))
	mux.Handle("GET /api/v1/storage/projects/{pid}/tables/{ns}/{t}/preview",
		auth.WithUser(resolver, http.HandlerFunc(h.Preview)))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

// makeFixtureService regenerates the simple/orphan/empty fixtures into
// a tempdir and returns a Service rooted there. Tests that want to
// exercise identifier errors or unknown projects just reuse this.
func makeFixtureService(t *testing.T) *Service {
	t.Helper()
	warehouse := filepath.Join(t.TempDir(), "warehouse")
	testdata.GenerateAll(t, warehouse)
	return &Service{
		WarehouseURI: "file://" + warehouse,
		OrgName:      "myorg",
		AllowLocal:   true,
	}
}

func TestListTables_Success(t *testing.T) {
	svc := makeFixtureServiceWithLK(t)
	srv := newTestServer(t, svc, stubResolver{}, allowedFake())

	resp, err := http.Get(srv.URL + "/api/v1/storage/projects/" + fixtureProjectID + "/tables")
	if err != nil {
		t.Fatalf("GET tables: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var body struct {
		Tables []tableListEntry `json:"tables"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	// Fixture has public/simple as the only resolvable table (orphan has
	// bad metadata, empty has no metadata — both silently dropped).
	if len(body.Tables) != 1 {
		t.Fatalf("got %d tables, want 1 (%v)", len(body.Tables), body.Tables)
	}
	if got, want := body.Tables[0].Namespace, "public"; got != want {
		t.Errorf("namespace = %q, want %q", got, want)
	}
	if got, want := body.Tables[0].Name, "simple"; got != want {
		t.Errorf("name = %q, want %q", got, want)
	}
	if body.Tables[0].CurrentSnapshotID == 0 {
		t.Errorf("expected non-zero snapshot id")
	}
}

func TestListTables_UnknownProject(t *testing.T) {
	// Matches Iceberg catalog semantics: listing tables under an
	// unknown project returns an empty list, not 404. Also avoids
	// exposing which project IDs exist to authenticated-but-non-member
	// callers (though here the caller is a member per the authz stub).
	svc := makeFixtureServiceWithLK(t)
	srv := newTestServer(t, svc, stubResolver{}, allowedFake())

	unknown := "00000000-0000-0000-0000-000000000099"
	resp, err := http.Get(srv.URL + "/api/v1/storage/projects/" + unknown + "/tables")
	if err != nil {
		t.Fatalf("GET tables: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var body struct {
		Tables []tableListEntry `json:"tables"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(body.Tables) != 0 {
		t.Errorf("expected empty tables, got %v", body.Tables)
	}
}

func TestListTables_NoMembership(t *testing.T) {
	svc := makeFixtureServiceWithLK(t)
	srv := newTestServer(t, svc, stubResolver{}, deniedFake())

	resp, err := http.Get(srv.URL + "/api/v1/storage/projects/" + fixtureProjectID + "/tables")
	if err != nil {
		t.Fatalf("GET tables: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", resp.StatusCode)
	}
	// The gate-error body must carry "kind" alongside "error" — parity with
	// the query proxy's {"error","kind"} shape for the same shared
	// projectgate.Error (RFC 025 Codex review, Minor finding).
	var body map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body["kind"] != "forbidden" {
		t.Errorf(`kind = %q, want "forbidden"`, body["kind"])
	}
}

func TestListTables_NoSession(t *testing.T) {
	svc := makeFixtureServiceWithLK(t)
	srv := newTestServer(t, svc, unauthResolver{}, allowedFake())

	resp, err := http.Get(srv.URL + "/api/v1/storage/projects/" + fixtureProjectID + "/tables")
	if err != nil {
		t.Fatalf("GET tables: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", resp.StatusCode)
	}
}

func TestListTables_InvalidProjectID(t *testing.T) {
	svc := makeFixtureServiceWithLK(t)
	srv := newTestServer(t, svc, stubResolver{}, allowedFake())

	resp, err := http.Get(srv.URL + "/api/v1/storage/projects/not-a-uuid/tables")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
	var body map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body["kind"] != "bad_request" {
		t.Errorf(`kind = %q, want "bad_request"`, body["kind"])
	}
}

// TestListTables_WarehouseResolutionFails: on the lakekeeper-configured
// path, a resolveWarehouse gate failure (e.g. no warehouse registered yet
// for the project) must carry "kind" alongside "error" — same parity as
// resolveProject and the query proxy (Minor finding, RFC 025 Codex review).
func TestListTables_WarehouseResolutionFails(t *testing.T) {
	svc := makeFixtureServiceWithLK(t)
	svc.LakekeeperURL = "http://lakekeeper.invalid" // never dialed: warehouse resolution fails first
	h := &HTTPHandlers{
		Svc: svc,
		Gate: &projectgate.Gate{
			LakekeeperProjectIDFor: svc.LakekeeperProjectIDFor,
			Authorizer:             allowedFake(),
			WarehouseFor: func(_ context.Context, _ string) (string, error) {
				return "", errors.New("no warehouse registered")
			},
		},
	}
	mux := http.NewServeMux()
	mux.Handle("GET /api/v1/storage/projects/{pid}/tables",
		auth.WithUser(stubResolver{}, http.HandlerFunc(h.ListTables)))
	srv := httptest.NewServer(mux)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/api/v1/storage/projects/" + fixtureProjectID + "/tables")
	if err != nil {
		t.Fatalf("GET tables: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", resp.StatusCode)
	}
	var body map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body["kind"] != "unavailable" {
		t.Errorf(`kind = %q, want "unavailable"`, body["kind"])
	}
}

func TestTableSchema_Success(t *testing.T) {
	svc := makeFixtureServiceWithLK(t)
	srv := newTestServer(t, svc, stubResolver{}, allowedFake())

	url := srv.URL + "/api/v1/storage/projects/" + fixtureProjectID + "/tables/public/simple/schema"
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET schema: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var body schemaResp
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(body.Columns) != 2 {
		t.Fatalf("columns = %d, want 2 (%v)", len(body.Columns), body.Columns)
	}
	if body.Columns[0].Name != "id" || body.Columns[0].Type != "long" {
		t.Errorf("col[0] = %+v, want id/long", body.Columns[0])
	}
	if body.Columns[1].Name != "name" || body.Columns[1].Type != "string" {
		t.Errorf("col[1] = %+v, want name/string", body.Columns[1])
	}
	// Both fixture columns are required, so nullable=false.
	for _, c := range body.Columns {
		if c.Nullable {
			t.Errorf("col %q nullable=true, want false", c.Name)
		}
	}
}

func TestTableSchema_InvalidIdentifier(t *testing.T) {
	svc := makeFixtureServiceWithLK(t)
	srv := newTestServer(t, svc, stubResolver{}, allowedFake())

	// Path segment ".." fails ValidIdentifier (first char must be alphanumeric).
	// The mux strips dot-segments before dispatch, so we target a segment
	// that's non-empty but starts with a dot/underscore — the canonical
	// "invalid identifier" case.
	url := srv.URL + "/api/v1/storage/projects/" + fixtureProjectID + "/tables/_bad/simple/schema"
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 (body=%v)", resp.StatusCode, resp.Header)
	}
}

// fakePreviewRunner implements PreviewRunner for the storage preview tests
// (RFC 025 Task 3.1). Preview no longer scans the table itself — it
// delegates the actual data read to the query-worker via this seam, so the
// walker-backed EncodeRecords tests this replaces no longer apply.
type fakePreviewRunner struct {
	res  *queryproxy.Result
	qerr *queryproxy.QueryError
	got  struct{ warehouse, ns, table string }
}

func (f *fakePreviewRunner) Preview(_ context.Context, _, warehouse, ns, table string, _ queryproxy.PreviewLimits) (*queryproxy.Result, *queryproxy.QueryError) {
	f.got.warehouse, f.got.ns, f.got.table = warehouse, ns, table
	return f.res, f.qerr
}

// previewWarehouseName is the bare warehouse name projectgatetest.AllowAll
// resolves for the preview tests below.
const previewWarehouseName = "mywarehouse"

// newPreviewTestServer wires just the Preview route against h. Preview no
// longer touches h.Svc (the query-worker owns the data read), so these
// tests build a minimal HTTPHandlers instead of reusing the fixture-walker
// newTestServer harness.
func newPreviewTestServer(t *testing.T, h *HTTPHandlers, resolver auth.UserResolver) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.Handle("GET /api/v1/storage/projects/{pid}/tables/{ns}/{t}/preview",
		auth.WithUser(resolver, http.HandlerFunc(h.Preview)))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

// TestPreview_Success covers the happy path: the fake runner's
// queryproxy.Result maps onto PreviewResponse verbatim, and Preview passes
// through the resolved ns/table/qualified-warehouse to the runner.
func TestPreview_Success(t *testing.T) {
	runner := &fakePreviewRunner{res: &queryproxy.Result{
		Schema:    []queryproxy.ResultColumn{{Name: "id", Type: "INTEGER"}},
		Rows:      [][]any{{1}},
		Truncated: true,
	}}
	h := &HTTPHandlers{
		Gate:  projectgatetest.AllowAll(fixtureLakekeeperProjectID, previewWarehouseName),
		Query: runner,
	}
	srv := newPreviewTestServer(t, h, stubResolver{})

	url := srv.URL + "/api/v1/storage/projects/" + fixtureProjectID + "/tables/public/simple/preview"
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET preview: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var body PreviewResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(body.Columns) != 1 || body.Columns[0].Name != "id" || body.Columns[0].Type != "INTEGER" {
		t.Errorf("columns = %+v, want [{id INTEGER}]", body.Columns)
	}
	if len(body.Rows) != 1 {
		t.Errorf("rows = %d, want 1 (%v)", len(body.Rows), body.Rows)
	}
	if !body.Truncated {
		t.Errorf("truncated = false, want true")
	}
	if runner.got.ns != "public" || runner.got.table != "simple" {
		t.Errorf("runner got ns/table = %q/%q, want public/simple", runner.got.ns, runner.got.table)
	}
	if want := fixtureLakekeeperProjectID + "/" + previewWarehouseName; runner.got.warehouse != want {
		t.Errorf("runner got warehouse = %q, want %q", runner.got.warehouse, want)
	}
}

// TestPreview_QueryDisabled asserts the 501 query_disabled soft-degrade
// when no query-service core is wired (h.Query left as the nil interface).
func TestPreview_QueryDisabled(t *testing.T) {
	h := &HTTPHandlers{
		Gate: projectgatetest.AllowAll(fixtureLakekeeperProjectID, previewWarehouseName),
		// Query intentionally left nil.
	}
	if h.Query != nil {
		t.Fatal("precondition: h.Query must be the nil interface")
	}
	srv := newPreviewTestServer(t, h, stubResolver{})

	url := srv.URL + "/api/v1/storage/projects/" + fixtureProjectID + "/tables/public/simple/preview"
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET preview: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotImplemented {
		t.Fatalf("status = %d, want 501", resp.StatusCode)
	}
	var body map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body["kind"] != "query_disabled" {
		t.Errorf(`kind = %q, want "query_disabled"`, body["kind"])
	}
}

// TestPreview_ResultTooLarge asserts the query-worker's result_too_large
// QueryError maps to a 413 whose message explains the cap in storage-UI
// terms ("too wide") rather than the query-worker's generic wording.
func TestPreview_ResultTooLarge(t *testing.T) {
	runner := &fakePreviewRunner{qerr: &queryproxy.QueryError{Status: http.StatusRequestEntityTooLarge, Kind: "result_too_large", Msg: "result too large"}}
	h := &HTTPHandlers{
		Gate:  projectgatetest.AllowAll(fixtureLakekeeperProjectID, previewWarehouseName),
		Query: runner,
	}
	srv := newPreviewTestServer(t, h, stubResolver{})

	url := srv.URL + "/api/v1/storage/projects/" + fixtureProjectID + "/tables/public/simple/preview"
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET preview: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413", resp.StatusCode)
	}
	var body map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !strings.Contains(body["error"], "too wide") {
		t.Errorf("error = %q, want it to contain %q", body["error"], "too wide")
	}
}

// TestPreview_InvalidIdentifier asserts identifiers are still rejected
// BEFORE the runner is invoked. "_bad" (not "../x") is the canonical
// invalid-identifier probe here — Go's ServeMux strips dot segments out of
// the path before PathValue, so "../x" never reaches the handler.
func TestPreview_InvalidIdentifier(t *testing.T) {
	runner := &fakePreviewRunner{}
	h := &HTTPHandlers{
		Gate:  projectgatetest.AllowAll(fixtureLakekeeperProjectID, previewWarehouseName),
		Query: runner,
	}
	srv := newPreviewTestServer(t, h, stubResolver{})

	url := srv.URL + "/api/v1/storage/projects/" + fixtureProjectID + "/tables/_bad/simple/preview"
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET preview: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
	if runner.got.ns != "" || runner.got.table != "" {
		t.Errorf("runner invoked despite invalid identifier: got=%+v", runner.got)
	}
}

func TestTableInfo_Success(t *testing.T) {
	svc := makeFixtureServiceWithLK(t)
	srv := newTestServer(t, svc, stubResolver{}, allowedFake())

	url := srv.URL + "/api/v1/storage/projects/" + fixtureProjectID + "/tables/public/simple/info"
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET info: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}

	var body struct {
		MetadataLocation  string          `json:"metadata_location"`
		CurrentSnapshotID int64           `json:"current_snapshot_id"`
		Snapshots         []snapshotBrief `json:"snapshots"`
		RowCount          *int64          `json:"row_count"`
		DataFileCount     *int64          `json:"data_file_count"`
		TotalFilesSize    *int64          `json:"total_files_size"`
	}
	if err := json.Unmarshal(raw, &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.MetadataLocation == "" {
		t.Error("metadata_location is empty")
	}
	if body.CurrentSnapshotID == 0 {
		t.Error("current_snapshot_id is zero")
	}
	if len(body.Snapshots) != 1 {
		t.Errorf("snapshots = %d, want 1 (fixture commits only one real snapshot)", len(body.Snapshots))
	}
	if body.RowCount == nil || *body.RowCount != 3 {
		t.Errorf("row_count = %v, want 3 (from total-records summary)", body.RowCount)
	}
	if body.DataFileCount == nil || *body.DataFileCount != 1 {
		t.Errorf("data_file_count = %v, want 1 (from total-data-files summary)", body.DataFileCount)
	}
	// total_files_size is the real parquet byte size (dynamic), so assert
	// only that it's present and positive (from the total-files-size summary).
	if body.TotalFilesSize == nil || *body.TotalFilesSize <= 0 {
		t.Errorf("total_files_size = %v, want a positive size (from total-files-size summary)", body.TotalFilesSize)
	}

	// data_files must be gone from the wire shape entirely (not just
	// empty) — decode into a permissive map to check for its absence.
	var raw2 map[string]any
	if err := json.Unmarshal(raw, &raw2); err != nil {
		t.Fatalf("decode raw: %v", err)
	}
	if _, present := raw2["data_files"]; present {
		t.Error("data_files must be gone from the wire shape")
	}
}

// TestTableInfo_NoSummary exercises a table whose current snapshot's
// Summary lacks total-records/total-data-files (e.g. a writer that
// didn't populate the standard Iceberg totals). RowCount and
// DataFileCount must come back nil rather than 0 or a manifest-derived
// approximation — the UI renders "—" for nil.
func TestTableInfo_NoSummary(t *testing.T) {
	warehouse := filepath.Join(t.TempDir(), "warehouse")
	if err := testdata.GenerateNoSummaryErr(warehouse); err != nil {
		t.Fatalf("generate no-summary fixture: %v", err)
	}
	svc := &Service{
		WarehouseURI: "file://" + warehouse,
		OrgName:      "myorg",
		AllowLocal:   true,
		LakekeeperProjectIDFor: func(_ context.Context, _ uuid.UUID) (string, error) {
			return fixtureLakekeeperProjectID, nil
		},
	}
	srv := newTestServer(t, svc, stubResolver{}, allowedFake())

	url := srv.URL + "/api/v1/storage/projects/" + fixtureProjectID + "/tables/public/nosummary/info"
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET info: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var body infoResp
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.RowCount != nil {
		t.Errorf("row_count = %v, want nil (no total-records in summary)", *body.RowCount)
	}
	if body.DataFileCount != nil {
		t.Errorf("data_file_count = %v, want nil (no total-data-files in summary)", *body.DataFileCount)
	}
	if body.TotalFilesSize != nil {
		t.Errorf("total_files_size = %v, want nil (no total-files-size in summary)", *body.TotalFilesSize)
	}
}

// TestLoadRequestedTable_RejectsSymlinkedMetadata exercises the
// defense-in-depth re-check on the resolved metadata path. The
// table-root symlink guard in loadRequestedTable already runs against
// .../tables/public/simple, but ResolveCurrentMetadata then follows
// metadata/ + picks vN.metadata.json — and if metadata/ itself is a
// symlink to outside the warehouse, the resolved metadata URI escapes
// and we'd happily read whatever's at the target.
//
// To reproduce: bootstrap a real warehouse, move the metadata/ dir of
// the fixture table OUTSIDE the warehouse, then symlink it back into
// place. The handler must return 400 — not 200 (which would mean the
// guard missed the escape) and not 500 (which would mean iceberg-go
// blew up trying to load through a now-broken state).
func TestLoadRequestedTable_RejectsSymlinkedMetadata(t *testing.T) {
	tmp := t.TempDir()
	warehouse := filepath.Join(tmp, "warehouse")
	testdata.GenerateAll(t, warehouse)

	// Locate the fixture metadata dir, move it outside the warehouse,
	// then symlink the original path to the moved location. This means
	// .../tables/public/simple itself is a regular dir (so the table-
	// root guard passes) but .../tables/public/simple/metadata is now a
	// symlink whose target lives in tmp/escaped-metadata, outside the
	// warehouse root.
	metaDir := filepath.Join(warehouse, "orgs", "myorg", "projects", fixtureProjectID, "tables", "public", "simple", "metadata")
	escaped := filepath.Join(tmp, "escaped-metadata")
	if err := os.Rename(metaDir, escaped); err != nil {
		t.Fatalf("rename metadata: %v", err)
	}
	if err := os.Symlink(escaped, metaDir); err != nil {
		t.Fatalf("symlink metadata: %v", err)
	}

	svc := &Service{
		WarehouseURI: "file://" + warehouse,
		OrgName:      "myorg",
		AllowLocal:   true,
		LakekeeperProjectIDFor: func(_ context.Context, _ uuid.UUID) (string, error) {
			return fixtureLakekeeperProjectID, nil
		},
	}
	srv := newTestServer(t, svc, stubResolver{}, allowedFake())

	url := srv.URL + "/api/v1/storage/projects/" + fixtureProjectID + "/tables/public/simple/info"
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET info: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 (symlinked metadata must be rejected, not silently followed)", resp.StatusCode)
	}
	var body map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	// Either guard is acceptable: the containment check ("metadata path
	// escapes warehouse") fires when EvalSymlinks resolves the metadata
	// URI before we get to the symlink-rejection step, and the symlink
	// guard ("metadata path traverses symlink") fires the other way
	// around. Both prove the defense-in-depth check caught the escape;
	// pinning to one specific message would couple the test to ordering.
	got := body["error"]
	if got != "metadata path escapes warehouse" && got != "metadata path traverses symlink" {
		t.Errorf("error = %q, want one of [metadata path escapes warehouse, metadata path traverses symlink]", got)
	}
}
