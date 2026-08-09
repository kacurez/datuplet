package apps_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/datuplet/datuplet/pkg/pipelineapi/apps"
	"github.com/datuplet/datuplet/pkg/pipelineapi/auth"
	"github.com/datuplet/datuplet/pkg/pipelineapi/authz"
	"github.com/datuplet/datuplet/pkg/pipelineapi/store"
)

// ---------------------------------------------------------------------------
// Fakes
// ---------------------------------------------------------------------------

// fakeResolver is a minimal auth.UserResolver: it either authenticates every
// request as `user` or authenticates none (the 401 half of the auth matrix).
type fakeResolver struct {
	user   *store.User
	authed bool
}

func (f *fakeResolver) UserFor(http.ResponseWriter, *http.Request) (*store.User, bool, error) {
	if !f.authed {
		return nil, false, nil
	}
	return f.user, true, nil
}
func (f *fakeResolver) Mode() string        { return "test" }
func (f *fakeResolver) SupportsLogin() bool { return false }

// fakeAuthz answers every Check with `allow` and records the (user, relation,
// object) triples it was asked about, so tests can assert the routes gate on
// the same relations the pipeline routes use.
type fakeAuthz struct {
	allow   bool
	checked []string
}

func (f *fakeAuthz) Check(_ context.Context, user, relation string, obj authz.Object) (bool, error) {
	f.checked = append(f.checked, user+"|"+relation+"|"+obj.String())
	return f.allow, nil
}
func (f *fakeAuthz) BatchCheck(context.Context, []authz.CheckQuery) ([]bool, []error) {
	panic("unused")
}
func (f *fakeAuthz) ListObjects(context.Context, string, string, authz.ObjectType) ([]authz.Object, error) {
	panic("unused")
}
func (f *fakeAuthz) WriteTuples(context.Context, []authz.Tuple) error  { panic("unused") }
func (f *fakeAuthz) DeleteTuples(context.Context, []authz.Tuple) error { panic("unused") }

// fakeProjects is the ProjectLookup seam: it maps the Datuplet project UUID
// to a lakekeeper project id without needing the row to be provisioned.
type fakeProjects struct {
	lakekeeperID string
	err          error
}

func (f *fakeProjects) LakekeeperProjectID(context.Context, uuid.UUID) (string, error) {
	return f.lakekeeperID, f.err
}

// orderingIdentity wraps apps.RecorderIdentity with a DB probe so the
// tuple-then-rows delete invariant is provable rather than assumed: when
// Unregister runs it records how many `apps` rows still exist. A recorded
// "rows=1" plus a gone row after the request proves Unregister ran FIRST.
type orderingIdentity struct {
	apps.RecorderIdentity
	pool *pgxpool.Pool
}

func (o *orderingIdentity) Unregister(ctx context.Context, appID, projectID string) error {
	var n int
	if err := o.pool.QueryRow(ctx,
		`SELECT count(*) FROM apps WHERE id = $1`, uuid.MustParse(appID),
	).Scan(&n); err != nil {
		return err
	}
	o.Calls = append(o.Calls, fmt.Sprintf("probe:apps_rows=%d", n))
	return o.RecorderIdentity.Unregister(ctx, appID, projectID)
}

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

type appsHarness struct {
	t         *testing.T
	pool      *pgxpool.Pool
	store     *apps.Store
	ident     *orderingIdentity
	authz     *fakeAuthz
	projects  *fakeProjects
	resolver  *fakeResolver
	mux       http.Handler
	projectID uuid.UUID
}

func newAppsHarness(t *testing.T) *appsHarness {
	t.Helper()
	pool, cleanup := testStore(t)
	t.Cleanup(cleanup)

	h := &appsHarness{
		t:        t,
		pool:     pool,
		store:    apps.NewStore(pool),
		ident:    &orderingIdentity{pool: pool},
		authz:    &fakeAuthz{allow: true},
		projects: &fakeProjects{lakekeeperID: "lk-project-1"},
		resolver: &fakeResolver{user: &store.User{ID: uuid.New(), Email: "a@b.c"}, authed: true},
	}
	h.projectID = testProject(t, pool, "proj-apps")

	handlers := &apps.Handlers{
		Store:    h.store,
		Identity: h.ident,
		Authz:    h.authz,
		Projects: h.projects,
	}
	mux := http.NewServeMux()
	handlers.Register(mux, func(next http.Handler) http.Handler {
		return auth.WithUser(h.resolver, next)
	})
	h.mux = mux
	return h
}

func (h *appsHarness) do(method, path, body string) *httptest.ResponseRecorder {
	h.t.Helper()
	var r *http.Request
	if body == "" {
		r = httptest.NewRequest(method, path, nil)
	} else {
		r = httptest.NewRequest(method, path, bytes.NewReader([]byte(body)))
		r.Header.Set("Content-Type", "application/json")
	}
	w := httptest.NewRecorder()
	h.mux.ServeHTTP(w, r)
	return w
}

func (h *appsHarness) appsPath(suffix string) string {
	return "/api/v1/projects/" + h.projectID.String() + "/apps" + suffix
}

func putBody(bundle []byte) string {
	return `{"bundle_base64":"` + base64.StdEncoding.EncodeToString(bundle) + `"}`
}

// putApp uploads a bundle and returns the decoded {app_id, version_hash}.
func (h *appsHarness) putApp(name string, bundle []byte) (appID, hash string) {
	h.t.Helper()
	w := h.do(http.MethodPut, h.appsPath("/"+name), putBody(bundle))
	if w.Code != http.StatusOK {
		h.t.Fatalf("PUT %s: status = %d, body = %s", name, w.Code, w.Body.String())
	}
	var got struct {
		AppID       string `json:"app_id"`
		VersionHash string `json:"version_hash"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		h.t.Fatalf("decode PUT response: %v (body %s)", err, w.Body.String())
	}
	return got.AppID, got.VersionHash
}

func decodeBody(t *testing.T, w *httptest.ResponseRecorder, dst any) {
	t.Helper()
	if err := json.Unmarshal(w.Body.Bytes(), dst); err != nil {
		t.Fatalf("decode body: %v (body %s)", err, w.Body.String())
	}
}

// ---------------------------------------------------------------------------
// PUT /api/v1/projects/{pid}/apps/{name}
// ---------------------------------------------------------------------------

func TestPutApp_CreatesAppRegistersIdentityAndMovesDraft(t *testing.T) {
	h := newAppsHarness(t)
	bundle := []byte("export async function render(ctx) { return {}; }")

	appID, hash := h.putApp("dash1", bundle)

	if _, err := uuid.Parse(appID); err != nil {
		t.Fatalf("app_id = %q, want a UUID: %v", appID, err)
	}
	if want := hexHash(bundle); hash != want {
		t.Fatalf("version_hash = %q, want %q (hex sha256 of raw bundle)", hash, want)
	}
	// Create must Register the app's FGA identity (spec §5.4), keyed by the
	// Datuplet project UUID (P1: P4's Register maps it to the lakekeeper id).
	want := "Register:" + appID + ":" + h.projectID.String()
	if len(h.ident.Calls) != 1 || h.ident.Calls[0] != want {
		t.Fatalf("identity calls = %v, want exactly [%s]", h.ident.Calls, want)
	}
	// Upload moves draft, never production (spec §5.1).
	resolved, err := h.store.Resolve(context.Background(), h.projectID, "dash1", "draft")
	if err != nil {
		t.Fatalf("Resolve draft: %v", err)
	}
	if resolved.VersionHash != hash {
		t.Fatalf("draft hash = %q, want %q", resolved.VersionHash, hash)
	}
	if _, err := h.store.Resolve(context.Background(), h.projectID, "dash1", "production"); err == nil {
		t.Fatal("production channel is set after a plain upload; want unset")
	}
}

func TestPutApp_SecondUploadDoesNotReRegister(t *testing.T) {
	h := newAppsHarness(t)
	appID, hash1 := h.putApp("dash1", []byte("v1"))
	appID2, hash2 := h.putApp("dash1", []byte("v2"))

	if appID2 != appID {
		t.Fatalf("app_id changed across uploads: %q -> %q", appID, appID2)
	}
	if hash1 == hash2 {
		t.Fatal("distinct bundles produced the same version hash")
	}
	if len(h.ident.Calls) != 1 {
		t.Fatalf("identity calls = %v, want a single Register across two uploads", h.ident.Calls)
	}
}

func TestPutApp_IdempotentSameBundle(t *testing.T) {
	h := newAppsHarness(t)
	bundle := []byte("same bytes")
	_, hash1 := h.putApp("dash1", bundle)
	_, hash2 := h.putApp("dash1", bundle)
	if hash1 != hash2 {
		t.Fatalf("hashes differ for identical bundles: %q vs %q", hash1, hash2)
	}
}

func TestPutApp_InvalidName(t *testing.T) {
	h := newAppsHarness(t)
	bad := []string{
		"Dash1",                       // uppercase
		"-dash",                       // leading dash
		"dash-",                       // trailing dash
		"da_sh",                       // underscore
		"dash.1",                      // dot
		strings.Repeat("a", 64),       // 64 > 63 chars
		"a" + strings.Repeat("b", 63), // 64 chars, valid alphabet
	}
	for _, name := range bad {
		w := h.do(http.MethodPut, h.appsPath("/"+name), putBody([]byte("x")))
		if w.Code != http.StatusBadRequest {
			t.Errorf("PUT name=%q: status = %d, want 400 (body %s)", name, w.Code, w.Body.String())
		}
	}
	good := []string{"a", "a1", "dash-1", "d" + strings.Repeat("a", 61) + "9"}
	for _, name := range good {
		w := h.do(http.MethodPut, h.appsPath("/"+name), putBody([]byte("x")))
		if w.Code != http.StatusOK {
			t.Errorf("PUT name=%q: status = %d, want 200 (body %s)", name, w.Code, w.Body.String())
		}
	}
}

func TestPutApp_BadBody(t *testing.T) {
	h := newAppsHarness(t)
	cases := map[string]string{
		"not json":            `{`,
		"missing field":       `{}`,
		"empty bundle":        `{"bundle_base64":""}`,
		"not base64":          `{"bundle_base64":"!!!!not base64!!!!"}`,
		"unknown field":       `{"bundle_base64":"aGk=","surprise":1}`,
		"wrong field type":    `{"bundle_base64":123}`,
		"empty decoded bytes": `{"bundle_base64":"===="}`,
	}
	for name, body := range cases {
		w := h.do(http.MethodPut, h.appsPath("/dash1"), body)
		if w.Code != http.StatusBadRequest {
			t.Errorf("%s: status = %d, want 400 (body %s)", name, w.Code, w.Body.String())
		}
	}
}

func TestPutApp_TooLarge(t *testing.T) {
	h := newAppsHarness(t)
	// One byte over the 5 MB raw cap (spec §4/§7) → 413, not a 500.
	w := h.do(http.MethodPut, h.appsPath("/dash1"), putBody(bytes.Repeat([]byte("a"), apps.MaxBundleBytes+1)))
	if w.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413 (body %s)", w.Code, w.Body.String())
	}
}

// ---------------------------------------------------------------------------
// GET detail / list
// ---------------------------------------------------------------------------

type channelJSON struct {
	VersionHash string `json:"version_hash"`
	UpdatedAt   string `json:"updated_at"`
}

type appDetailJSON struct {
	AppID     string                 `json:"app_id"`
	Name      string                 `json:"name"`
	CreatedAt string                 `json:"created_at"`
	Channels  map[string]channelJSON `json:"channels"`
	Versions  []struct {
		Hash      string `json:"hash"`
		SizeBytes int64  `json:"size_bytes"`
		CreatedAt string `json:"created_at"`
	} `json:"versions"`
}

func TestGetApp_DetailHasChannelsAndVersions(t *testing.T) {
	h := newAppsHarness(t)
	appID, hash1 := h.putApp("dash1", []byte("v1"))
	_, hash2 := h.putApp("dash1", []byte("v2"))
	if err := h.store.Promote(context.Background(), appID, hash1, ""); err != nil {
		t.Fatalf("Promote: %v", err)
	}

	w := h.do(http.MethodGet, h.appsPath("/dash1"), "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body %s)", w.Code, w.Body.String())
	}
	var got appDetailJSON
	decodeBody(t, w, &got)

	if got.AppID != appID || got.Name != "dash1" {
		t.Fatalf("app_id/name = %q/%q, want %q/dash1", got.AppID, got.Name, appID)
	}
	if got.Channels["draft"].VersionHash != hash2 {
		t.Fatalf("draft = %q, want %q", got.Channels["draft"].VersionHash, hash2)
	}
	if got.Channels["production"].VersionHash != hash1 {
		t.Fatalf("production = %q, want %q", got.Channels["production"].VersionHash, hash1)
	}
	if len(got.Versions) != 2 {
		t.Fatalf("versions = %d, want 2 (%+v)", len(got.Versions), got.Versions)
	}
	// Newest-first.
	if got.Versions[0].Hash != hash2 || got.Versions[1].Hash != hash1 {
		t.Fatalf("versions order = %q,%q, want newest-first %q,%q",
			got.Versions[0].Hash, got.Versions[1].Hash, hash2, hash1)
	}
	if got.Versions[0].SizeBytes != int64(len("v2")) {
		t.Fatalf("size_bytes = %d, want %d (raw size)", got.Versions[0].SizeBytes, len("v2"))
	}
}

func TestGetApp_NotFound(t *testing.T) {
	h := newAppsHarness(t)
	if w := h.do(http.MethodGet, h.appsPath("/nope"), ""); w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404 (body %s)", w.Code, w.Body.String())
	}
}

func TestListApps(t *testing.T) {
	h := newAppsHarness(t)
	if w := h.do(http.MethodGet, h.appsPath(""), ""); w.Code != http.StatusOK {
		t.Fatalf("empty list status = %d, want 200", w.Code)
	} else {
		var empty []appDetailJSON
		decodeBody(t, w, &empty)
		if len(empty) != 0 {
			t.Fatalf("empty project list = %+v, want []", empty)
		}
	}

	_, hashB := h.putApp("b-dash", []byte("b"))
	_, hashA := h.putApp("a-dash", []byte("a"))

	w := h.do(http.MethodGet, h.appsPath(""), "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body %s)", w.Code, w.Body.String())
	}
	var got []appDetailJSON
	decodeBody(t, w, &got)
	if len(got) != 2 {
		t.Fatalf("list = %d entries, want 2 (%+v)", len(got), got)
	}
	if got[0].Name != "a-dash" || got[1].Name != "b-dash" {
		t.Fatalf("list order = %q,%q, want a-dash,b-dash", got[0].Name, got[1].Name)
	}
	if got[0].Channels["draft"].VersionHash != hashA || got[1].Channels["draft"].VersionHash != hashB {
		t.Fatalf("list channels wrong: %+v", got)
	}
}

// ---------------------------------------------------------------------------
// DELETE — the tuple-then-rows security invariant
// ---------------------------------------------------------------------------

func TestDeleteApp_UnregistersIdentityBeforeDeletingRows(t *testing.T) {
	h := newAppsHarness(t)
	appID, _ := h.putApp("dash1", []byte("v1"))
	if _, _, err := mintViaHTTP(h, "dash1"); err != nil {
		t.Fatalf("mint token: %v", err)
	}
	h.ident.Calls = nil // drop the Register from create

	w := h.do(http.MethodDelete, h.appsPath("/dash1"), "")
	if w.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204 (body %s)", w.Code, w.Body.String())
	}

	// Ordering proof: the probe recorded from inside Unregister saw the app
	// row still present, and Unregister precedes nothing else in the log.
	wantCalls := []string{
		"probe:apps_rows=1",
		"Unregister:" + appID + ":" + h.projectID.String(),
	}
	if fmt.Sprint(h.ident.Calls) != fmt.Sprint(wantCalls) {
		t.Fatalf("identity calls = %v, want %v (tuple removal must precede row deletion)",
			h.ident.Calls, wantCalls)
	}

	// ...and the rows really are gone afterwards, cascades included.
	ctx := context.Background()
	for _, q := range []string{
		`SELECT count(*) FROM apps WHERE id = $1`,
		`SELECT count(*) FROM app_versions WHERE app_id = $1`,
		`SELECT count(*) FROM app_channels WHERE app_id = $1`,
		`SELECT count(*) FROM app_viewer_tokens WHERE app_id = $1`,
	} {
		var n int
		if err := h.pool.QueryRow(ctx, q, uuid.MustParse(appID)).Scan(&n); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		if n != 0 {
			t.Fatalf("%s: %d rows remain after delete, want 0", q, n)
		}
	}

	if w := h.do(http.MethodDelete, h.appsPath("/dash1"), ""); w.Code != http.StatusNotFound {
		t.Fatalf("re-delete status = %d, want 404", w.Code)
	}
}

func TestDeleteApp_KeepsRowsWhenUnregisterFails(t *testing.T) {
	h := newAppsHarness(t)
	appID, _ := h.putApp("dash1", []byte("v1"))

	failing := &failingIdentity{}
	handlers := &apps.Handlers{Store: h.store, Identity: failing, Authz: h.authz, Projects: h.projects}
	mux := http.NewServeMux()
	handlers.Register(mux, func(next http.Handler) http.Handler { return auth.WithUser(h.resolver, next) })

	r := httptest.NewRequest(http.MethodDelete, h.appsPath("/dash1"), nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, r)
	if w.Code < 500 {
		t.Fatalf("status = %d, want a 5xx when the tuple delete fails (body %s)", w.Code, w.Body.String())
	}
	var n int
	if err := h.pool.QueryRow(context.Background(),
		`SELECT count(*) FROM apps WHERE id = $1`, uuid.MustParse(appID)).Scan(&n); err != nil {
		t.Fatalf("count apps: %v", err)
	}
	if n != 1 {
		t.Fatalf("apps rows = %d, want 1 — rows must survive a failed tuple delete", n)
	}
}

type failingIdentity struct{ apps.RecorderIdentity }

func (f *failingIdentity) Unregister(context.Context, string, string) error {
	return fmt.Errorf("fga unavailable")
}

// ---------------------------------------------------------------------------
// POST .../promote
// ---------------------------------------------------------------------------

func TestPromote_HappyPathCASAndUnknownVersion(t *testing.T) {
	h := newAppsHarness(t)
	_, hash1 := h.putApp("dash1", []byte("v1"))
	_, hash2 := h.putApp("dash1", []byte("v2"))

	// First promote: expectedProduction null.
	w := h.do(http.MethodPost, h.appsPath("/dash1/promote"),
		`{"version":"`+hash1+`","expectedProduction":null}`)
	if w.Code != http.StatusOK {
		t.Fatalf("first promote status = %d, want 200 (body %s)", w.Code, w.Body.String())
	}
	var got struct {
		ProductionVersion string `json:"production_version"`
	}
	decodeBody(t, w, &got)
	if got.ProductionVersion != hash1 {
		t.Fatalf("production_version = %q, want %q", got.ProductionVersion, hash1)
	}

	// Stale CAS → 409.
	w = h.do(http.MethodPost, h.appsPath("/dash1/promote"),
		`{"version":"`+hash2+`","expectedProduction":null}`)
	if w.Code != http.StatusConflict {
		t.Fatalf("stale CAS status = %d, want 409 (body %s)", w.Code, w.Body.String())
	}

	// Correct CAS → 200.
	w = h.do(http.MethodPost, h.appsPath("/dash1/promote"),
		`{"version":"`+hash2+`","expectedProduction":"`+hash1+`"}`)
	if w.Code != http.StatusOK {
		t.Fatalf("CAS promote status = %d, want 200 (body %s)", w.Code, w.Body.String())
	}

	// Unknown version → 400 (settled: a bad request, not a 404).
	unknown := strings.Repeat("0", 64)
	w = h.do(http.MethodPost, h.appsPath("/dash1/promote"),
		`{"version":"`+unknown+`"}`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("unknown-version status = %d, want 400 (body %s)", w.Code, w.Body.String())
	}

	// Missing version → 400.
	if w := h.do(http.MethodPost, h.appsPath("/dash1/promote"), `{}`); w.Code != http.StatusBadRequest {
		t.Fatalf("missing-version status = %d, want 400", w.Code)
	}
	// Unknown app → 404.
	if w := h.do(http.MethodPost, h.appsPath("/nope/promote"),
		`{"version":"`+hash1+`"}`); w.Code != http.StatusNotFound {
		t.Fatalf("unknown-app status = %d, want 404", w.Code)
	}
}

// ---------------------------------------------------------------------------
// GET .../logs
// ---------------------------------------------------------------------------

func TestLogs_ListAndByRequestID(t *testing.T) {
	h := newAppsHarness(t)
	appID, hash := h.putApp("dash1", []byte("v1"))
	ctx := context.Background()

	base := time.Now().UTC().Add(-time.Hour).Truncate(time.Millisecond)
	var ids []string
	for i := 0; i < 3; i++ {
		rec := apps.RenderLogRecord{
			RequestID:     uuid.NewString(),
			AppID:         appID,
			VersionHash:   hash,
			Channel:       "draft",
			PrincipalKind: "user",
			PrincipalID:   "u1",
			StartedAt:     base.Add(time.Duration(i) * time.Minute),
			DurationMS:    int64(10 + i),
			Outcome:       "ok",
			LogText:       fmt.Sprintf("log %d", i),
		}
		if err := h.store.AppendRenderLog(ctx, rec); err != nil {
			t.Fatalf("AppendRenderLog: %v", err)
		}
		ids = append(ids, rec.RequestID)
	}

	w := h.do(http.MethodGet, h.appsPath("/dash1/logs"), "")
	if w.Code != http.StatusOK {
		t.Fatalf("list status = %d, want 200 (body %s)", w.Code, w.Body.String())
	}
	var list []struct {
		RequestID  string `json:"request_id"`
		Outcome    string `json:"outcome"`
		LogText    string `json:"log_text"`
		DurationMS int64  `json:"duration_ms"`
	}
	decodeBody(t, w, &list)
	if len(list) != 3 {
		t.Fatalf("logs = %d, want 3 (%+v)", len(list), list)
	}
	if list[0].RequestID != ids[2] {
		t.Fatalf("first log = %q, want newest-first %q", list[0].RequestID, ids[2])
	}

	// ?request_id=<known> → exactly that record.
	w = h.do(http.MethodGet, h.appsPath("/dash1/logs?request_id="+ids[1]), "")
	if w.Code != http.StatusOK {
		t.Fatalf("single status = %d, want 200 (body %s)", w.Code, w.Body.String())
	}
	var one struct {
		RequestID string `json:"request_id"`
		LogText   string `json:"log_text"`
	}
	decodeBody(t, w, &one)
	if one.RequestID != ids[1] || one.LogText != "log 1" {
		t.Fatalf("single record = %+v, want request_id %q / log 1", one, ids[1])
	}

	// ?request_id=<unknown but well-formed> → 404.
	w = h.do(http.MethodGet, h.appsPath("/dash1/logs?request_id="+uuid.NewString()), "")
	if w.Code != http.StatusNotFound {
		t.Fatalf("unknown request_id status = %d, want 404 (body %s)", w.Code, w.Body.String())
	}
	// Malformed request_id → 400.
	w = h.do(http.MethodGet, h.appsPath("/dash1/logs?request_id=not-a-uuid"), "")
	if w.Code != http.StatusBadRequest {
		t.Fatalf("malformed request_id status = %d, want 400", w.Code)
	}
	// Unknown app → 404.
	if w := h.do(http.MethodGet, h.appsPath("/nope/logs"), ""); w.Code != http.StatusNotFound {
		t.Fatalf("unknown app logs status = %d, want 404", w.Code)
	}
}

// ---------------------------------------------------------------------------
// Viewer tokens
// ---------------------------------------------------------------------------

// mintViaHTTP mints a viewer token through the route and returns
// (token_id, full `vw_…` token).
func mintViaHTTP(h *appsHarness, name string) (tokenID, token string, err error) {
	w := h.do(http.MethodPost, h.appsPath("/"+name+"/tokens"), "")
	if w.Code != http.StatusCreated {
		return "", "", fmt.Errorf("status = %d, want 201 (body %s)", w.Code, w.Body.String())
	}
	var got struct {
		TokenID string `json:"token_id"`
		Token   string `json:"token"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		return "", "", err
	}
	return got.TokenID, got.Token, nil
}

func TestCreateToken_ReturnsPlaintextExactlyOnce(t *testing.T) {
	h := newAppsHarness(t)
	appID, _ := h.putApp("dash1", []byte("v1"))

	tokenID, token, err := mintViaHTTP(h, "dash1")
	if err != nil {
		t.Fatalf("mint: %v", err)
	}
	if _, err := uuid.Parse(tokenID); err != nil {
		t.Fatalf("token_id = %q, want a UUID: %v", tokenID, err)
	}
	// Wire format: vw_<token_id>.<secret> (spec §5.3).
	if !strings.HasPrefix(token, "vw_"+tokenID+".") {
		t.Fatalf("token = %q, want prefix %q", token, "vw_"+tokenID+".")
	}
	secret := strings.TrimPrefix(token, "vw_"+tokenID+".")
	if secret == "" {
		t.Fatal("token carries an empty secret")
	}
	ok, err := h.store.VerifyToken(context.Background(), appID, tokenID, secret)
	if err != nil || !ok {
		t.Fatalf("VerifyToken(minted secret) = (%v, %v), want (true, nil)", ok, err)
	}

	// The plaintext must never appear again on any author route.
	for _, path := range []string{h.appsPath("/dash1"), h.appsPath(""), h.appsPath("/dash1/logs")} {
		w := h.do(http.MethodGet, path, "")
		if strings.Contains(w.Body.String(), secret) {
			t.Fatalf("GET %s leaked the token secret", path)
		}
		if strings.Contains(w.Body.String(), "vw_") {
			t.Fatalf("GET %s leaked a viewer token", path)
		}
	}
	// A second mint yields a different token.
	_, token2, err := mintViaHTTP(h, "dash1")
	if err != nil {
		t.Fatalf("second mint: %v", err)
	}
	if token2 == token {
		t.Fatal("two mints produced the same token")
	}
}

func TestDeleteToken_RevokesAndIsNotFoundTwiceOver(t *testing.T) {
	h := newAppsHarness(t)
	appID, _ := h.putApp("dash1", []byte("v1"))
	tokenID, token, err := mintViaHTTP(h, "dash1")
	if err != nil {
		t.Fatalf("mint: %v", err)
	}
	secret := strings.TrimPrefix(token, "vw_"+tokenID+".")

	w := h.do(http.MethodDelete, h.appsPath("/dash1/tokens/"+tokenID), "")
	if w.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204 (body %s)", w.Code, w.Body.String())
	}
	ok, err := h.store.VerifyToken(context.Background(), appID, tokenID, secret)
	if err != nil {
		t.Fatalf("VerifyToken: %v", err)
	}
	if ok {
		t.Fatal("token still verifies after revocation")
	}
	// Unknown token id → 404; malformed → 400.
	if w := h.do(http.MethodDelete, h.appsPath("/dash1/tokens/"+uuid.NewString()), ""); w.Code != http.StatusNotFound {
		t.Fatalf("unknown token status = %d, want 404", w.Code)
	}
	if w := h.do(http.MethodDelete, h.appsPath("/dash1/tokens/nope"), ""); w.Code != http.StatusBadRequest {
		t.Fatalf("malformed token status = %d, want 400", w.Code)
	}
	// Tokens of another app are not deletable through this app's path.
	otherID, otherToken, err := func() (string, string, error) {
		h.putApp("dash2", []byte("v1"))
		return mintViaHTTP(h, "dash2")
	}()
	if err != nil {
		t.Fatalf("mint on dash2: %v", err)
	}
	_ = otherToken
	if w := h.do(http.MethodDelete, h.appsPath("/dash1/tokens/"+otherID), ""); w.Code != http.StatusNotFound {
		t.Fatalf("cross-app token delete status = %d, want 404", w.Code)
	}
}

// ---------------------------------------------------------------------------
// GET .../tokens — list (RFC 028 token-list gap, Part 5 phase gate)
// ---------------------------------------------------------------------------

// tokenListItemJSON is this test file's decode target for the list-tokens
// response — RevokedAt has no `omitempty` tag so a missing key and an
// explicit `null` are distinguishable via the raw body text (checked
// separately below); decoding-wise both give a nil pointer either way.
type tokenListItemJSON struct {
	TokenID   string  `json:"token_id"`
	CreatedAt string  `json:"created_at"`
	RevokedAt *string `json:"revoked_at"`
}

func TestListTokens_ReturnsSummariesNotSecrets(t *testing.T) {
	h := newAppsHarness(t)
	h.putApp("dash1", []byte("v1"))

	// Empty before any mint.
	w := h.do(http.MethodGet, h.appsPath("/dash1/tokens"), "")
	if w.Code != http.StatusOK {
		t.Fatalf("empty list status = %d, want 200 (body %s)", w.Code, w.Body.String())
	}
	var empty []tokenListItemJSON
	decodeBody(t, w, &empty)
	if len(empty) != 0 {
		t.Fatalf("empty token list = %+v, want []", empty)
	}

	tokenID1, _, err := mintViaHTTP(h, "dash1")
	if err != nil {
		t.Fatalf("mint 1: %v", err)
	}
	tokenID2, _, err := mintViaHTTP(h, "dash1")
	if err != nil {
		t.Fatalf("mint 2: %v", err)
	}
	if w := h.do(http.MethodDelete, h.appsPath("/dash1/tokens/"+tokenID1), ""); w.Code != http.StatusNoContent {
		t.Fatalf("revoke status = %d, want 204 (body %s)", w.Code, w.Body.String())
	}

	w = h.do(http.MethodGet, h.appsPath("/dash1/tokens"), "")
	if w.Code != http.StatusOK {
		t.Fatalf("list status = %d, want 200 (body %s)", w.Code, w.Body.String())
	}
	body := w.Body.String()
	lower := strings.ToLower(body)
	if strings.Contains(body, "vw_") || strings.Contains(lower, "secret") || strings.Contains(lower, "salt") {
		t.Fatalf("token list response must never carry a secret/salt: %s", body)
	}
	// An active token's revoked_at must serialize as a literal null, not be
	// omitted (the route's documented contract).
	if !strings.Contains(body, `"revoked_at":null`) {
		t.Fatalf("active token's revoked_at must be literal null in the response, got body %s", body)
	}

	var got []tokenListItemJSON
	decodeBody(t, w, &got)
	if len(got) != 2 {
		t.Fatalf("list = %d entries, want 2 (%+v)", len(got), got)
	}
	// Newest-first: tokenID2 (minted second) sorts before tokenID1.
	if got[0].TokenID != tokenID2 || got[1].TokenID != tokenID1 {
		t.Fatalf("order = [%s, %s], want newest-first [%s, %s]",
			got[0].TokenID, got[1].TokenID, tokenID2, tokenID1)
	}
	if got[0].RevokedAt != nil {
		t.Fatalf("active token (tokenID2) revoked_at = %v, want nil", got[0].RevokedAt)
	}
	if got[1].RevokedAt == nil {
		t.Fatalf("revoked token (tokenID1) revoked_at = nil, want set")
	}

	// Unknown app -> 404, same as every other named author route.
	if w := h.do(http.MethodGet, h.appsPath("/nope/tokens"), ""); w.Code != http.StatusNotFound {
		t.Fatalf("unknown app status = %d, want 404 (body %s)", w.Code, w.Body.String())
	}
}

// ---------------------------------------------------------------------------
// Auth matrix — every route, 401 unauthenticated + 403 non-member
// ---------------------------------------------------------------------------

// authMatrixRoutes covers every author route registered by Register.
func authMatrixRoutes(h *appsHarness, tokenID string) []struct {
	method, path, body string
} {
	return []struct {
		method, path, body string
	}{
		{http.MethodPut, h.appsPath("/dash1"), putBody([]byte("v1"))},
		{http.MethodGet, h.appsPath(""), ""},
		{http.MethodGet, h.appsPath("/dash1"), ""},
		{http.MethodDelete, h.appsPath("/dash1"), ""},
		{http.MethodPost, h.appsPath("/dash1/promote"), `{"version":"` + strings.Repeat("a", 64) + `"}`},
		{http.MethodGet, h.appsPath("/dash1/logs"), ""},
		{http.MethodPost, h.appsPath("/dash1/tokens"), ""},
		{http.MethodGet, h.appsPath("/dash1/tokens"), ""},
		{http.MethodDelete, h.appsPath("/dash1/tokens/" + tokenID), ""},
	}
}

func TestAuthMatrix_401WithoutAuth(t *testing.T) {
	h := newAppsHarness(t)
	h.putApp("dash1", []byte("v1"))
	tokenID, _, err := mintViaHTTP(h, "dash1")
	if err != nil {
		t.Fatalf("mint: %v", err)
	}

	h.resolver.authed = false
	for _, rt := range authMatrixRoutes(h, tokenID) {
		w := h.do(rt.method, rt.path, rt.body)
		if w.Code != http.StatusUnauthorized {
			t.Errorf("%s %s: status = %d, want 401 (body %s)", rt.method, rt.path, w.Code, w.Body.String())
		}
	}
}

func TestAuthMatrix_403ForNonMember(t *testing.T) {
	h := newAppsHarness(t)
	h.putApp("dash1", []byte("v1"))
	tokenID, _, err := mintViaHTTP(h, "dash1")
	if err != nil {
		t.Fatalf("mint: %v", err)
	}

	h.authz.allow = false
	h.authz.checked = nil // drop the setup calls; assert only on the matrix below
	for _, rt := range authMatrixRoutes(h, tokenID) {
		w := h.do(rt.method, rt.path, rt.body)
		if w.Code != http.StatusForbidden {
			t.Errorf("%s %s: status = %d, want 403 (body %s)", rt.method, rt.path, w.Code, w.Body.String())
		}
	}
	// Every route asked the FGA backend about the request's user and the
	// project object — no route skips the check.
	if len(h.authz.checked) < len(authMatrixRoutes(h, tokenID)) {
		t.Fatalf("authz checks = %d, want at least one per route (%v)", len(h.authz.checked), h.authz.checked)
	}
	for _, c := range h.authz.checked {
		if !strings.HasSuffix(c, "|project:lk-project-1") {
			t.Fatalf("authz check %q does not target project:lk-project-1", c)
		}
		if !strings.HasPrefix(c, "user:oidc~"+h.resolver.user.ID.String()+"|") {
			t.Fatalf("authz check %q does not name the request user", c)
		}
	}
}

func TestRoutes_MalformedProjectAndUnprovisionedProject(t *testing.T) {
	h := newAppsHarness(t)
	// Malformed {pid} → 400 before any store work.
	r := httptest.NewRequest(http.MethodGet, "/api/v1/projects/not-a-uuid/apps", nil)
	w := httptest.NewRecorder()
	h.mux.ServeHTTP(w, r)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("malformed pid status = %d, want 400 (body %s)", w.Code, w.Body.String())
	}
	// Lakekeeper project not yet provisioned → 503 (mirrors mustHaveRelation).
	h.projects.lakekeeperID = ""
	if w := h.do(http.MethodGet, h.appsPath(""), ""); w.Code != http.StatusServiceUnavailable {
		t.Fatalf("unprovisioned project status = %d, want 503 (body %s)", w.Code, w.Body.String())
	}
}
