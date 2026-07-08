package http

import (
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/datuplet/datuplet/pkg/pipelineapi/auth"
	"github.com/datuplet/datuplet/pkg/pipelineapi/authz"
	"github.com/datuplet/datuplet/pkg/pipelineapi/runbackend"
	"github.com/datuplet/datuplet/pkg/pipelineapi/storage"
	"github.com/datuplet/datuplet/pkg/pipelineapi/tokens"
)

// Server is the HTTP surface of pipeline-api.
type Server struct {
	db           *pgxpool.Pool
	signer       *tokens.Signer
	cookieSecure bool
	k8s          client.Client
	publicURL    string // base URL advertised in OIDC discovery; e.g. "http://pipeline-api.datuplet.svc.cluster.local:8081"
	staticDir    string // ui/product/ directory; empty disables /ui/*
	backend      runbackend.Backend
	resolver     auth.UserResolver
	authzr       authz.Authorizer
	projects     ProjectReader
	pipelines    PipelineStore
	runs         RunReader
	storage      *storage.Service
	// cliToken: deploy-time public URL + warehouse name returned in the
	// `POST /api/v1/auth/token` response body so the CLI can talk to
	// lakekeeper directly. Empty = endpoint stays
	// registered but emits "" — the CLI then falls back to its own
	// configured value.
	cliLakekeeperURL string
	cliWarehouseName string
	// cliTokenLimiter rate-limits POST /api/v1/auth/token by client IP.
	// Initialised in NewServer; never nil.
	cliTokenLimiter *auth.Limiter
	// queryHandler is a pre-built http.Handler for POST /api/v1/query,
	// constructed in main.go at config time (same pattern as storage.NewForLakekeeper).
	// Nil when the query service is not configured; the route stays unregistered.
	queryHandler http.Handler
	// localQueryMintHandler is the pre-built http.Handler for
	// POST /api/v1/query/token (RFC 022 §5.3) — the per-invocation query-JWT
	// mint endpoint the laptop-side `datuplet-query` CLI calls. Built in
	// main.go whenever the signer exists (so the policy-off 403 path works
	// even when client-side query is disabled). Nil → route unregistered.
	localQueryMintHandler http.Handler
	// secretsK8s is the K8s client the project-secrets handlers use to
	// lazily ensure the managed Secret and apply merge-PATCHes. Wired via
	// WithSecrets; when nil, the secrets routes are left unregistered.
	secretsK8s client.Client
	// secretsClock supplies "now" for the "datuplet.io/updated-<key>"
	// annotation. Injected (not read inside the patch helper) so tests get
	// deterministic timestamps.
	secretsClock func() time.Time
	// registry is pipeline-api's view of the component registry: Resolve is
	// threaded into ValidatePipeline on the pipeline save path (replacing
	// R5's temporary nil), and List backs the /api/v1/components catalog
	// handlers. When nil, both the catalog routes stay unregistered AND
	// handlePutPipeline validates with a nil RegistryView (registry
	// resolution + config-schema checks skipped) — same soft-degrade shape
	// as the other With* seams.
	registry ComponentRegistry
}

// NewServer constructs a Server bound to the given DB pool. db may be nil
// for tests that don't exercise the data plane.
func NewServer(db *pgxpool.Pool) *Server {
	return &Server{
		db: db,
		// 10 requests / minute / IP on POST /api/v1/auth/token.
		// The limit is intentionally tight: argon2id verification dominates
		// the latency budget on the success path, so we want to stop a
		// brute-force attempt before it consumes CPU. 10/min/IP is a
		// comfortable ceiling for legitimate operator behavior (login from
		// a single laptop, occasional re-login when the JWT TTL elapses).
		cliTokenLimiter: auth.NewLimiter(10, time.Minute),
	}
}

// WithCookieSecure toggles the Secure flag on the session cookie.
// Production deployments behind TLS should set this true.
func (s *Server) WithCookieSecure(secure bool) *Server {
	s.cookieSecure = secure
	return s
}

// WithSigner attaches a signing key for JWT minting and JWKS publishing.
// When absent, /api/v1/auth/jwks.json returns 404 and token-minting handlers
// (Plan C2b) refuse to start.
func (s *Server) WithSigner(signer *tokens.Signer) *Server {
	s.signer = signer
	return s
}

// WithK8sClient attaches the Kubernetes client used by run-trigger.
func (s *Server) WithK8sClient(c client.Client) *Server {
	s.k8s = c
	return s
}

// WithPublicURL sets the base URL advertised in the OIDC discovery doc
// served at /.well-known/openid-configuration. Lakekeeper fetches that doc
// to discover jwks_uri + issuer; the URL must be reachable from
// lakekeeper's network position. Examples:
//
//	cluster: "http://pipeline-api.datuplet.svc.cluster.local:8081"
//	local mode (lakekeeper in compose, pipeline-api on host): "http://host.docker.internal:8081"
//
// When empty (or no signer), /.well-known/openid-configuration returns 404
// — useful for tests that don't need OIDC.
func (s *Server) WithPublicURL(url string) *Server {
	s.publicURL = url
	return s
}

// WithRunBackend wires the run-trigger/cancel backend. Register as the
// last step in the builder chain — the presence of a backend gates
// registration of the trigger + cancel routes.
func (s *Server) WithRunBackend(b runbackend.Backend) *Server {
	s.backend = b
	return s
}

// WithUserResolver wires the UserResolver used by WithUser middleware to
// resolve incoming requests to a *store.User. When nil, all authenticated
// routes are left unregistered (they would nil-deref otherwise).
func (s *Server) WithUserResolver(r auth.UserResolver) *Server {
	s.resolver = r
	return s
}

// WithAuthorizer wires the OpenFGA-backed Authorizer used by
// project-scoped handlers. When nil, project /
// pipeline / run handlers are left unregistered — same gate shape as
// the resolver/store wiring above.
func (s *Server) WithAuthorizer(a authz.Authorizer) *Server {
	s.authzr = a
	return s
}

// WithProjectReader wires the /api/v1/projects handlers. Cluster mode
// passes NewPgxProjectReader(pool); local mode passes
// NewLocalProjectReader(). When nil, the project endpoints are left
// unregistered (they would nil-deref otherwise).
func (s *Server) WithProjectReader(p ProjectReader) *Server {
	s.projects = p
	return s
}

// WithPipelineStore wires the pipeline endpoints (list/get/put/delete
// and the pipeline-read path of trigger). Gate shape mirrors
// WithProjectReader.
func (s *Server) WithPipelineStore(p PipelineStore) *Server {
	s.pipelines = p
	return s
}

// WithRunReader wires the read-only run endpoints (list/get). Trigger
// and cancel flow through WithRunBackend.
func (s *Server) WithRunReader(r RunReader) *Server {
	s.runs = r
	return s
}

// WithStorage enables the /api/v1/storage/* routes backed by a
// storage.Service. When nil, the catch-all /api/v1/storage/
// handler returns 503 so callers get a predictable response instead of
// a 404 and the rest of pipeline-api keeps serving.
func (s *Server) WithStorage(svc *storage.Service) *Server {
	s.storage = svc
	return s
}

// WithCLIClusterInfo wires the deploy-time public URL of lakekeeper and
// the warehouse name that `POST /api/v1/auth/token` returns in its
// response body so the CLI can talk to lakekeeper directly. Empty values
// are tolerated — the endpoint stays registered
// and emits "" so callers can still get a token; they fall back to the
// CLI's own configured cluster info.
func (s *Server) WithCLIClusterInfo(lakekeeperPublicURL, warehouseName string) *Server {
	s.cliLakekeeperURL = lakekeeperPublicURL
	s.cliWarehouseName = warehouseName
	return s
}

// WithQueryService wires the pre-built http.Handler for POST /api/v1/query.
// The handler must have been constructed by the caller (e.g. via
// queryproxy.Handler in main.go) so that construction errors surface as a
// normal return error at config time rather than a panic at request time.
// When h is nil, the endpoint is left unregistered (callers get 404).
// The route is also gated on a non-nil UserResolver (WithUserResolver) so the
// auth.WithUser middleware can identify the caller.
func (s *Server) WithQueryService(h http.Handler) *Server {
	s.queryHandler = h
	return s
}

// WithLocalQueryMint wires the pre-built http.Handler for
// POST /api/v1/query/token (RFC 022 §5.3): the per-invocation query-JWT mint
// endpoint the laptop-side `datuplet-query` CLI calls. Unlike WithQueryService,
// the handler should be wired whenever a signer exists — the endpoint is
// registered and returns a 403 refusal when the allowClientSideQuery policy is
// off (NOT a 404), because the client expects a clear refusal rather than a
// missing route. The route is gated on a non-nil UserResolver (WithUserResolver)
// so the auth.WithUser middleware can identify the caller. When h is nil, the
// route is left unregistered.
func (s *Server) WithLocalQueryMint(h http.Handler) *Server {
	s.localQueryMintHandler = h
	return s
}

// WithSecrets wires the project-scoped secrets endpoints
// (GET/PUT/DELETE /api/v1/projects/{pid}/secrets...). k8sClient lazily
// ensures the project namespace + managed Secret exist and applies the
// per-key merge-PATCHes; clock supplies "now" for the
// "datuplet.io/updated-<key>" annotation so tests get deterministic
// timestamps instead of the handler reading time.Now() itself. When
// k8sClient is nil, the secrets routes are left unregistered.
func (s *Server) WithSecrets(k8sClient client.Client, clock func() time.Time) *Server {
	s.secretsK8s = k8sClient
	s.secretsClock = clock
	return s
}

// WithRegistry wires pipeline-api's view of the component registry. Resolve
// is threaded into ValidatePipeline on the pipeline save path (PUT
// /api/v1/projects/{pid}/pipelines/{name}); List backs the
// GET /api/v1/components catalog routes, which register only when both this
// and WithUserResolver are wired. When nil (the default), the catalog
// routes stay unregistered and pipeline saves validate with a nil
// RegistryView, matching R5's pre-registry behavior.
func (s *Server) WithRegistry(r ComponentRegistry) *Server {
	s.registry = r
	return s
}

// Handler returns the configured http.Handler for the server.
// Authenticated routes only register when a UserResolver is present —
// the /healthz endpoint remains available without one so smoke tests of
// the binary still work.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.handleHealthz)
	// /metrics is registered unconditionally — useful even in reduced
	// (DB-less) mode. The pipeline-observer serves its own metrics on
	// :8081; pipeline-api's metrics land here on the main mux.
	mux.Handle("GET /metrics", promhttp.Handler())

	// JWKS endpoint is public: no DB or auth required, just a signing key.
	// OIDC discovery doc is also public — lakekeeper polls it to discover jwks_uri.
	if s.signer != nil {
		mux.HandleFunc("GET /api/v1/auth/jwks.json", s.handleJWKS)
		if s.publicURL != "" {
			mux.HandleFunc("GET /.well-known/openid-configuration", s.handleOIDCDiscovery)
		}
	}
	// login/logout only register when the resolver supports an interactive
	// password flow — local mode has no sessions to create or destroy.
	if s.resolver != nil && s.resolver.SupportsLogin() {
		mux.HandleFunc("POST /api/v1/auth/login", s.handleLogin)
		mux.HandleFunc("POST /api/v1/auth/logout", s.handleLogout)
	}

	// `POST /api/v1/auth/token` is the password-grant equivalent of /login
	// but returns a 1h JWT for the `datuplet run --remote` flow instead of
	// setting a session cookie. Requires a signer (to mint) and a DB pool
	// (to look up the user). It is NOT gated on the resolver — the
	// email+password IS the authentication
	// here. We only register the route when both the signer and the DB
	// pool are present so the handler can mint and authenticate; absent
	// either, callers get a clear 404.
	if s.signer != nil && s.db != nil {
		mux.HandleFunc("POST /api/v1/auth/token", s.handleCLIToken)
	}

	if s.resolver != nil {
		// /me is behind WithUser middleware — needs a resolver but not a DB.
		mux.Handle("GET /api/v1/auth/me", auth.WithUser(s.resolver, http.HandlerFunc(s.handleMe)))
	}

	// Project/pipeline/run handlers gate on s.authzr.
	// Each handler does its own per-relation Check via authzr inline.
	// The middleware just resolves the user.
	if s.resolver != nil && s.authzr != nil && s.projects != nil {
		mux.Handle("GET /api/v1/projects", auth.WithUser(s.resolver, http.HandlerFunc(s.handleListProjects)))
		mux.Handle("GET /api/v1/projects/{id}", auth.WithUser(s.resolver, http.HandlerFunc(s.handleGetProject)))
	}

	if s.resolver != nil && s.authzr != nil && s.pipelines != nil {
		mux.Handle("PUT /api/v1/projects/{pid}/pipelines/{name}", auth.WithUser(s.resolver, http.HandlerFunc(s.handlePutPipeline)))
		mux.Handle("GET /api/v1/projects/{pid}/pipelines", auth.WithUser(s.resolver, http.HandlerFunc(s.handleListPipelines)))
		mux.Handle("GET /api/v1/projects/{pid}/pipelines/{name}", auth.WithUser(s.resolver, http.HandlerFunc(s.handleGetPipeline)))
		mux.Handle("DELETE /api/v1/projects/{pid}/pipelines/{name}", auth.WithUser(s.resolver, http.HandlerFunc(s.handleDeletePipeline)))
	}

	// Project-secrets handlers gate on s.secretsK8s in addition to the
	// standard resolver/authzr/projects trio — mirrors the pipeline routes'
	// gate shape above.
	if s.resolver != nil && s.authzr != nil && s.projects != nil && s.secretsK8s != nil {
		mux.Handle("GET /api/v1/projects/{pid}/secrets", auth.WithUser(s.resolver, http.HandlerFunc(s.handleListSecrets)))
		mux.Handle("PUT /api/v1/projects/{pid}/secrets/{key}", auth.WithUser(s.resolver, http.HandlerFunc(s.handlePutSecret)))
		mux.Handle("DELETE /api/v1/projects/{pid}/secrets/{key}", auth.WithUser(s.resolver, http.HandlerFunc(s.handleDeleteSecret)))
	}

	if s.resolver != nil && s.authzr != nil && s.runs != nil {
		mux.Handle("GET /api/v1/projects/{pid}/runs",
			auth.WithUser(s.resolver, http.HandlerFunc(s.handleListRuns)))
		mux.Handle("GET /api/v1/projects/{pid}/runs/{id}",
			auth.WithUser(s.resolver, http.HandlerFunc(s.handleGetRun)))
	}

	if s.resolver != nil && s.authzr != nil && s.backend != nil {
		mux.Handle("POST /api/v1/projects/{pid}/runs/{id}/cancel",
			auth.WithUser(s.resolver, http.HandlerFunc(s.handleCancelRun)))
	}
	if s.resolver != nil && s.authzr != nil && s.backend != nil && s.pipelines != nil {
		mux.Handle("POST /api/v1/projects/{pid}/pipelines/{name}/runs",
			auth.WithUser(s.resolver, http.HandlerFunc(s.handleTriggerRun)))
	}

	// Component catalog routes: any authenticated user (WithUser only — no
	// project-scoped authz check, spec §4.7's shared picker).
	if s.resolver != nil && s.registry != nil {
		mux.Handle("GET /api/v1/components", auth.WithUser(s.resolver, http.HandlerFunc(s.handleListComponents)))
		mux.Handle("GET /api/v1/components/{name}", auth.WithUser(s.resolver, http.HandlerFunc(s.handleGetComponent)))
	}

	// Query service route. When a pre-built queryHandler + resolver are wired,
	// register POST /api/v1/query behind auth.WithUser middleware.
	// The handler is constructed in main.go at config time (same pattern as
	// storage.NewForLakekeeper), so no construction can fail here.
	// Absent env → queryHandler is nil → route stays unregistered (404).
	if s.queryHandler != nil && s.resolver != nil {
		mux.Handle("POST /api/v1/query", auth.WithUser(s.resolver, s.queryHandler))
	}

	// Local query-JWT mint route (RFC 022 §5.3). Registered whenever the
	// handler + resolver are wired — the handler itself returns a 403 refusal
	// when the allowClientSideQuery policy is off (NOT a 404), so the
	// `datuplet-query` CLI gets a clear refusal. Behind auth.WithUser: the
	// caller authenticates with the api-token bearer (aud=datuplet-api).
	if s.localQueryMintHandler != nil && s.resolver != nil {
		mux.Handle("POST /api/v1/query/token", auth.WithUser(s.resolver, s.localQueryMintHandler))
	}

	// Storage routes. When a storage.Service + resolver + authzr
	// are wired, we register the four read-only /api/v1/storage endpoints
	// behind the standard auth.WithUser middleware; the handler's own
	// FGA datuplet_member check enforces per-project access. When unwired
	// (envs absent), register a catch-all 503 under /api/v1/storage/ so
	// clients get a predictable response instead of a 404.
	switch {
	case s.storage != nil && s.resolver != nil && s.authzr != nil:
		h := &storage.HTTPHandlers{
			Svc:        s.storage,
			Authorizer: s.authzr,
			Emails:     pgxEmailLookup{pool: s.db},
		}
		mux.Handle("GET /api/v1/storage/projects/{pid}/tables",
			auth.WithUser(s.resolver, http.HandlerFunc(h.ListTables)))
		mux.Handle("GET /api/v1/storage/projects/{pid}/tables/{ns}/{t}/info",
			auth.WithUser(s.resolver, http.HandlerFunc(h.TableInfo)))
		mux.Handle("GET /api/v1/storage/projects/{pid}/tables/{ns}/{t}/schema",
			auth.WithUser(s.resolver, http.HandlerFunc(h.TableSchema)))
		mux.Handle("GET /api/v1/storage/projects/{pid}/tables/{ns}/{t}/preview",
			auth.WithUser(s.resolver, http.HandlerFunc(h.Preview)))
		mux.Handle("GET /api/v1/storage/projects/{pid}/tables/{ns}/{t}/snapshots",
			auth.WithUser(s.resolver, http.HandlerFunc(h.Snapshots)))
	default:
		mux.HandleFunc("GET /api/v1/storage/", func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "storage endpoints are not configured on this pipeline-api instance", http.StatusServiceUnavailable)
		})
	}

	// Static UI. Registered with a single "GET /ui/" pattern — Go 1.22's
	// routing makes it match every /ui/<anything> subpath. The handler
	// itself decides whether to serve a real file or fall back to
	// index.html for SPA deep-links.
	if s.staticDir != "" {
		mux.Handle("GET /ui/", s.staticHandler())
	}

	return mux
}

func (s *Server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, 200, map[string]string{"status": "ok"})
}
