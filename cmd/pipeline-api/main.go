// Package main is the pipeline-api server entrypoint.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/datuplet/datuplet/pkg/pipelineapi"
	"github.com/datuplet/datuplet/pkg/pipelineapi/auth"
	"github.com/datuplet/datuplet/pkg/pipelineapi/authz"
	pipelineapidb "github.com/datuplet/datuplet/pkg/pipelineapi/db"
	apihttp "github.com/datuplet/datuplet/pkg/pipelineapi/http"
	pkg8s "github.com/datuplet/datuplet/pkg/pipelineapi/k8s"
	"github.com/datuplet/datuplet/pkg/pipelineapi/queryproxy"
	"github.com/datuplet/datuplet/pkg/pipelineapi/runbackend"
	"github.com/datuplet/datuplet/pkg/pipelineapi/storage"
	"github.com/datuplet/datuplet/pkg/pipelineapi/store"
	"github.com/datuplet/datuplet/pkg/pipelineapi/tokens"

	// Blank-import the centralised iceberg-go IO scheme registration
	// package so this binary's `gs://` factory is the Datuplet override.
	// See pkg/datupleticeio/doc.go and RFC 019 §4.5.
	_ "github.com/datuplet/datuplet/pkg/datupleticeio"
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	switch os.Args[1] {
	case "serve":
		if err := runServe(os.Args[2:]); err != nil {
			fmt.Fprintf(os.Stderr, "serve: %v\n", err)
			os.Exit(1)
		}
	case "admin":
		if err := runAdmin(os.Args[2:]); err != nil {
			fmt.Fprintf(os.Stderr, "admin: %v\n", err)
			os.Exit(1)
		}
	case "reap-once":
		if err := runReapOnce(os.Args[2:]); err != nil {
			fmt.Fprintf(os.Stderr, "reap-once: %v\n", err)
			os.Exit(1)
		}
	case "-h", "--help", "help":
		printUsage()
	default:
		fmt.Fprintf(os.Stderr, "unknown subcommand: %q\n\n", os.Args[1])
		printUsage()
		os.Exit(1)
	}
}

// runServe is the entrypoint for `pipeline-api serve`. It parses flags,
// loads config, and starts the cluster-mode server (K8s + Postgres).
func runServe(args []string) error {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	addr := fs.String("addr", "", "HTTP listen address (also PIPELINE_API_ADDR; default :8081)")
	_ = fs.Parse(args)

	cfg := pipelineapi.LoadConfig()
	if *addr != "" {
		cfg.Addr = *addr
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	return runServeCluster(ctx, cfg)
}

// runServeCluster wires Postgres + K8s and starts the HTTP server.
func runServeCluster(ctx context.Context, cfg pipelineapi.Config) error {
	fmt.Println("pipeline-api starting...")
	fmt.Printf("  Mode: cluster\n")
	fmt.Printf("  Addr: %s\n", cfg.Addr)

	var pool *pgxpool.Pool
	if cfg.DatabaseURL != "" {
		var err error
		pool, err = pipelineapidb.Open(ctx, cfg.DatabaseURL)
		if err != nil {
			return fmt.Errorf("open db: %w", err)
		}
		defer pool.Close()
		fmt.Println("  DB: connected")

		// NOTE: Migrations no longer run at startup. The chart's pre-install
		// hook Job (pipeline-api-migrate-job) runs migrations before this
		// binary starts; use `pipeline-api admin migrate` for manual runs.
	} else {
		fmt.Println("  DB: disabled (DATABASE_URL not set)")
	}

	var signer *tokens.Signer
	if cfg.SigningKeyFile != "" {
		var err error
		signer, err = tokens.LoadPrivateKeyFromPEMFile(cfg.SigningKeyFile, cfg.KeyID)
		if err != nil {
			return fmt.Errorf("load signing key: %w", err)
		}
		fmt.Printf("  Signer: loaded (kid=%s)\n", cfg.KeyID)
	} else {
		fmt.Println("  Signer: disabled (SIGNING_KEY_FILE not set)")
	}

	var k8sClient client.Client
	if cfg.InCluster || cfg.KubeconfigPath != "" {
		var err error
		k8sClient, err = pkg8s.NewClient(pkg8s.ClientOpts{
			InCluster:      cfg.InCluster,
			KubeconfigPath: cfg.KubeconfigPath,
		})
		if err != nil {
			return fmt.Errorf("k8s client: %w", err)
		}
		fmt.Println("  K8s: connected")
	} else {
		fmt.Println("  K8s: disabled (no KUBECONFIG / in-cluster hint)")
	}

	// Build the FGA Authorizer. Required by both the K8sBackend (writes
	// synthetic-run-user tuples) and the HTTP handlers (per-relation Check).
	// Self-discovers store_id + model_id via OPENFGA_STORE_NAME +
	// OPENFGA_MODEL_VERSION. Soft-degrades when OPENFGA_MODEL_VERSION is
	// unset — handler routes that need authz stay unregistered. The
	// authz-bootstrap pre-install hook guarantees the store + pin tuple
	// exist before this binary starts.
	var authzr authz.Authorizer
	{
		openfgaURL := os.Getenv("OPENFGA_URL")
		if openfgaURL == "" {
			openfgaURL = "http://openfga.openfga.svc.cluster.local:8080"
		}
		openfgaAPIKey := os.Getenv("OPENFGA_API_KEY")
		storeName := os.Getenv("OPENFGA_STORE_NAME")
		if storeName == "" {
			storeName = "datuplet" // default; chart sets it explicitly via platform.openfgaStoreName
		}
		modelVersion := os.Getenv("OPENFGA_MODEL_VERSION")

		if modelVersion != "" {
			// authz-bootstrap pre-install hook guarantees the FGA store + pin
			// tuple are present. This call fails fast if the pin is missing —
			// kubelet restarts the pod with backoff. No retry loop.
			storeID, modelID, err := authz.ResolveStoreAndModel(ctx, openfgaURL, openfgaAPIKey, storeName, modelVersion)
			if err != nil {
				return fmt.Errorf("FGA model pin not found -- did authz-bootstrap run?: %w", err)
			}
			a, err := authz.NewOpenFGAAuthorizer(openfgaURL, storeID, modelID, openfgaAPIKey, 5*time.Second)
			if err != nil {
				return fmt.Errorf("create OpenFGA authorizer: %w", err)
			}
			authzr = a
			fmt.Printf("  Authz: openfga (%s, store=%s name=%s, model=%s version=%s)\n", openfgaURL, storeID, storeName, modelID, modelVersion)
		} else {
			log.Printf("authz disabled: OPENFGA_MODEL_VERSION not set (project/pipeline/run handlers stay unregistered)")
		}
	}

	// Lakekeeper URL is used by both the trigger warehouse resolver and the
	// storage service. Resolve it once here so both blocks share the same value.
	lakekeeperURL := os.Getenv("DATUPLET_LAKEKEEPER_URL")

	// K8s run lifecycle backend: the HTTP handler becomes a thin adapter
	// that calls this for trigger + cancel. Only constructed when we
	// have a k8s client, signer, and DB pool.
	var backend runbackend.Backend
	if k8sClient != nil && signer != nil && pool != nil {
		// Warehouse resolver for the trigger path. Uses the triggering user's
		// impersonation token (same one the storage UI uses) — the handler
		// enforces `data_admin` before this code runs, which transitively
		// grants lakekeeper's `can_list_warehouses`. nil when
		// DATUPLET_LAKEKEEPER_URL is unset — K8sBackend soft-degrades to an
		// empty warehouse claim in that case.
		var triggerWarehouseResolver func(ctx context.Context, lakekeeperProjectID string) (string, error)
		if lakekeeperURL != "" {
			triggerWarehouseResolver = storage.NewLakekeeperWarehouseResolver(storage.WarehouseResolverConfig{
				LakekeeperURL: lakekeeperURL,
				Minter: func(ctx context.Context) (tokens.ImpersonationToken, error) {
					return tokens.MintImpersonation(ctx, signer)
				},
			})
		}
		backend = runbackend.NewK8sBackend(runbackend.K8sOpts{
			Client:            k8sClient,
			RunInserter:       runbackend.NewStoreInserter(pool),
			ProjectNS:         runbackend.NewStoreProjectNS(pool, k8sClient),
			Minter:            runbackend.NewTokenMinter(signer),
			Audience:          cfg.Audience,
			DB:                pool,
			Authorizer:        authzr,
			WarehouseResolver: triggerWarehouseResolver,
		})
	}

	// Storage service: pipeline-api holds no long-lived S3 credentials.
	// Each storage request mints a short-lived impersonation JWT, calls
	// lakekeeper LoadTable, and reads parquet via the iceberg-go REST-
	// catalog table whose FS closure carries lakekeeper-vended STS creds
	// scoped to the table's S3 prefix. No S3_ACCESS_KEY / S3_SECRET_KEY
	// env vars are read. Warehouse name is resolved per-request via
	// lakekeeper — the multi-warehouse selector is deferred.
	//
	// NewForLakekeeper returns (nil, nil) when DATUPLET_LAKEKEEPER_URL is
	// empty — that's the soft-degrade signal; the server registers a
	// catch-all 503 on /api/v1/storage/.
	storageSvc, err := storage.NewForLakekeeper(lakekeeperURL)
	if err != nil {
		return fmt.Errorf("storage service: %w", err)
	}

	// warehouseResolver is used by the storage service only (per-request warehouse
	// resolution for the storage UI). The query service uses the static
	// DATUPLET_QUERY_WAREHOUSE env var set at boot — not this resolver.
	var warehouseResolver func(ctx context.Context, lakekeeperProjectID string) (string, error)

	if storageSvc != nil {
		if signer == nil {
			log.Printf("storage proxy: signer not loaded — impersonation JWT minting unavailable; storage handlers will return 500 on every catalog call (check SIGNING_KEY_FILE)")
		} else {
			// Impersonation minter — mints a 60s JWT for the authenticated user
			// in ctx. Lakekeeper validates it, checks FGA grants, returns table
			// metadata + vended STS creds. Per-request; never cached across
			// requests (STS creds have their own TTL).
			impersonationMinter := func(ctx context.Context) (tokens.ImpersonationToken, error) {
				return tokens.MintImpersonation(ctx, signer)
			}
			// Pgx-backed resolver: maps Datuplet project UUID → lakekeeper Project UUID.
			// pool==nil here would mean DATABASE_URL was unset in cluster mode,
			// which runServeCluster hard-requires elsewhere — but failing
			// closed here too is cheap insurance: without the resolver,
			// catalog calls drop the x-project-id header and lakekeeper
			// would route every user to its default project, silently
			// bypassing per-project FGA grants. Refuse to wire the storage
			// proxy in that state so admins notice immediately.
			if pool == nil {
				return fmt.Errorf("storage proxy: cluster mode requires a Postgres pool to resolve lakekeeper_project_id; pool is nil — check DATABASE_URL")
			}
			pgxProjectIDFor := func(ctx context.Context, datupletProjectID uuid.UUID) (string, error) {
				p, err := store.GetProjectByID(ctx, pool, datupletProjectID)
				if err != nil {
					return "", err
				}
				return p.LakekeeperProjectID, nil
			}
			// Per-request warehouse resolution from lakekeeper. The resolver
			// picks the first warehouse the user has FGA visibility into for
			// the requested project; multi-warehouse selector is deferred.
			warehouseResolver = storage.NewLakekeeperWarehouseResolver(storage.WarehouseResolverConfig{
				LakekeeperURL: lakekeeperURL,
				Minter:        impersonationMinter,
			})
			storageSvc.WithLakekeeper(lakekeeperURL, impersonationMinter, pgxProjectIDFor, warehouseResolver)
		}
		fmt.Printf("  Storage catalog: %s (warehouse: per-request)\n", lakekeeperURL)
	} else {
		log.Printf("storage proxy not wired: DATUPLET_LAKEKEEPER_URL=%q (storage handlers return 503 — set the env var in your deployment manifest)",
			lakekeeperURL)
	}

	// Query service: POST /api/v1/query ad-hoc SQL queries on lakekeeper tables.
	// Routes to the query-worker Service with JWT credentials. Soft-degrade when
	// DATUPLET_QUERY_WORKER_URL is absent — the endpoint stays unregistered
	// (clients get 404).
	//
	// Warehouse: the query-worker must attach to a specific lakekeeper warehouse
	// to resolve table metadata. The warehouse must be project-qualified
	// (format: "lakekeeper_project_id/warehouse_name") as lakekeeper requires for
	// attach (RFC 022 §6.1). Unlike storage handlers which resolve the warehouse
	// per-request, the query service uses a single static warehouse configured at
	// boot via DATUPLET_QUERY_WAREHOUSE. This design supports single-warehouse
	// deployments; multi-warehouse support is deferred (would require per-request
	// warehouse selection based on run context).
	//
	// The handler is built here at config time (same pattern as
	// storage.NewForLakekeeper) so that a present-but-invalid WorkerURL
	// hard-fails runServeCluster with a normal error rather than panicking at
	// request time. Absent env → soft-degrade → route unregistered → 404.
	var queryHandler http.Handler
	queryWorkerURL := os.Getenv("DATUPLET_QUERY_WORKER_URL")
	if queryWorkerURL != "" {
		queryWarehouse := os.Getenv("DATUPLET_QUERY_WAREHOUSE")
		if queryWarehouse == "" {
			log.Printf("query service: DATUPLET_QUERY_WAREHOUSE not set (POST /api/v1/query unavailable); set to enable (format: projectID/warehouse)")
		} else if lakekeeperURL == "" {
			log.Printf("query service: DATUPLET_LAKEKEEPER_URL not set (POST /api/v1/query unavailable); required for table catalog access")
		} else {
			queryProxyCfg := queryproxy.Config{
				WorkerURL:            queryWorkerURL,
				Warehouse:            queryWarehouse,
				DefaultTimeoutS:      envIntOr("DATUPLET_QUERY_DEFAULT_TIMEOUT_S", 0),      // 0 → use package default (60s)
				MaxTimeoutS:          envIntOr("DATUPLET_QUERY_MAX_TIMEOUT_S", 0),          // 0 → use package default (300s)
				DefaultMaxRows:       envIntOr("DATUPLET_QUERY_DEFAULT_MAX_ROWS", 0),       // 0 → use package default (1000)
				MaxMaxRows:           envIntOr("DATUPLET_QUERY_MAX_MAX_ROWS", 0),           // 0 → use package default (10000)
				DefaultMaxBytes:      envIntOr("DATUPLET_QUERY_DEFAULT_MAX_BYTES", 0),      // 0 → use package default (1 MiB)
				MaxMaxBytes:          envIntOr("DATUPLET_QUERY_MAX_MAX_BYTES", 0),          // 0 → use package default (10 MiB)
				PerPrincipalInflight: envIntOr("DATUPLET_QUERY_PER_PRINCIPAL_INFLIGHT", 0), // 0 → use package default (2)
			}
			// Build the handler now so an invalid WorkerURL (e.g. unparseable URL)
			// surfaces as a runServeCluster error rather than a panic on first request.
			h, err := queryproxy.Handler(queryProxyCfg, signer)
			if err != nil {
				return fmt.Errorf("query service: %w", err)
			}
			queryHandler = h
			fmt.Printf("  Query service: %s (warehouse: %s)\n", queryWorkerURL, queryWarehouse)
		}
	} else {
		log.Printf("query service not wired: DATUPLET_QUERY_WORKER_URL not set (POST /api/v1/query returns 404)")
	}

	srv := apihttp.NewServer(pool).
		WithCookieSecure(cfg.CookieSecure).
		WithSigner(signer).
		WithK8sClient(k8sClient).
		WithPublicURL(cfg.PublicURL).
		WithStaticDir(cfg.UIDir).
		WithRunBackend(backend).
		WithStorage(storageSvc).
		WithAuthorizer(authzr).
		// Cluster info is embedded in POST /api/v1/auth/token responses so
		// `datuplet run --remote` can reach lakekeeper. The lakekeeper URL
		// is the deployment-time public URL (distinct from
		// PIPELINE_API_PUBLIC_URL). Warehouse name is not a server-side env
		// var; it is resolved per-request via lakekeeper.
		WithCLIClusterInfo(cfg.LakekeeperPublicURL, "")
	// Query service: POST /api/v1/query ad-hoc SQL. Wired only when
	// DATUPLET_QUERY_WORKER_URL is set and handler construction succeeded above.
	// nil → route stays unregistered → 404.
	if queryHandler != nil {
		srv = srv.WithQueryService(queryHandler)
	}
	// Cluster-mode auth: Postgres-backed resolver (session cookies) chained
	// with a bearer-JWT resolver so CLI subcommands (trigger, storage) can
	// authenticate via Authorization: Bearer <api_token> minted by
	// POST /api/v1/auth/token. The ChainResolver tries the bearer path
	// first, then falls through to the cookie path — browser sessions are
	// unaffected.
	if pool != nil {
		cookieResolver := auth.NewPostgresResolver(pool, cfg.CookieSecure)
		var chainedResolver auth.UserResolver
		if signer != nil {
			chainedResolver = &auth.ChainResolver{
				Resolvers: []auth.UserResolver{
					&auth.BearerJWTResolver{
						PublicKey: signer.Public(),
						KeyID:     signer.KeyID,
						Pool:      auth.NewPgxUserLookup(pool),
					},
					cookieResolver,
				},
				// cookieResolver owns Mode()/SupportsLogin() so the login route
				// and /auth/me mode reporting reflect deployment mode (cluster),
				// not the bearer side-channel.
				Primary: cookieResolver,
			}
		} else {
			chainedResolver = cookieResolver
		}
		srv = srv.
			WithUserResolver(chainedResolver).
			WithProjectReader(apihttp.NewPgxProjectReader(pool, authzr)).
			WithPipelineStore(apihttp.NewPgxPipelineStore(pool)).
			WithRunReader(apihttp.NewPgxRunReader(pool))
	}
	if cfg.UIDir != "" {
		fmt.Printf("  UI: %s\n", cfg.UIDir)
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	httpSrv := &http.Server{
		Addr:              cfg.Addr,
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}

	// Observer is a separate Deployment (`cmd/pipeline-observer`) that
	// owns the informer cache + DB-writer lifecycle. Pipeline-api is
	// HTTP-only: trigger + cancel paths use k8sClient directly without an
	// informer; pipeline-observer mirrors PipelineRun status into Postgres
	// independently. Reaper runs as its own CronJob (`pipeline-api reap-once`).

	go func() {
		<-sigCh
		fmt.Println("\nReceived shutdown signal...")
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer shutdownCancel()
		_ = httpSrv.Shutdown(shutdownCtx)
	}()

	if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("listen: %w", err)
	}
	return nil
}

func printUsage() {
	fmt.Print(`pipeline-api — Datuplet central API

Usage:
  pipeline-api serve [--addr :8081]          Run the HTTP server (cluster mode)
  pipeline-api admin <command>               Admin operations
  pipeline-api reap-once [--max-age 24h]     Run the reaper once, then exit
                                             (used by the CronJob — see
                                             utils/deploy/k8s/pipeline-api-reaper.yaml)

Env:
  PIPELINE_API_ADDR           HTTP listen address (default :8081)
  DATABASE_URL                Postgres DSN
  PIPELINE_API_COOKIE_SECURE  Require HTTPS for session cookie (default false)
  REAPER_MAX_AGE              Default --max-age for reap-once (default 24h)
`)
}

// dbRunUpdater moved to pkg/pipelineapi/k8s/db_updater.go as
// pkg8s.NewDBRunUpdater so both `serve` and `reap-once` share the
// identity-validation + rv-plumbing logic.
//
// Note: the former `lookupOpenFGAStoreAndModel` helper (latest-model lookup)
// was removed — pipeline-api now uses `pkg/pipelineapi/authz.ResolveStoreAndModel`
// which queries the version-pin tuple at startup instead of picking the latest model.

// envIntOr parses the env var as an int, returning def if absent or malformed.
func envIntOr(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			return i
		}
	}
	return def
}
