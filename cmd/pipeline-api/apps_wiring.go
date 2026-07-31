package main

import (
	"fmt"
	"log"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/datuplet/datuplet/pkg/pipelineapi/apps"
	"github.com/datuplet/datuplet/pkg/pipelineapi/authz"
	apihttp "github.com/datuplet/datuplet/pkg/pipelineapi/http"
	"github.com/datuplet/datuplet/pkg/pipelineapi/tokens"
)

// wireUserApps attaches the RFC 028 user-apps control plane to srv: the author
// routes (WithApps) and the app-worker-facing internal API (WithAppsInternal).
//
// It is a named function rather than three lines inline in runServeCluster
// because it is the ONE thing that makes the whole control plane reachable —
// P2 and P3 both shipped their route blocks unwired while
// apps.NewIdentityManager's methods still panicked, so without this call every
// author route 404s in production and app-worker cannot render at all.
// apps_wiring_test.go asserts both blocks register, against this exact
// function.
//
// Soft-degrade rules, matching the rest of the binary's With* seams:
//
//   - authzr == nil (authz disabled): Server.Handler()'s own nil-gate already
//     skips both blocks, so nothing here needs a second guard.
//   - signer == nil (no signing key): the author routes still work — they are
//     pure Postgres + FGA. Only Mint fails, which the internal impersonate
//     route maps to 503. Better than withholding the whole surface.
//   - service token unset: the internal routes stay unregistered (404). This
//     is a supported configuration — a cluster that has not deployed
//     app-worker.
//   - service token file present but unreadable or empty: a BOOT ERROR. A
//     misconfigured Secret must not silently degrade into "app-worker gets
//     404s forever"; and apps.LoadServiceToken rejects an empty file outright
//     so a blank Secret can never degrade into "every bearer works".
func wireUserApps(
	srv *apihttp.Server,
	pool *pgxpool.Pool,
	authzr authz.Authorizer,
	signer *tokens.Signer,
	projects apihttp.ProjectReader,
) (*apihttp.Server, error) {
	identity := apps.NewIdentityManager(authzr, signer, apihttp.NewAppsProjectLookup(projects))
	srv = srv.WithApps(apps.NewStore(pool), identity)
	if signer == nil {
		log.Printf("user apps: signer not loaded — app author routes are live but POST /internal/v1/impersonate will 503 (check SIGNING_KEY_FILE)")
	}

	token, err := apps.ServiceTokenFromEnv()
	if err != nil {
		return nil, fmt.Errorf("user apps: load internal service token from %s: %w", apps.ServiceTokenFileEnv, err)
	}
	if token == nil {
		log.Printf("user apps: internal API DISABLED (%s not set) — app-worker cannot reach /internal/v1/*", apps.ServiceTokenFileEnv)
		return srv, nil
	}
	fmt.Printf("  User apps: author routes + internal API enabled\n")
	return srv.WithAppsInternal(token), nil
}
