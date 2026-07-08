package main

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/datuplet/datuplet/pkg/pipelineapi"
	"github.com/datuplet/datuplet/pkg/pipelineapi/auth"
	"github.com/datuplet/datuplet/pkg/pipelineapi/authz"
	pipelineapidb "github.com/datuplet/datuplet/pkg/pipelineapi/db"
	pkg8s "github.com/datuplet/datuplet/pkg/pipelineapi/k8s"
	"github.com/datuplet/datuplet/pkg/pipelineapi/lakekeeper"
	"github.com/datuplet/datuplet/pkg/pipelineapi/store"
	"github.com/datuplet/datuplet/pkg/pipelineapi/tokens"
)

func runAdmin(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("admin requires a subcommand (attach-warehouse | component | create-user | create-project | delete-project | ensure-project-authz | grant | keygen | lakekeeper-bootstrap | migrate)")
	}

	// Validate the subcommand BEFORE opening the DB or running migrations —
	// a typo shouldn't cause schema changes or a misleading config error.
	switch args[0] {
	case "attach-warehouse", "component", "create-user", "create-project", "delete-project", "ensure-project-authz",
		"grant", "keygen", "lakekeeper-bootstrap", "migrate":
		// ok
	default:
		return fmt.Errorf("unknown admin subcommand: %q (valid: attach-warehouse | component | create-user | create-project | delete-project | ensure-project-authz | grant | keygen | lakekeeper-bootstrap | migrate)", args[0])
	}

	// keygen + lakekeeper-bootstrap + component are DB-free —
	// skip DB open+migrate entirely.
	if args[0] == "keygen" {
		return adminKeygen(args[1:])
	}
	if args[0] == "lakekeeper-bootstrap" {
		return adminLakekeeperBootstrap(args[1:])
	}
	if args[0] == "component" {
		return runAdminComponent(args[1:])
	}
	// migrate manages its own DB connection — it IS the migration step.
	if args[0] == "migrate" {
		return adminMigrate(context.Background(), args[1:])
	}

	ctx := context.Background()
	cfg := pipelineapi.LoadConfig()
	if cfg.DatabaseURL == "" {
		return fmt.Errorf("DATABASE_URL is required for admin operations")
	}

	pool, err := pipelineapidb.Open(ctx, cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("open db: %w", err)
	}
	defer pool.Close()
	if err := pipelineapidb.Migrate(ctx, pool); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}

	switch args[0] {
	case "attach-warehouse":
		return adminAttachWarehouse(ctx, pool, args[1:])
	case "create-user":
		return adminCreateUser(ctx, pool, args[1:])
	case "create-project":
		return adminCreateProject(ctx, pool, args[1:])
	case "delete-project":
		return adminDeleteProject(ctx, pool, args[1:])
	case "ensure-project-authz":
		return adminEnsureProjectAuthz(ctx, pool, args[1:])
	case "grant":
		return adminGrant(ctx, pool, args[1:])
	}
	return nil // unreachable — subcommand validated above
}

func adminCreateUser(ctx context.Context, pool *pgxpool.Pool, args []string) error {
	fs := flag.NewFlagSet("create-user", flag.ExitOnError)
	email := fs.String("email", "", "User email (required)")
	password := fs.String("password", "", "User password (required; stored as argon2id)")
	_ = fs.Parse(args)
	if *email == "" || *password == "" {
		return fmt.Errorf("--email and --password are required")
	}

	hash, err := auth.HashPassword(*password)
	if err != nil {
		return fmt.Errorf("hash: %w", err)
	}
	u, err := store.CreateUser(ctx, pool, *email, hash)
	if err != nil {
		// Idempotent: re-runs from register.sh (or operators) shouldn't
		// fail if the user is already there. Matches the existing pattern
		// for `admin lakekeeper-bootstrap` ("bootstrap: already done") and
		// warehouse creation.
		if errors.Is(err, store.ErrUserAlreadyExists) {
			fmt.Printf("User %s already exists — skipping.\n", *email)
			return nil
		}
		return err
	}
	fmt.Printf("Created user %s (id=%s)\n", u.Email, u.ID)
	return nil
}

// projectProvisioningEnv bundles the connections + minters needed by the
// project-create / delete / ensure flows. The lakekeeper bridge is wired
// in three places (create, delete, reconcile); this struct keeps flag
// parsing + dial steps in one place so each subcommand stays focused on
// its own logic.
type projectProvisioningEnv struct {
	creatorUserID string // OIDC sub for the project_admin tuple
	lkManager     *lakekeeper.Manager
	authorizer    authz.Authorizer
}

// dialProjectProvisioning resolves CLI flags + env into a live
// LakekeeperManager + OpenFGA Authorizer + creator subject. Shared
// between the three project-lifecycle subcommands so the flag set is
// consistent and the connection cost is paid once per invocation.
//
// Creator identity: pass either --creator-user-id <uuid> directly, or
// --creator-email <email> for an email→UUID lookup against the users
// table. The email path requires `pool` to be non-nil; if both flags
// are set, --creator-user-id wins (explicit overrides convenience).
func dialProjectProvisioning(ctx context.Context, pool *pgxpool.Pool, fs *flag.FlagSet, args []string) (*projectProvisioningEnv, error) {
	creatorUserID := fs.String("creator-user-id", "",
		"OIDC sub (UUID) of the user who holds project_admin on the lakekeeper Project (one of --creator-user-id or --creator-email is required for create / delete / ensure)")
	creatorEmail := fs.String("creator-email", "",
		"User email — looked up against the users table to derive --creator-user-id (alternative to --creator-user-id)")
	lkURL := fs.String("lakekeeper-url", "http://lakekeeper.lakekeeper.svc.cluster.local:8181",
		"Lakekeeper management base URL")
	openfgaURL := fs.String("openfga-url", "http://openfga.openfga.svc.cluster.local:8080",
		"OpenFGA HTTP base URL (the apiURL passed to authz.NewOpenFGAAuthorizer)")
	storeID := fs.String("openfga-store-id", os.Getenv("OPENFGA_STORE_ID"),
		"OpenFGA store ULID (default from OPENFGA_STORE_ID; if unset, resolved from OPENFGA_STORE_NAME)")
	modelID := fs.String("openfga-model-id", os.Getenv("OPENFGA_MODEL_ID"),
		"OpenFGA authorization model ULID (default from OPENFGA_MODEL_ID; if unset, resolved from OPENFGA_MODEL_VERSION pin tuple)")
	storeName := fs.String("openfga-store-name", os.Getenv("OPENFGA_STORE_NAME"),
		"OpenFGA store name to resolve store ID by (default from OPENFGA_STORE_NAME; e.g. 'datuplet')")
	modelVersion := fs.String("openfga-model-version", os.Getenv("OPENFGA_MODEL_VERSION"),
		"OpenFGA model version to resolve model ID by reading the pin tuple (default from OPENFGA_MODEL_VERSION)")
	apiKey := fs.String("openfga-api-key", os.Getenv("OPENFGA_API_KEY"),
		"OpenFGA preshared API key for Bearer auth (default from OPENFGA_API_KEY)")
	keyFile := fs.String("signing-key-file", os.Getenv("SIGNING_KEY_FILE"),
		"Path to the RS256 PEM private key (default from SIGNING_KEY_FILE)")
	keyID := fs.String("key-id", os.Getenv("SIGNING_KEY_ID"),
		"JWK kid (default from SIGNING_KEY_ID, then 'key-1')")
	audience := fs.String("audience", tokens.TableTokenAudience,
		"JWT aud claim (must match LAKEKEEPER__OPENID_AUDIENCE)")
	_ = fs.Parse(args)

	if *keyFile == "" {
		return nil, fmt.Errorf("--signing-key-file is required (or set SIGNING_KEY_FILE)")
	}

	// Self-discovery fallback: if explicit IDs are missing but name+version
	// are set, resolve them via FGA REST (mirrors runServe in main.go).
	// Lets register.sh + ad-hoc admin invocations work with just
	// OPENFGA_STORE_NAME + OPENFGA_MODEL_VERSION envs from the Pod.
	if (*storeID == "" || *modelID == "") && *storeName != "" && *modelVersion != "" {
		sID, mID, err := authz.ResolveStoreAndModel(ctx, *openfgaURL, *apiKey, *storeName, *modelVersion)
		if err != nil {
			return nil, fmt.Errorf("resolve FGA store/model: %w", err)
		}
		if *storeID == "" {
			*storeID = sID
		}
		if *modelID == "" {
			*modelID = mID
		}
	}

	if *storeID == "" || *modelID == "" {
		return nil, fmt.Errorf("--openfga-store-id and --openfga-model-id are required (or set OPENFGA_STORE_ID / OPENFGA_MODEL_ID, or OPENFGA_STORE_NAME + OPENFGA_MODEL_VERSION for self-discovery)")
	}
	if *keyID == "" {
		*keyID = "key-1"
	}

	// Resolve --creator-email → user UUID. --creator-user-id wins when
	// both are set so an explicit override always beats the convenience
	// path. Lookup requires pool — callers that don't have one (none
	// today) must pass --creator-user-id directly.
	if *creatorUserID == "" && *creatorEmail != "" {
		if pool == nil {
			return nil, fmt.Errorf("--creator-email requires a database connection; pass --creator-user-id instead")
		}
		u, err := store.GetUserByEmail(ctx, pool, *creatorEmail)
		if err != nil {
			return nil, fmt.Errorf("--creator-email %q: %w", *creatorEmail, err)
		}
		*creatorUserID = u.ID.String()
	}

	signer, err := tokens.LoadPrivateKeyFromPEMFile(*keyFile, *keyID)
	if err != nil {
		return nil, fmt.Errorf("load signing key: %w", err)
	}
	// Subject is "admin" so the token carries the `oidc~admin` →
	// project_admin tuple the bootstrap flow writes on the default
	// lakekeeper Project. Without it lakekeeper rejects every management
	// call with 403.
	minter := func() (string, error) {
		return tokens.MintServiceToken(signer, tokens.ServiceTokenSpec{
			Subject:  "admin",
			Audience: *audience,
			Lifetime: 5 * time.Minute,
		})
	}
	mgr, err := lakekeeper.New(*lkURL, minter, 30*time.Second)
	if err != nil {
		return nil, fmt.Errorf("lakekeeper manager: %w", err)
	}
	authorizer, err := authz.NewOpenFGAAuthorizer(*openfgaURL, *storeID, *modelID, os.Getenv("OPENFGA_API_KEY"), 5*time.Second)
	if err != nil {
		return nil, fmt.Errorf("openfga authorizer: %w", err)
	}
	return &projectProvisioningEnv{
		creatorUserID: *creatorUserID,
		lkManager:     mgr,
		authorizer:    authorizer,
	}, nil
}

// adminCreateProject creates a Datuplet project, allocates a matching
// lakekeeper Project (POST /management/v1/project), grants the creator
// project_admin via FGA, and persists the lakekeeper project-id back
// onto the projects row. Compensating-action ordering on failure:
//
//	postgres INSERT  ─┬─ ok ──> lakekeeper POST  ─┬─ ok ──> FGA WRITE  ─┬─ ok ──> UPDATE row
//	                  fail               fail                  fail              fail
//	                  return        delete pg row,        delete lk proj,  delete lk proj,
//	                                  return                pg row,           pg row, FGA del,
//	                                                        return            return
//
// Fail-loud on any rollback failure: half-state leaks are louder than a
// noisy error.
func adminCreateProject(ctx context.Context, pool *pgxpool.Pool, args []string) error {
	fs := flag.NewFlagSet("create-project", flag.ExitOnError)
	name := fs.String("name", "", "Project display name (required; must be unique)")
	withNamespace := fs.Bool("with-namespace", false, "Also create the K8s Namespace (datuplet-<uuid>) with the project-id label")
	kubeconfig := fs.String("kubeconfig", "", "Path to kubeconfig (used with --with-namespace)")
	env, err := dialProjectProvisioning(ctx, pool, fs, args)
	if err != nil {
		return err
	}
	if *name == "" {
		return fmt.Errorf("--name is required")
	}
	if env.creatorUserID == "" {
		return fmt.Errorf("--creator-user-id or --creator-email is required (the user receiving project_admin)")
	}

	// Step 1: Postgres INSERT. Idempotent re-run: if the project already
	// exists, look it up and reuse — subsequent steps (lakekeeper probe,
	// FGA write, lakekeeper-id UPDATE) are already probe-then-set and
	// tolerate previous-state.
	p, err := store.CreateProject(ctx, pool, *name)
	if err != nil {
		if errors.Is(err, store.ErrProjectAlreadyExists) {
			p, err = store.GetProjectByName(ctx, pool, *name)
			if err != nil {
				return fmt.Errorf("project %q already exists but lookup failed: %w", *name, err)
			}
			fmt.Printf("Project %q already exists — running subsequent steps for idempotency.\n", p.Name)
			fmt.Printf("  id:          %s\n", p.ID)
			fmt.Printf("  namespace:   %s\n", p.K8sNamespace)
		} else {
			return err
		}
	} else {
		fmt.Printf("Created project %q\n", p.Name)
		fmt.Printf("  id:          %s\n", p.ID)
		fmt.Printf("  namespace:   %s\n", p.K8sNamespace)
	}

	// rollback compensates a partial create. Each step calls rollback()
	// before returning the error so Postgres + lakekeeper + FGA stay
	// in sync after the call returns.
	rollbackPostgres := func(reason error) error {
		if derr := store.DeleteProject(ctx, pool, p.ID); derr != nil {
			return fmt.Errorf("%w (ALSO: rollback of project row failed: %v — clean up manually by DELETE FROM projects WHERE id = '%s')", reason, derr, p.ID)
		}
		return fmt.Errorf("%w (project row rolled back; safe to retry)", reason)
	}

	// Step 2: lakekeeper Project. Probe-first so a re-run after a partial
	// failure (where the postgres row was rolled back but the lakekeeper
	// project survived under the same name) doesn't double-create.
	lakekeeperID, err := env.lkManager.FindProjectIDByName(ctx, *name)
	if err != nil {
		return rollbackPostgres(fmt.Errorf("find lakekeeper project: %w", err))
	}
	if lakekeeperID == "" {
		lakekeeperID, err = env.lkManager.CreateProject(ctx, *name)
		if err != nil {
			return rollbackPostgres(fmt.Errorf("create lakekeeper project: %w", err))
		}
		fmt.Printf("  lakekeeper:  Project %s created\n", lakekeeperID)
	} else {
		fmt.Printf("  lakekeeper:  Project %s reused (already existed)\n", lakekeeperID)
	}

	rollbackLakekeeper := func(reason error) error {
		if derr := env.lkManager.DeleteProject(ctx, lakekeeperID); derr != nil {
			return fmt.Errorf("%w (ALSO: rollback of lakekeeper Project %s failed: %v — clean up manually)", reason, lakekeeperID, derr)
		}
		return rollbackPostgres(reason)
	}

	// Step 3: FGA tuple. project_admin is the lock-out-protected
	// lakekeeper builtin.
	if err := authz.WriteAdminTuple(ctx, env.authorizer, env.creatorUserID, lakekeeperID); err != nil {
		return rollbackLakekeeper(fmt.Errorf("write FGA project_admin tuple: %w", err))
	}
	fmt.Printf("  FGA:         user:oidc~%s project_admin project:%s\n", env.creatorUserID, lakekeeperID)

	rollbackFGA := func(reason error) error {
		// Best-effort delete of the tuple we just wrote. Failures here
		// would leave an orphan tuple but no project to attach it to —
		// the reconciler can clean those up on the next pass.
		if derr := authz.DeleteProjectTuples(ctx, env.authorizer, lakekeeperID, []string{env.creatorUserID}); derr != nil {
			return fmt.Errorf("%w (ALSO: rollback of FGA tuple failed: %v — orphan tuple for project:%s left behind)", reason, derr, lakekeeperID)
		}
		return rollbackLakekeeper(reason)
	}

	// Step 4: persist the lakekeeper id back onto the row.
	if err := store.SetLakekeeperProjectID(ctx, pool, p.ID, lakekeeperID); err != nil {
		return rollbackFGA(fmt.Errorf("UPDATE projects.lakekeeper_project_id: %w", err))
	}

	// Step 5 (optional): k8s Namespace. Failures here also roll back
	// everything — the namespace is a hard prerequisite for runs.
	if *withNamespace {
		if *kubeconfig == "" {
			*kubeconfig = os.Getenv("KUBECONFIG")
		}
		c, err := pkg8s.NewClient(pkg8s.ClientOpts{KubeconfigPath: *kubeconfig})
		if err != nil {
			return rollbackFGA(fmt.Errorf("k8s client: %w", err))
		}
		if err := pkg8s.EnsureProjectNamespace(ctx, c, p.ID); err != nil {
			return rollbackFGA(fmt.Errorf("ensure namespace: %w", err))
		}
		fmt.Printf("  k8s:         Namespace %s created\n", p.K8sNamespace)
	} else {
		fmt.Printf("  (rerun with --with-namespace to create the K8s Namespace object)\n")
	}
	return nil
}

// adminDeleteProject tears down a project's lakekeeper Project + FGA
// tuples + projects row in reverse-create order. Idempotent on partial
// failures: re-running after each failure is safe because every step
// tolerates "already gone" (DeleteProjectTuples skips missing tuples;
// LakekeeperManager.DeleteProject swallows 404s; store.DeleteProject
// returns ErrProjectNotFound which we surface but accept).
func adminDeleteProject(ctx context.Context, pool *pgxpool.Pool, args []string) error {
	fs := flag.NewFlagSet("delete-project", flag.ExitOnError)
	idStr := fs.String("id", "", "Datuplet project UUID (one of --id or --name is required)")
	name := fs.String("name", "", "Datuplet project display name (one of --id or --name is required)")
	env, err := dialProjectProvisioning(ctx, pool, fs, args)
	if err != nil {
		return err
	}
	if *idStr == "" && *name == "" {
		return fmt.Errorf("one of --id or --name is required")
	}
	if env.creatorUserID == "" {
		fmt.Fprintln(os.Stderr, "delete-project requires --creator-user-id <sub> or --creator-email <email> (the user who originally created the project — used to delete the project_admin FGA tuple). Look it up via 'pipeline-api admin list-projects' or in the auth_events log if available.")
		return fmt.Errorf("--creator-user-id or --creator-email is required")
	}

	var proj *store.Project
	if *idStr != "" {
		id, err := uuid.Parse(*idStr)
		if err != nil {
			return fmt.Errorf("parse --id: %w", err)
		}
		proj, err = store.GetProjectByID(ctx, pool, id)
		if err != nil {
			return fmt.Errorf("lookup project: %w", err)
		}
	} else {
		proj, err = store.GetProjectByName(ctx, pool, *name)
		if err != nil {
			return fmt.Errorf("lookup project: %w", err)
		}
	}
	fmt.Printf("Deleting project %q (id=%s, lakekeeper_project_id=%q)\n", proj.Name, proj.ID, proj.LakekeeperProjectID)

	// Step 1: FGA tuples. Delete BEFORE the lakekeeper Project so a
	// dangling tuple doesn't grant access to a project that's about to
	// die — and so re-running the delete after a lakekeeper failure
	// just retries lakekeeper + Postgres without re-attempting FGA.
	// --creator-user-id is required (enforced above), so the only skip
	// case is when no lakekeeper_project_id was ever persisted.
	if proj.LakekeeperProjectID != "" {
		if err := authz.DeleteProjectTuples(ctx, env.authorizer, proj.LakekeeperProjectID, []string{env.creatorUserID}); err != nil {
			return fmt.Errorf("delete FGA tuples: %w", err)
		}
		fmt.Printf("  FGA:         tuples deleted for project:%s\n", proj.LakekeeperProjectID)
	} else {
		fmt.Printf("  FGA:         skipped (no lakekeeper_project_id on the row)\n")
	}

	// Step 2: lakekeeper Project. Idempotent (404 = success).
	if proj.LakekeeperProjectID != "" {
		if err := env.lkManager.DeleteProject(ctx, proj.LakekeeperProjectID); err != nil {
			return fmt.Errorf("delete lakekeeper project: %w", err)
		}
		fmt.Printf("  lakekeeper:  Project %s deleted\n", proj.LakekeeperProjectID)
	}

	// Step 3: Postgres row. Last so a failure here leaves Postgres as
	// the only place still holding the project — easy to spot and
	// retry.
	if err := store.DeleteProject(ctx, pool, proj.ID); err != nil {
		return fmt.Errorf("delete project row: %w", err)
	}
	fmt.Printf("  postgres:    row deleted\n")
	return nil
}

// adminEnsureProjectAuthz sweeps every projects row and brings its
// lakekeeper Project + FGA tuples back into sync. Useful for first-boot
// recovery, manual repair, and as a smoke-test idempotency check.
//
// Strategy per row:
//  1. If lakekeeper_project_id is empty, allocate a fresh lakekeeper
//     Project (or reuse one named after the Datuplet project, if it
//     already exists from a half-completed create).
//  2. Verify lakekeeper still knows about that Project. If not, allocate
//     a new one.
//  3. Verify the creator has project_admin via FGA Check. If not, write
//     the tuple.
//  4. Persist any new lakekeeper_project_id back to the row.
//
// --creator-user-id is required because we need to know whose
// project_admin tuple to (re-)write. In a multi-user world this would
// iterate over a creator/owner column; currently one admin grant
// per project is persisted.
func adminEnsureProjectAuthz(ctx context.Context, pool *pgxpool.Pool, args []string) error {
	fs := flag.NewFlagSet("ensure-project-authz", flag.ExitOnError)
	env, err := dialProjectProvisioning(ctx, pool, fs, args)
	if err != nil {
		return err
	}
	if env.creatorUserID == "" {
		return fmt.Errorf("--creator-user-id or --creator-email is required")
	}

	projects, err := store.ListAllProjects(ctx, pool)
	if err != nil {
		return fmt.Errorf("list projects: %w", err)
	}
	fmt.Printf("Reconciling %d project(s)\n", len(projects))

	for _, p := range projects {
		fmt.Printf("- %s (id=%s)\n", p.Name, p.ID)

		// Step 1+2: lakekeeper Project. Probe by name first (handles
		// both "row missing the id" and "id stored but lakekeeper lost
		// the project").
		lakekeeperID := p.LakekeeperProjectID
		if lakekeeperID != "" {
			exists, perr := env.lkManager.ProjectExists(ctx, lakekeeperID)
			if perr != nil {
				return fmt.Errorf("probe lakekeeper project for %q: %w", p.Name, perr)
			}
			if !exists {
				fmt.Printf("    lakekeeper Project %s missing — re-allocating\n", lakekeeperID)
				lakekeeperID = ""
			}
		}
		if lakekeeperID == "" {
			byName, ferr := env.lkManager.FindProjectIDByName(ctx, p.Name)
			if ferr != nil {
				return fmt.Errorf("find lakekeeper project for %q: %w", p.Name, ferr)
			}
			if byName != "" {
				lakekeeperID = byName
				fmt.Printf("    lakekeeper Project %s reused (matched on name)\n", lakekeeperID)
			} else {
				lakekeeperID, err = env.lkManager.CreateProject(ctx, p.Name)
				if err != nil {
					return fmt.Errorf("create lakekeeper project for %q: %w", p.Name, err)
				}
				fmt.Printf("    lakekeeper Project %s created\n", lakekeeperID)
			}
		}

		// Step 3: FGA. Check + write-if-missing so duplicates are not
		// raised even on the OpenFGA "no duplicate" path.
		ok, cerr := authz.EnsureUserHasProjectAdmin(ctx, env.authorizer, env.creatorUserID, lakekeeperID)
		if cerr != nil {
			return fmt.Errorf("check project_admin for %q: %w", p.Name, cerr)
		}
		if !ok {
			if werr := authz.WriteAdminTuple(ctx, env.authorizer, env.creatorUserID, lakekeeperID); werr != nil {
				return fmt.Errorf("write project_admin for %q: %w", p.Name, werr)
			}
			fmt.Printf("    FGA tuple user:oidc~%s project_admin project:%s written\n", env.creatorUserID, lakekeeperID)
		} else {
			fmt.Printf("    FGA tuple already in place\n")
		}

		// Step 4: persist any new id.
		if lakekeeperID != p.LakekeeperProjectID {
			if uerr := store.SetLakekeeperProjectID(ctx, pool, p.ID, lakekeeperID); uerr != nil {
				return fmt.Errorf("persist lakekeeper_project_id for %q: %w", p.Name, uerr)
			}
			fmt.Printf("    projects.lakekeeper_project_id updated to %s\n", lakekeeperID)
		}
	}
	fmt.Println("Reconcile complete.")
	return nil
}

// adminGrant writes an FGA membership tuple granting a user a role on a
// Datuplet project. The `--role` flag maps as follows:
//
//	admin  → project_admin  (lock-out-protected lakekeeper builtin)
//	editor → editor         (data_admin + describe via union)
//	user   → editor         (alias for editor — backward compat)
//	viewer → viewer         (describe only)
//
// NOTE: revoke is not yet a separate subcommand. To remove a grant, use the
// OpenFGA management API directly or re-run delete-project + create-project.
func adminGrant(ctx context.Context, pool *pgxpool.Pool, args []string) error {
	fs := flag.NewFlagSet("grant", flag.ExitOnError)
	email := fs.String("user", "", "User email (required)")
	projectName := fs.String("project", "", "Project name (required)")
	role := fs.String("role", "editor", "Role: 'admin', 'editor' (or 'user' alias), or 'viewer'")
	env, err := dialProjectProvisioning(ctx, pool, fs, args)
	if err != nil {
		return err
	}
	if *email == "" || *projectName == "" {
		return fmt.Errorf("--user and --project are required")
	}

	// Resolve the user row from Postgres.
	u, err := store.GetUserByEmail(ctx, pool, *email)
	if err != nil {
		return fmt.Errorf("user: %w", err)
	}

	// Resolve the project row and its lakekeeper Project ID.
	proj, err := store.GetProjectByName(ctx, pool, *projectName)
	if err != nil {
		return fmt.Errorf("project %q not found: %w", *projectName, err)
	}
	if proj.LakekeeperProjectID == "" {
		return fmt.Errorf("project %q has no lakekeeper Project ID — wait for ensure-project-authz to complete", *projectName)
	}

	// Map --role to the FGA relation.
	var relation string
	switch *role {
	case "admin":
		relation = "project_admin"
	case "editor", "user":
		relation = "editor"
	case "viewer":
		relation = "viewer"
	default:
		return fmt.Errorf("unknown role %q (valid: admin | editor | user | viewer)", *role)
	}

	tuple := authz.Tuple{
		User:     authz.UserObject(u.ID.String()).String(),
		Relation: relation,
		Object:   authz.ProjectObject(proj.LakekeeperProjectID),
	}
	if err := env.authorizer.WriteTuples(ctx, []authz.Tuple{tuple}); err != nil {
		return fmt.Errorf("write FGA tuple: %w", err)
	}
	fmt.Printf("Granted role %q on %q to %s\n", relation, *projectName, u.Email)
	fmt.Printf("  FGA tuple: %s %s %s\n", tuple.User, tuple.Relation, tuple.Object.String())
	return nil
}

func adminKeygen(args []string) error {
	fs := flag.NewFlagSet("keygen", flag.ExitOnError)
	privOut := fs.String("private-out", "", "Output path for the PEM-encoded private key (required)")
	pubOut := fs.String("public-out", "", "Output path for the PEM-encoded public key (required)")
	bits := fs.Int("bits", 2048, "RSA key size in bits (2048 or 4096)")
	force := fs.Bool("force", false, "Overwrite existing files")
	_ = fs.Parse(args)

	if *privOut == "" || *pubOut == "" {
		return fmt.Errorf("--private-out and --public-out are required")
	}
	if *privOut == *pubOut {
		// Same destination would write the private key then overwrite it
		// with the public key — silently leaving no usable private key.
		return fmt.Errorf("--private-out and --public-out must point to different files")
	}
	if *bits != 2048 && *bits != 4096 {
		return fmt.Errorf("--bits must be 2048 or 4096")
	}

	// Refuse to overwrite unless --force. Existence check runs before any
	// writes; under --force we still preserve the existing keypair until
	// both new files are fully written.
	if !*force {
		for _, p := range []string{*privOut, *pubOut} {
			if _, err := os.Stat(p); err == nil {
				return fmt.Errorf("%s already exists (pass --force to overwrite)", p)
			}
		}
	}

	priv, err := rsa.GenerateKey(rand.Reader, *bits)
	if err != nil {
		return fmt.Errorf("generate: %w", err)
	}

	privDER, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		return fmt.Errorf("marshal priv: %w", err)
	}
	pubDER, err := x509.MarshalPKIXPublicKey(&priv.PublicKey)
	if err != nil {
		return fmt.Errorf("marshal pub: %w", err)
	}

	// Atomic swap: write to .tmp siblings, then rename into place. Rename
	// on the same filesystem is atomic on POSIX and overwrites the target,
	// so rotation under --force is safe — if either write fails, the
	// existing keypair is untouched.
	privTmp := *privOut + ".tmp"
	pubTmp := *pubOut + ".tmp"

	if err := os.WriteFile(privTmp, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: privDER}), 0o400); err != nil {
		return fmt.Errorf("write priv.tmp: %w", err)
	}
	if err := os.WriteFile(pubTmp, pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: pubDER}), 0o444); err != nil {
		_ = os.Remove(privTmp)
		return fmt.Errorf("write pub.tmp: %w", err)
	}
	if err := os.Rename(privTmp, *privOut); err != nil {
		_ = os.Remove(privTmp)
		_ = os.Remove(pubTmp)
		return fmt.Errorf("rename priv: %w", err)
	}
	if err := os.Rename(pubTmp, *pubOut); err != nil {
		// Private key is already in place under its new identity; the
		// caller will see this error and can rerun with the old public
		// key still on disk. Worst case: mismatched keys briefly, but no
		// silent loss.
		_ = os.Remove(pubTmp)
		return fmt.Errorf("rename pub: %w", err)
	}

	fmt.Printf("Wrote %s (mode 0400) and %s (mode 0444)\n", *privOut, *pubOut)
	fmt.Println("Point pipeline-api at the private key via SIGNING_KEY_FILE; lakekeeper consumes the JWKS endpoint pipeline-api publishes at /api/v1/auth/jwks.json.")
	return nil
}
