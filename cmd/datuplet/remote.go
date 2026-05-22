package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/datuplet/datuplet/pkg/lib/orchestrator/docker"
	"github.com/datuplet/datuplet/pkg/pipeline"
	"github.com/google/uuid"
)

// remoteArgs holds the resolved configuration for `datuplet run --remote`.
// All fields are populated by loadRemoteArgs; never populated directly.
type remoteArgs struct {
	// Remote is the pipeline-api URL passed via --remote (informational
	// only — used in error messages and the success print).
	Remote string
	// PipelineYAML is the host path to the pipeline YAML file.
	PipelineYAML string
	// Token is the raw lakekeeper JWT bearer string (NEVER print or log this).
	// Used by `datuplet run --remote` for lakekeeper / gateway auth.
	Token string
	// APIToken is the pipeline-api bearer token (aud=datuplet-api,
	// token_kind=cli-api, NEVER print or log this). Used by `datuplet
	// trigger` and `datuplet storage` for pipeline-api auth.
	APIToken string
	// TokenPath is the absolute host path to the token file. The Docker
	// orchestrator bind-mounts this path at
	// /var/run/secrets/datuplet-runtoken/token in every container.
	// Must be absolute (Docker -v requirement).
	TokenPath string
	// LakekeeperURL is the lakekeeper REST catalog base URL.
	LakekeeperURL string
	// WarehouseName is the lakekeeper warehouse name.
	WarehouseName string
	// ID is the Datuplet project UUID — distinct from LakekeeperProjectID.
	// pipeline-api's /api/v1/projects/{pid}/... routes parse {pid} as
	// this Datuplet ID; lakekeeper calls use LakekeeperProjectID.
	ID string
	// LakekeeperProjectID is the lakekeeper Project UUID forwarded as
	// `x-project-id` on every catalog/STS call. Resolved from
	// cluster.json::projects via the `--project <name>` flag (if set)
	// or auto-defaulted when the user has exactly one project. Multi-
	// project users without --project get a friendly error listing
	// their projects.
	LakekeeperProjectID string
	// ProjectName is the human-readable project name (for the success
	// banner / debugging). Resolved alongside LakekeeperProjectID.
	ProjectName string
}

// loadRemoteArgs reads ~/.datuplet/token (or tokenFileFlag if non-empty) and
// ~/.datuplet/cluster.json, validates the token expiry, resolves the
// active project from the user's available projects, and returns a
// populated remoteArgs. Returns a human-friendly error mentioning
// `datuplet login --remote` on any missing/expired credential.
//
// projectFlag is the value passed via `--project <name>` (empty = unset).
// Resolution rules (multi-project ergonomics):
//   - 0 projects → error: ask an admin to grant.
//   - 1 project + flag empty → use that one (most common dev case).
//   - 1 project + flag matches its name → same outcome, explicit.
//   - 1 project + flag doesn't match → error listing what's available.
//   - N projects + flag empty → error listing what's available.
//   - N projects + flag matches one → use that one.
//   - N projects + flag doesn't match → error listing what's available.
func loadRemoteArgs(remote, tokenFileFlag, projectFlag string) (*remoteArgs, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return nil, fmt.Errorf("resolve home dir: %w", err)
	}

	// Resolve token file path.
	tokenPath := tokenFileFlag
	if tokenPath == "" {
		tokenPath = filepath.Join(home, ".datuplet", "token")
	}
	// Always resolve to absolute path — Docker bind-mounts require it.
	tokenPath, err = filepath.Abs(tokenPath)
	if err != nil {
		return nil, fmt.Errorf("resolve token path: %w", err)
	}

	tokBytes, err := os.ReadFile(tokenPath)
	if err != nil {
		return nil, fmt.Errorf("read token file %s: %w\n(run `datuplet login --remote %s` first)", tokenPath, err, remote)
	}
	rawToken := string(bytes.TrimSpace(tokBytes))
	if rawToken == "" {
		return nil, fmt.Errorf("token file %s is empty\n(run `datuplet login --remote %s` first)", tokenPath, remote)
	}

	// Read cluster.json — always from ~/.datuplet regardless of --token-file.
	clusterPath := filepath.Join(home, ".datuplet", "cluster.json")
	clusterBytes, err := os.ReadFile(clusterPath)
	if err != nil {
		return nil, fmt.Errorf("read cluster config: %w\n(run `datuplet login --remote %s` first)", err, remote)
	}

	var meta clusterMeta
	if err := json.Unmarshal(clusterBytes, &meta); err != nil {
		return nil, fmt.Errorf("parse cluster.json: %w\n(run `datuplet login --remote %s` first)", err, remote)
	}

	// Validate expiry — fail closed: a missing or unparseable expires_at is
	// treated as a credentials problem, not silently skipped.
	if meta.ExpiresAt == "" {
		return nil, fmt.Errorf("token metadata corrupt (expires_at missing)\n(run `datuplet login --remote %s` first)", remote)
	}
	exp, parseErr := time.Parse(time.RFC3339, meta.ExpiresAt)
	if parseErr != nil {
		return nil, fmt.Errorf("token metadata corrupt (expires_at not RFC3339: %v)\n(run `datuplet login --remote %s` first)", parseErr, remote)
	}
	if time.Now().After(exp) {
		return nil, fmt.Errorf("token expired at %s\n(run `datuplet login --remote %s` first)", meta.ExpiresAt, remote)
	}

	// Validate that --remote matches the URL we logged into. An empty
	// PipelineAPIURL means this cluster.json was written by an older version
	// of `datuplet login` that did not record the URL — treat as mismatch.
	if err := validateRemoteURL(remote, meta.PipelineAPIURL); err != nil {
		return nil, err
	}

	// NOTE: lakekeeper_url validation is consumer-specific. `run --remote`
	// (local-Docker exec) calls requireLakekeeperURL() in runRemote because
	// it bind-mounts the URL into spawned containers. `trigger` and
	// `storage` talk only to pipeline-api, which has its own lakekeeper
	// connection — they don't need this field. Earlier we validated it
	// unconditionally here, which incorrectly blocked the trigger/storage
	// paths against clusters where the lakekeeper_url is not advertised
	// in the /auth/token response (deploy-config-dependent).

	// Read api-token — always from ~/.datuplet/api-token regardless of
	// --token-file. Validate expiry from meta.APIExpiresAt so we catch
	// stale tokens before the first HTTP call. Soft-fail: an empty api-token
	// means the server is an older version; trigger/storage will report a
	// clear error when they get 401.
	apiTokenPath := filepath.Join(home, ".datuplet", "api-token")
	apiTokBytes, apiTokErr := os.ReadFile(apiTokenPath)
	var rawAPIToken string
	if apiTokErr == nil {
		rawAPIToken = string(bytes.TrimSpace(apiTokBytes))
	}
	if rawAPIToken != "" && meta.APIExpiresAt != "" {
		apiExp, apiParseErr := time.Parse(time.RFC3339, meta.APIExpiresAt)
		if apiParseErr != nil {
			return nil, fmt.Errorf("api-token metadata corrupt (api_expires_at not RFC3339: %v)\n(run `datuplet login --remote %s` first)", apiParseErr, remote)
		}
		if time.Now().After(apiExp) {
			return nil, fmt.Errorf("api-token expired at %s\n(run `datuplet login --remote %s` first)", meta.APIExpiresAt, remote)
		}
	}

	id, projectID, projectName, perr := resolveProject(meta.Projects, projectFlag, remote)
	if perr != nil {
		return nil, perr
	}

	return &remoteArgs{
		Remote:              remote,
		Token:               rawToken,    // SECURITY: never logged or printed
		APIToken:            rawAPIToken, // SECURITY: never logged or printed
		TokenPath:           tokenPath,
		LakekeeperURL:       meta.LakekeeperURL,
		WarehouseName:       meta.WarehouseName,
		ID:                  id,
		LakekeeperProjectID: projectID,
		ProjectName:         projectName,
	}, nil
}

// resolveProject implements the project-selection rules documented on
// loadRemoteArgs. Returns (id, lakekeeper_project_id, name, nil) on success,
// where id is the Datuplet project UUID and lakekeeper_project_id is the
// corresponding lakekeeper project identifier.
// On error returns a human-friendly message that lists the user's
// available projects so they can pick one.
func resolveProject(projects []clusterMetaProject, flag, remote string) (id, lakekeeperID, name string, err error) {
	if len(projects) == 0 {
		return "", "", "", fmt.Errorf("no projects available — ask an admin to grant you access via\n  pipeline-api admin grant --user <your-email> --project <name> --role editor\n(then re-run `datuplet login --remote %s`)", remote)
	}
	if flag == "" {
		if len(projects) == 1 {
			return projects[0].ID, projects[0].LakekeeperProjectID, projects[0].Name, nil
		}
		// Ambiguous — list and ask.
		names := make([]string, 0, len(projects))
		for _, p := range projects {
			names = append(names, p.Name)
		}
		return "", "", "", fmt.Errorf("you have access to multiple projects; pass --project <name>\navailable: %s", strings.Join(names, ", "))
	}
	for _, p := range projects {
		if p.Name == flag {
			return p.ID, p.LakekeeperProjectID, p.Name, nil
		}
	}
	names := make([]string, 0, len(projects))
	for _, p := range projects {
		names = append(names, p.Name)
	}
	return "", "", "", fmt.Errorf("project %q not found in your accessible projects\navailable: %s", flag, strings.Join(names, ", "))
}

// RequireAPIToken returns an error when r.APIToken is empty. Trigger and
// storage commands must call this right after loadRemoteArgs to surface a
// clear, actionable message instead of a cryptic 401 from pipeline-api.
// The condition arises when the user has a token file from an older
// pipeline-api that did not yet issue cli-api bearer tokens.
func (r *remoteArgs) RequireAPIToken() error {
	if r.APIToken == "" {
		return fmt.Errorf("WARN: ~/.datuplet/api-token not present — your `datuplet login` may be from an older pipeline-api. Re-run `datuplet login --remote %s` against an upgraded server", r.Remote)
	}
	return nil
}

// generateRunID returns a fresh UUID string for use as a pipeline run
// identifier. Extracted as a function so tests can assert uniqueness without
// launching Docker containers.
func generateRunID() string {
	return uuid.New().String()
}

// normalizeURL strips trailing slashes and lowercases the scheme + host so
// that minor formatting differences (e.g. "HTTP://Localhost:30081/" vs
// "http://localhost:30081") compare equal.
func normalizeURL(raw string) string {
	u, err := url.Parse(strings.TrimRight(raw, "/"))
	if err != nil {
		// Fall back to simple lowercase + trim if url.Parse fails.
		return strings.ToLower(strings.TrimRight(raw, "/"))
	}
	u.Scheme = strings.ToLower(u.Scheme)
	u.Host = strings.ToLower(u.Host)
	return u.String()
}

// validateRemoteURL checks that the requested --remote URL matches the one
// stored in cluster.json. Returns nil if they match after normalization.
func validateRemoteURL(requested, saved string) error {
	if saved == "" {
		return fmt.Errorf("--remote %q does not match logged-in URL (unknown — old cluster.json)\n(run `datuplet login --remote %s` first)", requested, requested)
	}
	if normalizeURL(requested) != normalizeURL(saved) {
		return fmt.Errorf("--remote %q does not match logged-in URL %q\n(run `datuplet login --remote %s` first)", requested, saved, requested)
	}
	return nil
}

// runRemote implements `datuplet run --remote <url> <pipeline.yaml>`.
//
// It reads the stored token + cluster config, generates a fresh run-id,
// sets the process env so the pipeline Controller and Docker orchestrator
// pick up the remote lakekeeper details, mounts the token file into every
// spawned container, and drives pkg/pipeline.Controller to completion.
//
// Security invariants:
//   - args.Token is NEVER printed, logged, or interpolated into any
//     user-visible string.
//   - The token file is bind-mounted read-only; its host path is always
//     absolute.
//   - Ctrl+C cancels the context; the orchestrator's existing container
//     cleanup on ctx cancellation prevents token-holding containers from
//     lingering beyond the JWT's TTL.
func runRemote(remoteFlag, tokenFileFlag, projectFlag, pipelineYAML string) error {
	args, err := loadRemoteArgs(remoteFlag, tokenFileFlag, projectFlag)
	if err != nil {
		return err
	}
	// Local-Docker exec mode REQUIRES lakekeeper_url — it bind-mounts the
	// URL into spawned containers as DATUPLET_LAKEKEEPER_URL. Trigger and
	// storage do not need this and skip the check.
	if args.LakekeeperURL == "" {
		return fmt.Errorf("cluster.json missing lakekeeper_url\n(run `datuplet login --remote %s` first against a cluster that publishes it)", remoteFlag)
	}
	args.PipelineYAML = pipelineYAML

	// Generate a fresh run-id. This is pre-seeded into the Controller so
	// the printed run-id matches the datuplet.run-id iceberg snapshot key.
	runID := generateRunID()

	// Set process env so the Controller (loadInfraConfigFromEnv) and
	// spawned containers pick up the remote lakekeeper config.
	// We use the DATUPLET_* prefixed names that runner.go reads; the
	// orchestrator translates them into container-level LAKEKEEPER_URL /
	// WAREHOUSE_NAME / LAKEKEEPER_PROJECT_ID vars automatically.
	os.Setenv("DATUPLET_LAKEKEEPER_URL", args.LakekeeperURL)              //nolint:errcheck
	os.Setenv("DATUPLET_LAKEKEEPER_WAREHOUSE", args.WarehouseName)        //nolint:errcheck
	os.Setenv("DATUPLET_LAKEKEEPER_PROJECT_ID", args.LakekeeperProjectID) //nolint:errcheck

	// Build the Docker orchestrator. The network name is intentionally
	// generic — containers only need to reach each other via the gateway
	// sidecar hostname, which is on the same network.
	orch, err := docker.NewDockerOrchestrator("datuplet-cli-net")
	if err != nil {
		return fmt.Errorf("create docker orchestrator: %w", err)
	}
	defer orch.Cleanup(context.Background()) //nolint:errcheck

	// Tell the orchestrator to bind-mount ~/.datuplet/token (host) at
	// /var/run/secrets/datuplet-runtoken/token (container) in every
	// gateway sidecar and table-commit container so they can authenticate
	// to lakekeeper. The path is already absolute (loadRemoteArgs calls
	// filepath.Abs). Docker requires absolute paths for -v mounts.
	orch.SetRunTokenHostPath(args.TokenPath)

	// Ensure the bridge network exists before launching any container.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := orch.EnsureNetwork(ctx); err != nil {
		return fmt.Errorf("ensure docker network: %w", err)
	}

	ctrl := pipeline.New(orch)
	// Pre-seed run-id so the success message matches the iceberg audit trail.
	ctrl.SetRunID(runID)

	if err := ctrl.LoadPipeline(pipelineYAML); err != nil {
		return fmt.Errorf("load pipeline: %w", err)
	}

	// Wire Ctrl+C → context cancel → orchestrator's existing container
	// cleanup path. No additional cleanup is needed here: the
	// orchestrator's defer in ExecuteComponent / ExecuteTableCommit stops
	// and removes containers on any exit path.
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		fmt.Fprintln(os.Stderr, "\nreceived interrupt — stopping containers")
		cancel()
	}()

	fmt.Printf("datuplet run --remote %s (project=%s run-id=%s)\n", args.Remote, args.ProjectName, runID)

	if err := ctrl.Run(ctx); err != nil {
		return fmt.Errorf("pipeline run failed: %w", err)
	}

	fmt.Printf("run %s succeeded — snapshots: %s/ui/storage\n", runID, args.Remote)
	return nil
}
