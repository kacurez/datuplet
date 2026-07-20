package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
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

// Environment variables that close the loop for headless (agent) CLI usage
// — see RFC 027 §7. Honored only when the corresponding flag is unset.
const (
	envAPIToken = "DATUPLET_API_TOKEN"
	envRemote   = "DATUPLET_REMOTE"
)

// loadRemoteArgs reads ~/.datuplet/token (or tokenFileFlag if non-empty) and
// ~/.datuplet/cluster.json, validates the token expiry, resolves the
// active project from the user's available projects, and returns a
// populated remoteArgs. Returns a human-friendly error mentioning
// `datuplet login --remote` on any missing/expired credential.
//
// Headless resolution (RFC 027 §7): the pipeline-api bearer token and the
// remote URL each resolve through a three-tier precedence so agents never
// need to run `datuplet login` interactively:
//   - api-token: --token-file > $DATUPLET_API_TOKEN > ~/.datuplet/api-token
//   - remote:    --remote (the `remote` param) > $DATUPLET_REMOTE > cluster.json's
//     stored pipeline_api_url
//
// When both the api-token and the remote resolve from flags/env alone (no
// --token-file), loadRemoteArgs never touches ~/.datuplet at all — the
// fast path below returns immediately. In that mode there is no local
// project list to resolve against, so projectFlag (if any) is passed
// straight through as the Datuplet project ID: agents must supply the
// project UUID via --project when running fully headless.
//
// projectFlag is the value passed via `--project <name>` (empty = unset).
// Resolution rules (multi-project ergonomics) for the disk-backed path:
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

	envAPITokenVal := os.Getenv(envAPIToken)
	effectiveRemote := remote
	if effectiveRemote == "" {
		effectiveRemote = os.Getenv(envRemote)
	}

	// Headless fast path: the api-token and the remote are both already
	// resolved from flags/env — no ~/.datuplet file needs to be read.
	if tokenFileFlag == "" && effectiveRemote != "" && envAPITokenVal != "" {
		return &remoteArgs{
			Remote:   effectiveRemote,
			APIToken: envAPITokenVal, // SECURITY: never logged or printed
			ID:       projectFlag,
		}, nil
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
		return nil, fmt.Errorf("read token file %s: %w\n(run `datuplet login --remote %s` first)", tokenPath, err, effectiveRemote)
	}
	rawToken := string(bytes.TrimSpace(tokBytes))
	if rawToken == "" {
		return nil, fmt.Errorf("token file %s is empty\n(run `datuplet login --remote %s` first)", tokenPath, effectiveRemote)
	}

	// Read cluster.json — always from ~/.datuplet regardless of --token-file.
	clusterPath := filepath.Join(home, ".datuplet", "cluster.json")
	clusterBytes, err := os.ReadFile(clusterPath)
	if err != nil {
		return nil, fmt.Errorf("read cluster config: %w\n(run `datuplet login --remote %s` first)", err, effectiveRemote)
	}

	var meta clusterMeta
	if err := json.Unmarshal(clusterBytes, &meta); err != nil {
		return nil, fmt.Errorf("parse cluster.json: %w\n(run `datuplet login --remote %s` first)", err, effectiveRemote)
	}

	// Validate expiry — fail closed: a missing or unparseable expires_at is
	// treated as a credentials problem, not silently skipped.
	if meta.ExpiresAt == "" {
		return nil, fmt.Errorf("token metadata corrupt (expires_at missing)\n(run `datuplet login --remote %s` first)", effectiveRemote)
	}
	exp, parseErr := time.Parse(time.RFC3339, meta.ExpiresAt)
	if parseErr != nil {
		return nil, fmt.Errorf("token metadata corrupt (expires_at not RFC3339: %v)\n(run `datuplet login --remote %s` first)", parseErr, effectiveRemote)
	}
	if time.Now().After(exp) {
		return nil, fmt.Errorf("token expired at %s\n(run `datuplet login --remote %s` first)", meta.ExpiresAt, effectiveRemote)
	}

	// Remote resolution's third tier: neither --remote nor $DATUPLET_REMOTE
	// was set, so trust whatever this cluster.json was logged into.
	if effectiveRemote == "" {
		effectiveRemote = meta.PipelineAPIURL
	}

	// Validate that the resolved remote matches the URL we logged into. An
	// empty PipelineAPIURL means this cluster.json was written by an older
	// version of `datuplet login` that did not record the URL — treat as
	// mismatch.
	if err := validateRemoteURL(effectiveRemote, meta.PipelineAPIURL); err != nil {
		return nil, err
	}

	// NOTE: lakekeeper_url validation is consumer-specific. `trigger` and
	// `storage` talk only to pipeline-api, which has its own lakekeeper
	// connection — they don't need this field. Earlier we validated it
	// unconditionally here, which incorrectly blocked the trigger/storage
	// paths against clusters where the lakekeeper_url is not advertised
	// in the /auth/token response (deploy-config-dependent).

	// Resolve api-token: --token-file (reuse the file already read above as
	// rawToken) > $DATUPLET_API_TOKEN > ~/.datuplet/api-token. Validate
	// expiry from meta.APIExpiresAt so we catch stale tokens before the
	// first HTTP call. Soft-fail on the default-file tier: an empty
	// api-token means the server is an older version; trigger/storage will
	// report a clear error when they get 401.
	var rawAPIToken string
	switch {
	case tokenFileFlag != "":
		rawAPIToken = rawToken
	case envAPITokenVal != "":
		rawAPIToken = envAPITokenVal
	default:
		apiTokenPath := filepath.Join(home, ".datuplet", "api-token")
		apiTokBytes, apiTokErr := os.ReadFile(apiTokenPath)
		if apiTokErr == nil {
			rawAPIToken = string(bytes.TrimSpace(apiTokBytes))
		}
	}
	if rawAPIToken != "" && meta.APIExpiresAt != "" {
		apiExp, apiParseErr := time.Parse(time.RFC3339, meta.APIExpiresAt)
		if apiParseErr != nil {
			return nil, fmt.Errorf("api-token metadata corrupt (api_expires_at not RFC3339: %v)\n(run `datuplet login --remote %s` first)", apiParseErr, effectiveRemote)
		}
		if time.Now().After(apiExp) {
			return nil, fmt.Errorf("api-token expired at %s\n(run `datuplet login --remote %s` first)", meta.APIExpiresAt, effectiveRemote)
		}
	}

	id, projectID, projectName, perr := resolveProject(meta.Projects, projectFlag, effectiveRemote)
	if perr != nil {
		return nil, perr
	}

	return &remoteArgs{
		Remote:              effectiveRemote,
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
