package main

import (
	"bytes"
	"context"
	"embed"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

// appsScaffoldFS embeds the `datuplet apps init` scaffold (spec §5.5):
// app.js (an Appendix A example trimmed to one query), datuplet.d.ts (types
// for ctx/datuplet.query/OutputDoc blocks), esbuild.mjs + package.json (a
// working `npm run build` bundle step), and README.md. Mirrors
// ui/appshell/embed.go's directory-embed shape — the embed source (this
// file) lives in the same tree as apps_scaffold/ because `//go:embed`
// patterns cannot climb out of their own directory (no `..`).
//
//go:embed apps_scaffold
var appsScaffoldFS embed.FS

// appsScaffoldRoot is the embedded root directory name, used to compute
// paths relative to it when walking the FS.
const appsScaffoldRoot = "apps_scaffold"

// maxBundleBytes mirrors pkg/pipelineapi/apps.MaxBundleBytes (spec §4/§7's
// 5 MB raw bundle cap). Hand-mirrored as a CLI-local constant rather than
// imported, matching this package's existing convention of mirroring server
// shapes/limits locally (see pipelineDetailJSON, componentSummaryJSON).
// Enforced here BEFORE any HTTP call so an oversize bundle never leaves the
// machine just to be told "no" by the server's 413.
const maxBundleBytes = 5 * 1024 * 1024

// runApps dispatches `datuplet apps <sub> ...`.
//
// Subcommands implemented here (spec §5.5's CLI table):
//   - init:   scaffold a new app directory locally (no network, no --project)
//   - put:    PUT    /api/v1/projects/{pid}/apps/{name}   (upload -> draft)
//   - get:    GET    /api/v1/projects/{pid}/apps/{name}
//   - list:   GET    /api/v1/projects/{pid}/apps
//   - delete: DELETE /api/v1/projects/{pid}/apps/{name}
//   - render: GET    {remote}/apps/{pid}/{name}[@draft]   (app-worker route)
//   - logs:   GET    /api/v1/projects/{pid}/apps/{name}/logs
//
// promote/token land in a follow-up task against the same dispatcher and
// parseAppsFlags (see the doc comment there).
func runApps(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: datuplet apps <init|put|get|list|delete|render|logs> [args]\n%s", appsHelpText())
	}
	sub := args[0]
	rest := args[1:]
	switch sub {
	case "init":
		return runAppsInit(rest)
	case "put":
		return runAppsPut(rest)
	case "get", "show":
		return runAppsGet(rest)
	case "list", "ls":
		return runAppsList(rest)
	case "delete", "del", "rm":
		return runAppsDelete(rest)
	case "render":
		return runAppsRender(rest)
	case "logs":
		return runAppsLogs(rest)
	case "help", "-h", "--help":
		fmt.Println(appsHelpText())
		return nil
	default:
		return fmt.Errorf("unknown apps subcommand %q\n%s", sub, appsHelpText())
	}
}

func appsHelpText() string {
	return `apps subcommands:
  init <dir>                      scaffold a new app in <dir> (refuses a non-empty dir)
  put <name> --bundle <file>      upload a bundle, moving the app's draft channel to it
  get <name>                      show one app's channels + versions
  list                            list apps in the current project
  delete <name>                   delete an app (rows + its FGA identity)
  render <name> --channel draft   server-side render (the agent's test step);
                                  prints the OutputDoc JSON on success, one
                                  {error,kind,request_id,author_log} object on failure
  logs <name> [--request-id <id>] recent render logs, or one record by request id

common flags:
  --remote <url>     pipeline-api URL (defaults to logged-in cluster)
  --token-file <p>   override default ~/.datuplet/token path
  --project <name>   project to operate in (falls back to $DATUPLET_PROJECT,
                      then the logged-in cluster's default)
  --json             emit JSON output (put, get, list, render, logs)

put-only flags:
  --bundle <file>    path to the built IIFE bundle (see 'datuplet apps init'
                      and its esbuild.mjs); rejected locally above 5 MB

render-only flags:
  --channel <c>      draft (the @draft route) or production (default); anything
                      else is rejected locally
  --param k=v        bind a render param (repeatable): --param days=7 --param country=DE

logs-only flags:
  --request-id <id>  return exactly one render's record (exit 1 if not found)

exit codes (render): 0 success, 1 render failure (the app reported an error),
  20 transport failure (could not reach the service) — the agent loop branches
  on this split.

examples:
  datuplet apps init ./sales-overview
  cd sales-overview && npm install && npm run build
  datuplet apps put sales-overview --project myproj --bundle bundle.js
  datuplet apps render sales-overview --project myproj --channel draft --param days=7 --json
  datuplet apps logs sales-overview --project myproj --request-id <id> --json
  datuplet apps promote sales-overview --project myproj --version <hash>
`
}

// parseAppsFlags extracts the flags common to every `datuplet apps`
// subcommand from an arbitrary positional slice, following
// parsePipelineFlags's/parseComponentsFlags's hand-rolled convention (flags
// in any order, a single positional-args slice remains).
//
// render/promote/logs/token add their own flags here (one grammar for the
// whole `apps` family, never a second parser): --channel and the REPEATABLE
// --param (render), --request-id (logs). Threading a new return value through
// forces every existing call site to update (Go won't compile otherwise) —
// that is the intended safety net. Still to come as those subcommands land:
// --version / --expected-production (promote).
func parseAppsFlags(args []string) (positional []string, remote, tokenFile, project, bundle, channel, requestID string, params []string, asJSON bool, err error) {
	i := 0
	for i < len(args) {
		a := args[i]
		switch {
		case a == "--remote":
			if i+1 >= len(args) {
				err = fmt.Errorf("--remote requires a value")
				return
			}
			remote = args[i+1]
			i += 2
		case strings.HasPrefix(a, "--remote="):
			remote = strings.TrimPrefix(a, "--remote=")
			i++
		case a == "--token-file":
			if i+1 >= len(args) {
				err = fmt.Errorf("--token-file requires a value")
				return
			}
			tokenFile = args[i+1]
			i += 2
		case strings.HasPrefix(a, "--token-file="):
			tokenFile = strings.TrimPrefix(a, "--token-file=")
			i++
		case a == "--project":
			if i+1 >= len(args) {
				err = fmt.Errorf("--project requires a value")
				return
			}
			project = args[i+1]
			i += 2
		case strings.HasPrefix(a, "--project="):
			project = strings.TrimPrefix(a, "--project=")
			i++
		case a == "--bundle":
			if i+1 >= len(args) {
				err = fmt.Errorf("--bundle requires a value")
				return
			}
			bundle = args[i+1]
			i += 2
		case strings.HasPrefix(a, "--bundle="):
			bundle = strings.TrimPrefix(a, "--bundle=")
			i++
		case a == "--channel":
			if i+1 >= len(args) {
				err = fmt.Errorf("--channel requires a value")
				return
			}
			channel = args[i+1]
			i += 2
		case strings.HasPrefix(a, "--channel="):
			channel = strings.TrimPrefix(a, "--channel=")
			i++
		case a == "--request-id":
			if i+1 >= len(args) {
				err = fmt.Errorf("--request-id requires a value")
				return
			}
			requestID = args[i+1]
			i += 2
		case strings.HasPrefix(a, "--request-id="):
			requestID = strings.TrimPrefix(a, "--request-id=")
			i++
		case a == "--param":
			if i+1 >= len(args) {
				err = fmt.Errorf("--param requires a value")
				return
			}
			params = append(params, args[i+1])
			i += 2
		case strings.HasPrefix(a, "--param="):
			params = append(params, strings.TrimPrefix(a, "--param="))
			i++
		case a == "--json":
			asJSON = true
			i++
		case strings.HasPrefix(a, "-"):
			err = fmt.Errorf("unknown flag %q", a)
			return
		default:
			positional = append(positional, a)
			i++
		}
	}
	return
}

// appNamePattern mirrors pkg/pipelineapi/apps.appNamePattern
// (handlers.go) — the server's app-name grammar (spec §4.1): a DNS-label-
// style name. This MUST be enforced locally before a name is embedded in a
// URL path segment: url.PathEscape does NOT encode "." — PathEscape("..")
// == ".." — so an unvalidated name like ".." would let net/http's ServeMux
// (or an intermediary proxy) canonicalize the request path to a DIFFERENT,
// unintended endpoint (e.g. the bare .../apps collection, or something
// outside it) before any handler runs. This was gate-review finding C1-1
// (Major).
const appNamePattern = `^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$`

var appNameRe = regexp.MustCompile(appNamePattern)

// validateAppName is the single source of truth for the app-name check.
// Every named command (put/get/delete) calls it FIRST — right after
// extracting the positional <name> and before loadRemoteArgs or any network
// I/O — so a bad name fails fast without touching credentials, project
// resolution, or the wire at all. appsURL also calls it before embedding a
// name in a path segment, so no future named subcommand can reach the wire
// with an unvalidated name even if it forgets the early call.
func validateAppName(name string) error {
	if !appNameRe.MatchString(name) {
		return fmt.Errorf("invalid app name %q: must match %s (lowercase alphanumerics and '-', not starting or ending with '-', 1-63 chars)", name, appNamePattern)
	}
	return nil
}

// appsURL composes /api/v1/projects/{pid}/apps[/{name}[/{suffix}]],
// mirroring pipelineURL/componentsURL. suffix (e.g. "promote", "logs",
// "tokens") is appended after {name} when non-empty — unused by this task's
// subcommands, provided for the render/promote/logs/token follow-up. When
// name is non-empty it is validated via validateAppName before being
// escaped into the path; list's call (name == "") skips validation, since
// there is nothing to validate.
func appsURL(remote, projectID, name, suffix string) (string, error) {
	base := strings.TrimRight(remote, "/") + "/api/v1/projects/" + url.PathEscape(projectID) + "/apps"
	if name != "" {
		if err := validateAppName(name); err != nil {
			return "", err
		}
		base += "/" + url.PathEscape(name)
	}
	if suffix != "" {
		base += "/" + suffix
	}
	return base, nil
}

// ---------------------------------------------------------------------------
// Wire shapes — hand-mirrored from pkg/pipelineapi/apps/handlers.go, the
// same convention pipeline.go/components.go use for their server shapes.
// ---------------------------------------------------------------------------

// appPutResponseJSON mirrors putAppResponse.
type appPutResponseJSON struct {
	AppID       string `json:"app_id"`
	VersionHash string `json:"version_hash"`
}

// appChannelJSON mirrors channelJSON.
type appChannelJSON struct {
	VersionHash string `json:"version_hash"`
	UpdatedAt   string `json:"updated_at"`
}

// appVersionJSON mirrors versionJSON in apps/handlers.go (named with an
// "app" prefix here to avoid colliding with this package's own versionJSON,
// which mirrors the unrelated component-catalog shape in components.go).
type appVersionJSON struct {
	Hash      string `json:"hash"`
	SizeBytes int64  `json:"size_bytes"`
	CreatedAt string `json:"created_at"`
}

// appJSON mirrors appJSON server-side: both the list entry and the detail
// shape. Versions is populated by the detail route only.
type appJSON struct {
	AppID     string                    `json:"app_id"`
	Name      string                    `json:"name"`
	CreatedAt string                    `json:"created_at"`
	Channels  map[string]appChannelJSON `json:"channels"`
	Versions  []appVersionJSON          `json:"versions,omitempty"`
}

// appChannelOrder fixes the display order of the two valid channel names
// (migration 013's CHECK constraint — no other values exist) so table
// output is deterministic instead of depending on Go's random map iteration.
var appChannelOrder = []string{"production", "draft"}

// shortHash truncates a hex hash for compact table display. Full hashes
// remain available via --json or `apps get` (needed verbatim for
// `apps promote --version <hash>`).
func shortHash(h string) string {
	const n = 12
	if len(h) > n {
		return h[:n]
	}
	return h
}

// formatChannels renders a compact "production=<hash> draft=<hash>" summary
// for the list table; "-" when the app has no channels pointed anywhere yet.
func formatChannels(channels map[string]appChannelJSON) string {
	if len(channels) == 0 {
		return "-"
	}
	parts := make([]string, 0, len(channels))
	for _, ch := range appChannelOrder {
		if c, ok := channels[ch]; ok {
			parts = append(parts, ch+"="+shortHash(c.VersionHash))
		}
	}
	if len(parts) == 0 {
		return "-"
	}
	return strings.Join(parts, " ")
}

// ---------------------------------------------------------------------------
// init — local scaffold, no network, no --project (spec §5.5's table lists
// no project flag for init; it writes files only).
// ---------------------------------------------------------------------------

// runAppsInit implements `datuplet apps init <dir>`. Creates dir (and any
// missing parents) if absent, or refuses if it exists and already contains
// files — this is a scaffold command, not an overwrite/merge one.
func runAppsInit(args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("usage: datuplet apps init <dir>")
	}
	dir := args[0]

	entries, statErr := os.ReadDir(dir)
	switch {
	case statErr == nil:
		if len(entries) > 0 {
			return fmt.Errorf("init: %s is not empty; refusing to overwrite (choose an empty or new directory)", dir)
		}
	case os.IsNotExist(statErr):
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("init: create %s: %w", dir, err)
		}
	default:
		return fmt.Errorf("init: stat %s: %w", dir, statErr)
	}

	if err := writeAppsScaffold(dir); err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "scaffolded a Datuplet app in %s\n(next: cd %s && npm install && npm run build)\n", dir, dir)
	return nil
}

// writeAppsScaffold copies every file under the embedded apps_scaffold/
// directory into dir, preserving the relative tree (flat today; fs.WalkDir
// handles subdirectories too, so the scaffold can grow without code changes
// here).
func writeAppsScaffold(dir string) error {
	return fs.WalkDir(appsScaffoldFS, appsScaffoldRoot, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(appsScaffoldRoot, path)
		if err != nil {
			return fmt.Errorf("compute scaffold-relative path for %s: %w", path, err)
		}
		if rel == "." {
			return nil
		}
		target := filepath.Join(dir, rel)
		if d.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		data, err := appsScaffoldFS.ReadFile(path)
		if err != nil {
			return fmt.Errorf("read embedded scaffold file %s: %w", path, err)
		}
		return os.WriteFile(target, data, 0o644)
	})
}

// ---------------------------------------------------------------------------
// put/get/list/delete — author routes on pipeline-api (P2).
// ---------------------------------------------------------------------------

// runAppsPut implements `datuplet apps put <name> --bundle <file>`. Reads
// the bundle file, rejects it locally if it isn't a regular file or exceeds
// the 5 MB raw cap (before any network I/O), base64-encodes it, and PUTs
// {"bundle_base64": "..."} — the exact shape pkg/pipelineapi/apps's
// putAppRequest decodes.
func runAppsPut(args []string) error {
	positional, remote, tokenFile, project, bundlePath, _, _, _, asJSON, err := parseAppsFlags(args)
	if err != nil {
		return err
	}
	if len(positional) != 1 {
		return fmt.Errorf("usage: datuplet apps put <name> --bundle <file>")
	}
	name := positional[0]
	if err := validateAppName(name); err != nil {
		return err
	}
	if bundlePath == "" {
		return fmt.Errorf("put: --bundle <file> is required\nusage: datuplet apps put <name> --bundle <file>")
	}

	info, err := os.Stat(bundlePath)
	if err != nil {
		return fmt.Errorf("stat bundle %s: %w", bundlePath, err)
	}
	// Reject anything that isn't a plain file BEFORE trusting its reported
	// size: a FIFO/device/directory can report a bogus or zero Size(), which
	// would let the cap below be bypassed (or a non-file slurped).
	if !info.Mode().IsRegular() {
		return fmt.Errorf("put: %s is not a regular file (bundle must be a plain file)", bundlePath)
	}
	// Enforce the 5 MB raw cap as a LOCAL error before reading the file or
	// making any HTTP call — no reason to ship an oversize bundle just to
	// receive the server's 413. This Stat-based check is the fast path.
	if info.Size() > maxBundleBytes {
		return fmt.Errorf("put: bundle %s is %d bytes, exceeds the %d byte (5 MB) limit", bundlePath, info.Size(), maxBundleBytes)
	}
	f, err := os.Open(bundlePath)
	if err != nil {
		return fmt.Errorf("open bundle %s: %w", bundlePath, err)
	}
	// Re-check the actual byte count against a LimitReader capped one byte
	// above the limit, rather than trusting Stat's size a second time: the
	// file can grow between Stat and Open/Read (TOCTOU), and this also
	// bounds memory use instead of fully reading an oversize file just to
	// reject it.
	raw, err := io.ReadAll(io.LimitReader(f, maxBundleBytes+1))
	closeErr := f.Close()
	if err != nil {
		return fmt.Errorf("read bundle %s: %w", bundlePath, err)
	}
	if closeErr != nil {
		return fmt.Errorf("close bundle %s: %w", bundlePath, closeErr)
	}
	if len(raw) > maxBundleBytes {
		return fmt.Errorf("put: bundle %s exceeds the %d byte (5 MB) limit", bundlePath, maxBundleBytes)
	}

	resolved, err := loadRemoteArgs(remote, tokenFile, project)
	if err != nil {
		return err
	}
	if err := resolved.RequireAPIToken(); err != nil {
		return err
	}

	reqBody, err := json.Marshal(struct {
		BundleBase64 string `json:"bundle_base64"`
	}{BundleBase64: base64.StdEncoding.EncodeToString(raw)})
	if err != nil {
		return fmt.Errorf("encode bundle: %w", err)
	}

	putURL, err := appsURL(resolved.Remote, resolved.ID, name, "")
	if err != nil {
		return err
	}
	status, respBody, err := doAuthedRequest(context.Background(),
		http.MethodPut, putURL,
		resolved.APIToken, "application/json", bytes.NewReader(reqBody))
	if err != nil {
		return err
	}
	if status != http.StatusOK {
		return fmt.Errorf("put app: HTTP %d: %s", status, string(respBody))
	}

	if asJSON {
		fmt.Println(string(respBody))
		return nil
	}
	var decoded appPutResponseJSON
	if err := json.Unmarshal(respBody, &decoded); err != nil {
		return fmt.Errorf("decode put response: %w", err)
	}
	fmt.Printf("%-38s %s\n", "APP_ID", "VERSION_HASH")
	fmt.Printf("%-38s %s\n", decoded.AppID, decoded.VersionHash)
	return nil
}

// runAppsGet implements `datuplet apps get <name>`.
func runAppsGet(args []string) error {
	positional, remote, tokenFile, project, _, _, _, _, asJSON, err := parseAppsFlags(args)
	if err != nil {
		return err
	}
	if len(positional) != 1 {
		return fmt.Errorf("usage: datuplet apps get <name>")
	}
	name := positional[0]
	if err := validateAppName(name); err != nil {
		return err
	}

	resolved, err := loadRemoteArgs(remote, tokenFile, project)
	if err != nil {
		return err
	}
	if err := resolved.RequireAPIToken(); err != nil {
		return err
	}

	getURL, err := appsURL(resolved.Remote, resolved.ID, name, "")
	if err != nil {
		return err
	}
	status, body, err := doAuthedRequest(context.Background(),
		http.MethodGet, getURL,
		resolved.APIToken, "", nil)
	if err != nil {
		return err
	}
	if status == http.StatusNotFound {
		return fmt.Errorf("app %q not found in project %q", name, resolved.ProjectName)
	}
	if status != http.StatusOK {
		return fmt.Errorf("get app: HTTP %d: %s", status, string(body))
	}

	if asJSON {
		fmt.Println(string(body))
		return nil
	}

	var detail appJSON
	if err := json.Unmarshal(body, &detail); err != nil {
		return fmt.Errorf("decode get response: %w", err)
	}
	fmt.Printf("Name:    %s\n", detail.Name)
	fmt.Printf("App ID:  %s\n", detail.AppID)
	fmt.Printf("Created: %s\n", detail.CreatedAt)
	fmt.Println()
	fmt.Printf("%-12s %s\n", "CHANNEL", "VERSION_HASH")
	for _, ch := range appChannelOrder {
		hash := "(none)"
		if c, ok := detail.Channels[ch]; ok {
			hash = c.VersionHash
		}
		fmt.Printf("%-12s %s\n", ch, hash)
	}
	fmt.Println()
	fmt.Printf("%-70s %-12s %s\n", "VERSION_HASH", "SIZE_BYTES", "CREATED")
	for _, v := range detail.Versions {
		fmt.Printf("%-70s %-12d %s\n", v.Hash, v.SizeBytes, v.CreatedAt)
	}
	return nil
}

// runAppsList implements `datuplet apps list`.
func runAppsList(args []string) error {
	positional, remote, tokenFile, project, _, _, _, _, asJSON, err := parseAppsFlags(args)
	if err != nil {
		return err
	}
	if len(positional) > 0 {
		return fmt.Errorf("list takes no positional args; got %q", positional)
	}

	resolved, err := loadRemoteArgs(remote, tokenFile, project)
	if err != nil {
		return err
	}
	if err := resolved.RequireAPIToken(); err != nil {
		return err
	}

	listURL, err := appsURL(resolved.Remote, resolved.ID, "", "")
	if err != nil {
		return err // unreachable in practice: name == "" skips validation
	}
	status, body, err := doAuthedRequest(context.Background(),
		http.MethodGet, listURL,
		resolved.APIToken, "", nil)
	if err != nil {
		return err
	}
	if status != http.StatusOK {
		return fmt.Errorf("list apps: HTTP %d: %s", status, string(body))
	}

	if asJSON {
		fmt.Println(string(body))
		return nil
	}

	var items []appJSON
	if err := json.Unmarshal(body, &items); err != nil {
		return fmt.Errorf("decode list response: %w", err)
	}
	if len(items) == 0 {
		fmt.Println("(no apps)")
		return nil
	}
	fmt.Printf("%-24s %-38s %-25s %s\n", "NAME", "APP_ID", "CREATED", "CHANNELS")
	for _, a := range items {
		fmt.Printf("%-24s %-38s %-25s %s\n", a.Name, a.AppID, a.CreatedAt, formatChannels(a.Channels))
	}
	return nil
}

// runAppsDelete implements `datuplet apps delete <name>`. No interactive
// confirmation prompt: spec §5.5 requires every `apps` subcommand to be
// non-interactive (R6, agent-first) — unlike `pipeline delete`, which
// predates that requirement.
func runAppsDelete(args []string) error {
	positional, remote, tokenFile, project, _, _, _, _, _, err := parseAppsFlags(args)
	if err != nil {
		return err
	}
	if len(positional) != 1 {
		return fmt.Errorf("usage: datuplet apps delete <name>")
	}
	name := positional[0]
	if err := validateAppName(name); err != nil {
		return err
	}

	resolved, err := loadRemoteArgs(remote, tokenFile, project)
	if err != nil {
		return err
	}
	if err := resolved.RequireAPIToken(); err != nil {
		return err
	}

	deleteURL, err := appsURL(resolved.Remote, resolved.ID, name, "")
	if err != nil {
		return err
	}
	status, body, err := doAuthedRequest(context.Background(),
		http.MethodDelete, deleteURL,
		resolved.APIToken, "", nil)
	if err != nil {
		return err
	}
	switch status {
	case http.StatusNoContent, http.StatusOK:
		fmt.Fprintf(os.Stderr, "app %q deleted\n", name)
		return nil
	case http.StatusNotFound:
		return fmt.Errorf("app %q not found in project %q", name, resolved.ProjectName)
	default:
		return fmt.Errorf("delete app: HTTP %d: %s", status, string(body))
	}
}

// ---------------------------------------------------------------------------
// render — the agent's test step. GET {remote}/apps/{pid}/{name}[@draft] on
// the app-worker route (spec §4.1's ingress path prefix, same host as
// pipeline-api — no separate app-worker URL), Accept: application/json,
// bearer api-token.
// ---------------------------------------------------------------------------

// appsRenderHTTPClient is dedicated to `datuplet apps render`. Its timeout
// must sit ABOVE the server-side render wall-clock hard cap (30 s, spec §7)
// plus connection + ingress overhead, so a slow-but-valid render returns the
// worker's structured envelope rather than the CLI firing its own opaque
// transport timeout first — pipelineHTTPClient's fixed 30 s is too tight
// against that 30 s cap (C0 §2). CheckRedirect refuses ALL redirects so the
// author's bearer api-token can never be forwarded to a redirect target: this
// is the first `apps` call that crosses to the app-worker route rather than a
// plain pipeline-api /api/v1 path, and it carries the bearer (mirrors
// query.go's queryHTTPClient).
var appsRenderHTTPClient = &http.Client{
	Timeout: 60 * time.Second,
	CheckRedirect: func(*http.Request, []*http.Request) error {
		return fmt.Errorf("datuplet apps render: unexpected redirect (refused)")
	},
}

// appsRenderMaxResponseBytes bounds the render response read. The OutputDoc is
// capped at 2 MiB (spec §7); 4 MiB gives headroom for JSON framing while still
// bounding a misbehaving endpoint.
const appsRenderMaxResponseBytes = 4 << 20

// appsRenderEnvelope mirrors app-worker's §8 error envelope (pkg/appworker:
// {error, kind, request_id}). A non-empty Kind is the signal that a non-200
// response is a genuine render failure (exit 1) rather than a transport /
// non-envelope error (exit 20).
type appsRenderEnvelope struct {
	Error     string `json:"error"`
	Kind      string `json:"kind"`
	RequestID string `json:"request_id"`
}

// appsRenderFailureJSON is the ONE machine-readable object `apps render` prints
// on a render failure with --json (spec §5.5): the §8 envelope plus the
// matching author render-log record. AuthorLog is a json.RawMessage so it
// serializes as the verbatim log record, or `null` when the lookup 404s (a nil
// RawMessage marshals to null).
type appsRenderFailureJSON struct {
	Error     string          `json:"error"`
	Kind      string          `json:"kind"`
	RequestID string          `json:"request_id"`
	AuthorLog json.RawMessage `json:"author_log"`
}

// appsRenderLogRecord mirrors the fields of pkg/pipelineapi/apps.renderLogJSON
// the CLI renders in text mode (hand-mirrored, this package's convention).
type appsRenderLogRecord struct {
	RequestID   string `json:"request_id"`
	VersionHash string `json:"version_hash"`
	Channel     string `json:"channel"`
	StartedAt   string `json:"started_at"`
	DurationMS  int64  `json:"duration_ms"`
	Outcome     string `json:"outcome"`
	LogText     string `json:"log_text"`
	Error       string `json:"error"`
}

// runAppsRender implements
// `datuplet apps render <name> --channel draft|production --param k=v … [--json]`.
//
// It is a thin wrapper over the render route (spec §5.5): GET
// {remote}/apps/{pid}/{name}[@draft]?<params> with the author's bearer
// api-token and Accept: application/json (which selects the JSON response
// mode, §4.2). This is the agent's test step — implement → put → render →
// assert on JSON → iterate, no browser needed.
//
// Output & exit codes (a hard contract the agent loop branches on):
//   - HTTP 200: print the OutputDoc JSON verbatim; exit 0.
//   - non-200 carrying the §8 envelope {error, kind, request_id}: a render
//     failure (user-error class). Fetch the matching author log via
//     logs?request_id=<id> and, with --json, print ONE object
//     {error, kind, request_id, author_log} (author_log null on a logs 404);
//     in text mode, human-format the same fields + the log excerpt on stderr.
//     Exit 1.
//   - could not reach the service, or a non-200 with no envelope: transport
//     failure. Exit 20 (via exitCodeErr, the repo's FailedApplication band).
func runAppsRender(args []string) error {
	positional, remote, tokenFile, project, _, channel, _, params, asJSON, err := parseAppsFlags(args)
	if err != nil {
		return err
	}
	if len(positional) != 1 {
		return fmt.Errorf("usage: datuplet apps render <name> --channel draft|production [--param k=v ...] [--json]")
	}
	name := positional[0]
	// Validate the name (and channel/params) locally, before any network I/O
	// or credential resolution — deterministic, cheap, and independent of
	// ambient ~/.datuplet state (mirrors get/put/delete).
	if err := validateAppName(name); err != nil {
		return err
	}
	channel, err = normalizeRenderChannel(channel)
	if err != nil {
		return err
	}
	paramQuery, err := encodeRenderParams(params)
	if err != nil {
		return err
	}

	resolved, err := loadRemoteArgs(remote, tokenFile, project)
	if err != nil {
		return err
	}
	if err := resolved.RequireAPIToken(); err != nil {
		return err
	}

	renderURL, err := appsRenderURL(resolved.Remote, resolved.ID, name, channel, paramQuery)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, renderURL, nil)
	if err != nil {
		return &exitCodeErr{code: 20, err: fmt.Errorf("apps render: build request: %w", err)}
	}
	req.Header.Set("Authorization", "Bearer "+resolved.APIToken)
	req.Header.Set("Accept", "application/json")

	resp, err := appsRenderHTTPClient.Do(req)
	if err != nil {
		// Could not reach the service (or a refused redirect) — transport class.
		return &exitCodeErr{code: 20, err: fmt.Errorf("apps render: %s: %w", renderURL, err)}
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, appsRenderMaxResponseBytes))
	if err != nil {
		return &exitCodeErr{code: 20, err: fmt.Errorf("apps render: read response: %w", err)}
	}

	if resp.StatusCode == http.StatusOK {
		// Success: the OutputDoc, printed verbatim — the agent asserts on it.
		fmt.Println(strings.TrimRight(string(body), "\n"))
		return nil
	}

	// Non-200: distinguish a structured §8 render-failure envelope (exit 1)
	// from an unstructured/transport error (exit 20). A non-empty kind is the
	// discriminator — a proxy/ingress error page has no `kind`.
	var env appsRenderEnvelope
	if json.Unmarshal(body, &env) != nil || env.Kind == "" {
		return &exitCodeErr{code: 20, err: fmt.Errorf("apps render: HTTP %d (no error envelope): %s",
			resp.StatusCode, truncateForError(string(body)))}
	}

	// Render failure. Best-effort author-log fetch (never turns this into a
	// transport failure): a 404 / miss yields author_log:null.
	authorLog := fetchAuthorLog(context.Background(), resolved.Remote, resolved.APIToken, resolved.ID, name, env.RequestID)

	if asJSON {
		obj, merr := json.Marshal(appsRenderFailureJSON{
			Error: env.Error, Kind: env.Kind, RequestID: env.RequestID, AuthorLog: authorLog,
		})
		if merr != nil {
			return &exitCodeErr{code: 20, err: fmt.Errorf("apps render: encode failure object: %w", merr)}
		}
		fmt.Println(string(obj))
	} else {
		formatRenderFailureText(os.Stderr, env, authorLog)
	}
	// Plain error → default exit 1: the render reached the service and the app
	// (or its input) failed. Distinct from the exitCodeErr{20} transport paths.
	return fmt.Errorf("render failed: %s (kind=%s, request_id=%s)", env.Error, env.Kind, env.RequestID)
}

// normalizeRenderChannel validates --channel. Empty defaults to production
// (the bare route); "draft" selects the @draft route. Anything else is a
// deterministic local error — never a silent fallback to production.
func normalizeRenderChannel(channel string) (string, error) {
	switch channel {
	case "", "production":
		return "production", nil
	case "draft":
		return "draft", nil
	default:
		return "", fmt.Errorf("invalid --channel %q: want draft or production", channel)
	}
}

// encodeRenderParams turns the repeatable --param k=v pairs into a sorted,
// URL-escaped query string. Repeated keys are last-wins (ctx.params is a map,
// spec §6.5). Reserved names (token, block) are left for the worker to strip
// (§6.5) — the CLI does not special-case them.
func encodeRenderParams(params []string) (string, error) {
	vals := url.Values{}
	for _, p := range params {
		k, v, ok := strings.Cut(p, "=")
		if !ok {
			return "", fmt.Errorf("invalid --param %q: want key=value", p)
		}
		if k == "" {
			return "", fmt.Errorf("invalid --param %q: empty key", p)
		}
		vals.Set(k, v)
	}
	return vals.Encode(), nil
}

// appsRenderURL builds the render URL: {remote}/apps/{pid}/{name} for
// production, {remote}/apps/{pid}/{name}@draft for draft, plus the encoded
// query. The @draft marker is appended AFTER escaping {name} so it stays a
// literal path delimiter (the worker splits {name} on it, §4.1/W6) — running
// "name@draft" through url.PathEscape would encode '@' to %40 and break the
// split. resolved.Remote is the spec §4.1 ingress host every other subcommand
// already resolves — no separate app-worker URL, no new hostname (C0 §5b).
func appsRenderURL(remote, projectID, name, channel, query string) (string, error) {
	if err := validateAppName(name); err != nil {
		return "", err
	}
	base := strings.TrimRight(remote, "/") + "/apps/" + url.PathEscape(projectID) + "/" + url.PathEscape(name)
	if channel == "draft" {
		base += "@draft"
	}
	if query != "" {
		base += "?" + query
	}
	return base, nil
}

// fetchAuthorLog fetches the render-log record matching requestID from
// pipeline-api's author logs route, for the render-failure combined object.
// Best-effort: an empty requestID, a 404 (aged out of the ring buffer / never
// existed), an invalid-JSON body, or any transport hiccup yields a nil
// RawMessage → author_log:null (spec §5.5). It never turns a render failure
// into a transport failure.
func fetchAuthorLog(ctx context.Context, remote, apiToken, projectID, name, requestID string) json.RawMessage {
	if requestID == "" {
		return nil
	}
	logsURL, err := appsURL(remote, projectID, name, "logs")
	if err != nil {
		return nil
	}
	logsURL += "?request_id=" + url.QueryEscape(requestID)
	status, body, err := doAuthedRequest(ctx, http.MethodGet, logsURL, apiToken, "", nil)
	if err != nil || status != http.StatusOK || !json.Valid(body) {
		return nil
	}
	return json.RawMessage(body)
}

// formatRenderFailureText writes the human-readable render-failure block to w
// (stderr in text mode, spec §5.5): the envelope fields plus the author log's
// excerpt (outcome/error/log_text), or a note when no author log was found.
func formatRenderFailureText(w io.Writer, env appsRenderEnvelope, authorLog json.RawMessage) {
	fmt.Fprintln(w, "render failed")
	fmt.Fprintf(w, "  kind:       %s\n", env.Kind)
	fmt.Fprintf(w, "  request_id: %s\n", env.RequestID)
	fmt.Fprintf(w, "  error:      %s\n", env.Error)
	if len(authorLog) == 0 || string(authorLog) == "null" {
		fmt.Fprintf(w, "  author log: (none for request_id %s)\n", env.RequestID)
		return
	}
	var rec appsRenderLogRecord
	if err := json.Unmarshal(authorLog, &rec); err != nil {
		fmt.Fprintf(w, "  author log: %s\n", string(authorLog))
		return
	}
	fmt.Fprintln(w, "  author log:")
	if rec.Outcome != "" {
		fmt.Fprintf(w, "    outcome:  %s\n", rec.Outcome)
	}
	if rec.Error != "" {
		fmt.Fprintf(w, "    error:    %s\n", rec.Error)
	}
	if rec.LogText != "" {
		fmt.Fprintf(w, "    log:\n%s\n", indentLines(rec.LogText, "      "))
	}
}

// truncateForError bounds a server body echoed into an error message so a
// large/HTML error page doesn't flood the terminal.
func truncateForError(s string) string {
	s = strings.TrimSpace(s)
	const max = 500
	if len(s) > max {
		return s[:max] + "... (truncated)"
	}
	return s
}

// indentLines prefixes every line of s with prefix (for nested log excerpts).
func indentLines(s, prefix string) string {
	lines := strings.Split(strings.TrimRight(s, "\n"), "\n")
	for i, ln := range lines {
		lines[i] = prefix + ln
	}
	return strings.Join(lines, "\n")
}

// ---------------------------------------------------------------------------
// logs — author-facing render logs on pipeline-api (spec §6.6). Powers the
// render-failure diagnostics loop (`apps render` fetches from here too).
// ---------------------------------------------------------------------------

// runAppsLogs implements `datuplet apps logs <name> [--request-id <id>] [--json]`.
// Hits pipeline-api's author route GET /api/v1/projects/{pid}/apps/{name}/logs.
// Without --request-id it lists the recent render-log records (time, outcome,
// duration); with one it prints that single record, or exits 1 with "not
// found" when the author route 404s (aged out of the ring buffer or never
// existed, §6.6).
func runAppsLogs(args []string) error {
	positional, remote, tokenFile, project, _, _, requestID, _, asJSON, err := parseAppsFlags(args)
	if err != nil {
		return err
	}
	if len(positional) != 1 {
		return fmt.Errorf("usage: datuplet apps logs <name> [--request-id <id>] [--json]")
	}
	name := positional[0]
	if err := validateAppName(name); err != nil {
		return err
	}

	resolved, err := loadRemoteArgs(remote, tokenFile, project)
	if err != nil {
		return err
	}
	if err := resolved.RequireAPIToken(); err != nil {
		return err
	}

	logsURL, err := appsURL(resolved.Remote, resolved.ID, name, "logs")
	if err != nil {
		return err
	}
	if requestID != "" {
		logsURL += "?request_id=" + url.QueryEscape(requestID)
	}
	status, body, err := doAuthedRequest(context.Background(),
		http.MethodGet, logsURL, resolved.APIToken, "", nil)
	if err != nil {
		return err
	}
	if requestID != "" && status == http.StatusNotFound {
		return fmt.Errorf("no render log for request_id %q (not found)", requestID)
	}
	if status != http.StatusOK {
		return fmt.Errorf("apps logs: HTTP %d: %s", status, truncateForError(string(body)))
	}

	if asJSON {
		fmt.Println(string(body))
		return nil
	}

	// Text mode. A single record (by request_id) prints as a field block; the
	// list prints as a compact table.
	if requestID != "" {
		var rec appsRenderLogRecord
		if err := json.Unmarshal(body, &rec); err != nil {
			return fmt.Errorf("decode logs response: %w", err)
		}
		printRenderLogRecord(os.Stdout, rec)
		return nil
	}
	var recs []appsRenderLogRecord
	if err := json.Unmarshal(body, &recs); err != nil {
		return fmt.Errorf("decode logs response: %w", err)
	}
	if len(recs) == 0 {
		fmt.Println("(no render logs)")
		return nil
	}
	fmt.Printf("%-25s %-14s %-12s %-11s %s\n", "STARTED_AT", "OUTCOME", "DURATION_MS", "CHANNEL", "REQUEST_ID")
	for _, r := range recs {
		fmt.Printf("%-25s %-14s %-12d %-11s %s\n", r.StartedAt, r.Outcome, r.DurationMS, r.Channel, r.RequestID)
	}
	return nil
}

// printRenderLogRecord renders one render-log record as a human field block.
func printRenderLogRecord(w io.Writer, r appsRenderLogRecord) {
	fmt.Fprintf(w, "Request ID:   %s\n", r.RequestID)
	fmt.Fprintf(w, "Outcome:      %s\n", r.Outcome)
	fmt.Fprintf(w, "Started:      %s\n", r.StartedAt)
	fmt.Fprintf(w, "Duration ms:  %d\n", r.DurationMS)
	fmt.Fprintf(w, "Channel:      %s\n", r.Channel)
	fmt.Fprintf(w, "Version:      %s\n", r.VersionHash)
	if r.Error != "" {
		fmt.Fprintf(w, "Error:        %s\n", r.Error)
	}
	if r.LogText != "" {
		fmt.Fprintf(w, "Log:\n%s\n", indentLines(r.LogText, "  "))
	}
}
