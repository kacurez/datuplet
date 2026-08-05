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
// Subcommands implemented here (spec §5.5's CLI table, C1 slice):
//   - init:   scaffold a new app directory locally (no network, no --project)
//   - put:    PUT    /api/v1/projects/{pid}/apps/{name}   (upload -> draft)
//   - get:    GET    /api/v1/projects/{pid}/apps/{name}
//   - list:   GET    /api/v1/projects/{pid}/apps
//   - delete: DELETE /api/v1/projects/{pid}/apps/{name}
//
// render/promote/logs/token land in a follow-up task against the same
// dispatcher and parseAppsFlags (see the doc comment there).
func runApps(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: datuplet apps <init|put|get|list|delete> [args]\n%s", appsHelpText())
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

common flags:
  --remote <url>     pipeline-api URL (defaults to logged-in cluster)
  --token-file <p>   override default ~/.datuplet/token path
  --project <name>   project to operate in (falls back to $DATUPLET_PROJECT,
                      then the logged-in cluster's default)
  --json             emit JSON output (put, get, list)

put-only flags:
  --bundle <file>    path to the built IIFE bundle (see 'datuplet apps init'
                      and its esbuild.mjs); rejected locally above 5 MB

examples:
  datuplet apps init ./sales-overview
  cd sales-overview && npm install && npm run build
  datuplet apps put sales-overview --project myproj --bundle bundle.js
  datuplet apps get sales-overview --project myproj --json
  datuplet apps list --project myproj
  datuplet apps delete sales-overview --project myproj
`
}

// parseAppsFlags extracts the flags common to every `datuplet apps`
// subcommand from an arbitrary positional slice, following
// parsePipelineFlags's/parseComponentsFlags's hand-rolled convention (flags
// in any order, a single positional-args slice remains).
//
// A follow-up task extends this for render/promote/logs/token's additional
// flags (--channel, --param, repeatable; --version; --request-id;
// --expected-production) rather than introducing a second parser — add
// fields and cases here, thread the new return values through, and every
// existing call site keeps compiling (Go requires updating them, which is
// the point: one flag grammar for the whole `apps` family).
func parseAppsFlags(args []string) (positional []string, remote, tokenFile, project, bundle string, asJSON bool, err error) {
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
	positional, remote, tokenFile, project, bundlePath, asJSON, err := parseAppsFlags(args)
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
	positional, remote, tokenFile, project, _, asJSON, err := parseAppsFlags(args)
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
	positional, remote, tokenFile, project, _, asJSON, err := parseAppsFlags(args)
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
	positional, remote, tokenFile, project, _, _, err := parseAppsFlags(args)
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
