package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
)

// componentIOJSON mirrors the catalog's io capability object
// (pkg/pipelineapi/http/component_handlers.go). Always present with
// resolved (never-empty) mode strings: "none" | "optional" | "required".
type componentIOJSON struct {
	Inputs  string `json:"inputs"`
	Outputs string `json:"outputs"`
}

// componentSummaryVersionJSON is one entry of componentSummaryJSON.Versions
// — the list/catalog-picker shape. Deliberately has no ConfigSchema: only
// the per-component detail endpoint carries that (see versionJSON below).
type componentSummaryVersionJSON struct {
	Version    string `json:"version"`
	Prerelease bool   `json:"prerelease"`
	Image      string `json:"image"`
}

// componentSummaryJSON mirrors one entry of GET /api/v1/components.
type componentSummaryJSON struct {
	Name           string                        `json:"name"`
	DisplayName    string                        `json:"displayName"`
	Description    string                        `json:"description"`
	Deprecated     bool                          `json:"deprecated"`
	DefaultVersion string                        `json:"defaultVersion"`
	IO             componentIOJSON               `json:"io"`
	Versions       []componentSummaryVersionJSON `json:"versions"`
}

// versionJSON mirrors componentDetailVersionJSON server-side — adds
// ConfigSchema, only ever populated by the per-component detail endpoint.
// resolveVersion resolves to one of these.
type versionJSON struct {
	Version      string `json:"version"`
	Prerelease   bool   `json:"prerelease"`
	Image        string `json:"image"`
	ConfigSchema string `json:"configSchema"`
}

// componentDetailJSON mirrors the body of GET /api/v1/components/{name}.
type componentDetailJSON struct {
	Name           string          `json:"name"`
	DisplayName    string          `json:"displayName"`
	Description    string          `json:"description"`
	Deprecated     bool            `json:"deprecated"`
	DefaultVersion string          `json:"defaultVersion"`
	IO             componentIOJSON `json:"io"`
	Versions       []versionJSON   `json:"versions"`
}

// stableVersionPattern mirrors pkg/k8s/api/v1/component_types.go's
// stableVersionPattern exactly (vMAJOR.MINOR.PATCH only — no bare "1.2.3",
// no prerelease suffix). Kept in sync manually; the CLI never talks to
// that package directly, it only needs the same matching rule to resolve
// "highest stable semver" the same way the server does.
var stableVersionPattern = regexp.MustCompile(`^v(\d+)\.(\d+)\.(\d+)$`)

// parseStableVersion extracts (major, minor, patch) from a stable semver
// string. ok is false for anything stableVersionPattern rejects.
func parseStableVersion(v string) (major, minor, patch int, ok bool) {
	m := stableVersionPattern.FindStringSubmatch(v)
	if m == nil {
		return 0, 0, 0, false
	}
	major, _ = strconv.Atoi(m[1])
	minor, _ = strconv.Atoi(m[2])
	patch, _ = strconv.Atoi(m[3])
	return major, minor, patch, true
}

// resolveVersion picks the version `datuplet components get --schema` (and
// plain get --version) resolves to. Pure — no I/O — so it's testable
// directly. Precedence (spec §7):
//  1. want (--version), if non-empty — error if not found in c.Versions.
//  2. c.DefaultVersion, if set — error if it's not found in c.Versions (a
//     catalog whose defaultVersion doesn't resolve is a server-side bug
//     worth surfacing, not silently overriding).
//  3. The highest stable semver among c.Versions (prerelease entries
//     excluded) — error if none are stable.
func resolveVersion(c componentDetailJSON, want string) (versionJSON, error) {
	if want != "" {
		for _, v := range c.Versions {
			if v.Version == want {
				return v, nil
			}
		}
		return versionJSON{}, fmt.Errorf("version %q not found for component %q", want, c.Name)
	}
	if c.DefaultVersion != "" {
		for _, v := range c.Versions {
			if v.Version == c.DefaultVersion {
				return v, nil
			}
		}
		return versionJSON{}, fmt.Errorf("defaultVersion %q not found among versions for component %q", c.DefaultVersion, c.Name)
	}
	var (
		best                            versionJSON
		bestMajor, bestMinor, bestPatch int
		found                           bool
	)
	for _, v := range c.Versions {
		if v.Prerelease {
			continue
		}
		major, minor, patch, ok := parseStableVersion(v.Version)
		if !ok {
			continue
		}
		if !found ||
			major > bestMajor ||
			(major == bestMajor && minor > bestMinor) ||
			(major == bestMajor && minor == bestMinor && patch > bestPatch) {
			best, bestMajor, bestMinor, bestPatch, found = v, major, minor, patch, true
		}
	}
	if !found {
		return versionJSON{}, fmt.Errorf("no stable version found for component %q (all versions are prerelease or non-semver)", c.Name)
	}
	return best, nil
}

// componentsURL composes the /api/v1/components[/{name}] URL. This
// endpoint is NOT project-scoped (spec §4.7 — shared catalog, open to any
// authenticated user), unlike pipelineURL.
func componentsURL(remote, name string) string {
	base := strings.TrimRight(remote, "/") + "/api/v1/components"
	if name != "" {
		base += "/" + url.PathEscape(name)
	}
	return base
}

// fetchComponentsList calls GET /api/v1/components and returns both the
// raw body (for --json passthrough) and the decoded items (for the table
// view). Split out from runComponentsList so it's testable directly
// against an httptest.Server without going through os.Stdout.
func fetchComponentsList(ctx context.Context, remote, apiToken string) (body []byte, items []componentSummaryJSON, err error) {
	status, body, err := doAuthedRequest(ctx, http.MethodGet, componentsURL(remote, ""), apiToken, nil)
	if err != nil {
		return nil, nil, err
	}
	if status != http.StatusOK {
		return nil, nil, fmt.Errorf("list components: HTTP %d: %s", status, string(body))
	}
	if err := json.Unmarshal(body, &items); err != nil {
		return nil, nil, fmt.Errorf("decode list response: %w", err)
	}
	return body, items, nil
}

// fetchComponentDetail calls GET /api/v1/components/{name}. Returns a
// friendly error for 404 (component not found) instead of a raw status
// code. Split out from runComponentsGet for the same reason as
// fetchComponentsList above.
func fetchComponentDetail(ctx context.Context, remote, apiToken, name string) (body []byte, detail componentDetailJSON, err error) {
	status, body, err := doAuthedRequest(ctx, http.MethodGet, componentsURL(remote, name), apiToken, nil)
	if err != nil {
		return nil, componentDetailJSON{}, err
	}
	if status == http.StatusNotFound {
		return nil, componentDetailJSON{}, fmt.Errorf("component %q not found", name)
	}
	if status != http.StatusOK {
		return nil, componentDetailJSON{}, fmt.Errorf("get component: HTTP %d: %s", status, string(body))
	}
	if err := json.Unmarshal(body, &detail); err != nil {
		return nil, componentDetailJSON{}, fmt.Errorf("decode get response: %w", err)
	}
	return body, detail, nil
}

// runComponents dispatches `datuplet components <sub> ...`.
//
// Subcommands (mirror the catalog's read surface — spec §7):
//   - list: GET /api/v1/components         (no configSchema)
//   - get:  GET /api/v1/components/{name}  (configSchema per version)
//
// Both require an api-token, same as pipeline/trigger/storage. This
// endpoint is not project-scoped, so unlike those commands there is no
// --project flag here.
func runComponents(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: datuplet components <list|get> [args]\n%s", componentsHelpText())
	}
	sub := args[0]
	rest := args[1:]
	switch sub {
	case "list", "ls":
		return runComponentsList(rest)
	case "get", "show":
		return runComponentsGet(rest)
	case "help", "-h", "--help":
		fmt.Println(componentsHelpText())
		return nil
	default:
		return fmt.Errorf("unknown components subcommand %q\n%s", sub, componentsHelpText())
	}
}

func componentsHelpText() string {
	return `components subcommands:
  list                            list the component catalog
  get <name>                      show one component's detail

common flags:
  --remote <url>     pipeline-api URL (defaults to logged-in cluster)
  --token-file <p>   override default ~/.datuplet/token path
  --json             emit JSON output (list, get)

get-only flags:
  --version <v>      resolve a specific version (default: registry
                      defaultVersion, else the highest stable semver)
  --schema            print the resolved version's configSchema verbatim
                      (always uses the detail endpoint)

examples:
  datuplet components list --json
  datuplet components get data-generator --schema
  datuplet components get sql-transform --version v0.9.1 --json
`
}

// parseComponentsFlags extracts --remote / --token-file / --version /
// --json / --schema from an arbitrary positional slice.
//
// Hand-rolled to match parsePipelineFlags's convention (flags in any
// order, single positional remains) — see cmd/datuplet/pipeline.go.
func parseComponentsFlags(args []string) (positional []string, remote, tokenFile, version string, asJSON, asSchema bool, err error) {
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
		case a == "--version":
			if i+1 >= len(args) {
				err = fmt.Errorf("--version requires a value")
				return
			}
			version = args[i+1]
			i += 2
		case strings.HasPrefix(a, "--version="):
			version = strings.TrimPrefix(a, "--version=")
			i++
		case a == "--json":
			asJSON = true
			i++
		case a == "--schema":
			asSchema = true
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

// runComponentsList implements `datuplet components list`.
func runComponentsList(args []string) error {
	positional, remote, tokenFile, _, asJSON, asSchema, err := parseComponentsFlags(args)
	if err != nil {
		return err
	}
	if asSchema {
		return fmt.Errorf("list does not support --schema (use 'datuplet components get <name> --schema')")
	}
	if len(positional) > 0 {
		return fmt.Errorf("list takes no positional args; got %q", positional)
	}

	resolved, err := loadRemoteArgs(remote, tokenFile, "")
	if err != nil {
		return err
	}
	if err := resolved.RequireAPIToken(); err != nil {
		return err
	}

	body, items, err := fetchComponentsList(context.Background(), resolved.Remote, resolved.APIToken)
	if err != nil {
		return err
	}

	if asJSON {
		fmt.Println(string(body))
		return nil
	}

	if len(items) == 0 {
		fmt.Println("(no components)")
		return nil
	}
	fmt.Printf("%-24s %-24s %-12s %-18s %s\n", "NAME", "DISPLAY", "DEFAULT", "IO", "DEPRECATED")
	for _, c := range items {
		fmt.Printf("%-24s %-24s %-12s %-18s %v\n",
			c.Name, c.DisplayName, c.DefaultVersion,
			c.IO.Inputs+"/"+c.IO.Outputs, c.Deprecated)
	}
	return nil
}

// runComponentsGet implements `datuplet components get <name>`. `get` and
// `--schema` always call the detail endpoint (never list — only detail
// carries configSchema).
func runComponentsGet(args []string) error {
	positional, remote, tokenFile, version, asJSON, asSchema, err := parseComponentsFlags(args)
	if err != nil {
		return err
	}
	if asJSON && asSchema {
		return fmt.Errorf("--json and --schema are mutually exclusive")
	}
	if len(positional) != 1 {
		return fmt.Errorf("usage: datuplet components get <name> [--version v] [--json|--schema]")
	}
	name := positional[0]

	resolved, err := loadRemoteArgs(remote, tokenFile, "")
	if err != nil {
		return err
	}
	if err := resolved.RequireAPIToken(); err != nil {
		return err
	}

	body, detail, err := fetchComponentDetail(context.Background(), resolved.Remote, resolved.APIToken, name)
	if err != nil {
		return err
	}

	if asJSON {
		fmt.Println(string(body))
		return nil
	}

	if asSchema {
		v, err := resolveVersion(detail, version)
		if err != nil {
			return err
		}
		// Verbatim + trailing newline — nothing else (spec §7).
		fmt.Println(v.ConfigSchema)
		return nil
	}

	// --version without --schema: validate it resolves (nicer error than
	// silently ignoring an unknown version), but the default view below
	// still lists every version regardless.
	if version != "" {
		if _, err := resolveVersion(detail, version); err != nil {
			return err
		}
	}

	fmt.Printf("Name:        %s\n", detail.Name)
	fmt.Printf("Display:     %s\n", detail.DisplayName)
	fmt.Printf("Description: %s\n", detail.Description)
	fmt.Printf("Deprecated:  %v\n", detail.Deprecated)
	fmt.Printf("Default:     %s\n", detail.DefaultVersion)
	fmt.Printf("IO:          inputs=%s outputs=%s\n", detail.IO.Inputs, detail.IO.Outputs)
	fmt.Println()
	fmt.Printf("%-16s %-12s %s\n", "VERSION", "PRERELEASE", "IMAGE")
	for _, v := range detail.Versions {
		fmt.Printf("%-16s %-12v %s\n", v.Version, v.Prerelease, v.Image)
	}
	return nil
}
