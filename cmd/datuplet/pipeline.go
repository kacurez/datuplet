package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// pipelineHTTPClient mirrors triggerHTTPClient — 30 s per-request cap
// on top of context cancellation. Pipeline CRUD is fast (no
// long-running operations), so this is a defensive ceiling for hung
// servers.
var pipelineHTTPClient = &http.Client{Timeout: 30 * time.Second}

// pipelineDetailJSON mirrors pkg/pipelineapi/http/pipeline_handlers.go.
// Kept in sync manually — the API never breaks this shape (we'd notice
// in CLI tests). Doc is the raw canonical-JSON PipelineDoc (RFC 027 §5.1);
// GET ?format=yaml renders it as YAML instead (S6) — see runPipelineGet.
type pipelineDetailJSON struct {
	ID        string          `json:"id"`
	Name      string          `json:"name"`
	Doc       json.RawMessage `json:"doc"`
	CreatedAt string          `json:"created_at"`
	UpdatedAt string          `json:"updated_at"`
}

// pipelineRefJSON mirrors the list-item shape from the API. Description is
// the doc's top-level description (RFC 027 §5.2, S6); older servers may omit
// it, which decodes to "" here. CreatedAt / UpdatedAt are omitempty
// server-side (local-file mode doesn't stat).
type pipelineRefJSON struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	CreatedAt   string `json:"created_at,omitempty"`
	UpdatedAt   string `json:"updated_at,omitempty"`
}

// runPipeline dispatches `datuplet pipeline <sub> ...`.
//
// Subcommands (mirror the API's CRUD surface — see
// pkg/pipelineapi/http/server.go):
//   - list:   GET    /api/v1/projects/{pid}/pipelines
//   - get:    GET    /api/v1/projects/{pid}/pipelines/{name}
//   - put:    PUT    /api/v1/projects/{pid}/pipelines/{name}  (create OR update)
//   - delete: DELETE /api/v1/projects/{pid}/pipelines/{name}
//
// All require an api-token (from ~/.datuplet/api-token), the same
// token the trigger + storage subcommands use. Authentication errors
// surface with a clean "run datuplet login" message via
// resolved.RequireAPIToken.
func runPipeline(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: datuplet pipeline <list|get|put|delete> [args]\n%s", pipelineHelpText())
	}
	sub := args[0]
	rest := args[1:]
	switch sub {
	case "list", "ls":
		return runPipelineList(rest)
	case "get", "show":
		return runPipelineGet(rest)
	case "put", "apply", "upsert":
		return runPipelinePut(rest)
	case "delete", "del", "rm":
		return runPipelineDelete(rest)
	case "validate":
		return runPipelineValidate(rest)
	case "help", "-h", "--help":
		fmt.Println(pipelineHelpText())
		return nil
	default:
		return fmt.Errorf("unknown pipeline subcommand %q\n%s", sub, pipelineHelpText())
	}
}

func pipelineHelpText() string {
	return `pipeline subcommands:
  list                            list pipelines in the current project
  get <name>                      print one pipeline's YAML
  put [<name>] -f <file>          create-or-update from YAML/JSON file
                                  (name is optional — defaults to the doc's top-level name)
  validate -f <file> [--name <n>] validate a doc without persisting
                                  (--name engages the update-mode resource-gate diff)
  delete <name>                   delete a pipeline (prompts unless -y)

common flags:
  --remote <url>     pipeline-api URL (defaults to logged-in cluster)
  --token-file <p>   override default ~/.datuplet/token path
  --project <name>   project to operate in (defaults to first project)
  --json             emit JSON output (list, get, validate)
  -f, --file <path>  read pipeline body from a YAML/JSON file ('-' for stdin)
  --name <n>         validate only: diff against this stored pipeline
  -y, --yes          skip the interactive confirmation on delete

exit codes (validate only):
  0   no error-severity finding (warnings still print)
  1   at least one error-severity finding
  2+  transport/HTTP failure — the validate request itself failed

examples:
  datuplet pipeline list --json
  datuplet pipeline get gen-big-pipeline > backup.yaml
  datuplet pipeline put -f gen-big-pipeline.yaml
  datuplet pipeline validate -f gen-big-pipeline.yaml
  datuplet pipeline validate -f gen-big-pipeline.yaml --name gen-big-pipeline --json
  datuplet pipeline delete gen-big-pipeline -y
`
}

// parsePipelineFlags extracts the standard --remote / --token-file /
// --project / -f / --json / -y / --name flags from an arbitrary positional
// slice. --name is only meaningful to `validate` (engages its update-mode
// resource-gate diff — spec §5.2/§7); other subcommands simply discard it.
// Returns (positionalArgs, remote, tokenFile, project, file, name, asJSON, yes, err).
//
// Hand-rolled instead of flag.NewFlagSet because the CLI's other
// commands all do their own parsing for consistency (trigger.go,
// storage.go) and we want the same UX: flags in any order, single
// positional remains.
func parsePipelineFlags(args []string) (positional []string, remote, tokenFile, project, file, name string, asJSON, yes bool, err error) {
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
		case a == "-f" || a == "--file":
			if i+1 >= len(args) {
				err = fmt.Errorf("%s requires a value", a)
				return
			}
			file = args[i+1]
			i += 2
		case strings.HasPrefix(a, "--file="):
			file = strings.TrimPrefix(a, "--file=")
			i++
		case a == "--name":
			if i+1 >= len(args) {
				err = fmt.Errorf("--name requires a value")
				return
			}
			name = args[i+1]
			i += 2
		case strings.HasPrefix(a, "--name="):
			name = strings.TrimPrefix(a, "--name=")
			i++
		case a == "--json":
			asJSON = true
			i++
		case a == "-y" || a == "--yes":
			yes = true
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

// pipelineURL composes the /api/v1/projects/{pid}/pipelines[/{name}]
// URL. `name` may be empty for list. URL-escapes both segments.
func pipelineURL(remote, projectID, name string) string {
	base := strings.TrimRight(remote, "/") + "/api/v1/projects/" + url.PathEscape(projectID) + "/pipelines"
	if name != "" {
		base += "/" + url.PathEscape(name)
	}
	return base
}

// doAuthedRequest issues an HTTP request with Authorization: Bearer
// <apiToken> and returns body bytes + status. Used by all four
// CRUD ops here so error handling (status check, bounded read) lives
// in one place. contentType is only set on the request when body != nil;
// callers with no body (GET/DELETE) may pass "". A body with an empty
// contentType defaults to "application/yaml" (the server treats anything
// that isn't exactly "application/json" as YAML — see pipeline_handlers.go).
func doAuthedRequest(ctx context.Context, method, urlStr, apiToken, contentType string, body io.Reader) (status int, respBody []byte, err error) {
	req, err := http.NewRequestWithContext(ctx, method, urlStr, body)
	if err != nil {
		return 0, nil, fmt.Errorf("build %s %s: %w", method, urlStr, err)
	}
	req.Header.Set("Authorization", "Bearer "+apiToken)
	if body != nil {
		if contentType == "" {
			contentType = "application/yaml"
		}
		req.Header.Set("Content-Type", contentType)
	}
	resp, err := pipelineHTTPClient.Do(req)
	if err != nil {
		return 0, nil, fmt.Errorf("%s %s: %w", method, urlStr, err)
	}
	defer resp.Body.Close()
	// Bound the response so a misbehaving server can't OOM the CLI.
	// 4 MiB is generous for any pipeline body we'd reasonably store.
	respBody, err = io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return resp.StatusCode, nil, fmt.Errorf("read response: %w", err)
	}
	return resp.StatusCode, respBody, nil
}

// runPipelineList implements `datuplet pipeline list`.
func runPipelineList(args []string) error {
	positional, remote, tokenFile, project, _, _, asJSON, _, err := parsePipelineFlags(args)
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

	status, body, err := doAuthedRequest(context.Background(),
		http.MethodGet, pipelineURL(resolved.Remote, resolved.ID, ""),
		resolved.APIToken, "", nil)
	if err != nil {
		return err
	}
	if status != http.StatusOK {
		return fmt.Errorf("list pipelines: HTTP %d: %s", status, string(body))
	}

	if asJSON {
		// Pass through the server's JSON verbatim.
		fmt.Print(string(body))
		return nil
	}

	var items []pipelineRefJSON
	if err := json.Unmarshal(body, &items); err != nil {
		return fmt.Errorf("decode list response: %w", err)
	}
	if len(items) == 0 {
		fmt.Println("(no pipelines)")
		return nil
	}
	// Render a DESCRIPTION column only when at least one item has a
	// non-empty description — older servers omit the field entirely
	// (decodes to "" here), so this keeps the table clean for them.
	hasDescription := false
	for _, p := range items {
		if p.Description != "" {
			hasDescription = true
			break
		}
	}
	if hasDescription {
		fmt.Printf("%-40s %-40s %s\n", "NAME", "DESCRIPTION", "UPDATED")
		for _, p := range items {
			stamp := p.UpdatedAt
			if stamp == "" {
				stamp = p.CreatedAt
			}
			fmt.Printf("%-40s %-40s %s\n", p.Name, p.Description, stamp)
		}
		return nil
	}
	// Simple two-column table — name + updated_at (or created_at fallback).
	// Local-mode responses may omit timestamps entirely.
	fmt.Printf("%-40s %s\n", "NAME", "UPDATED")
	for _, p := range items {
		stamp := p.UpdatedAt
		if stamp == "" {
			stamp = p.CreatedAt
		}
		fmt.Printf("%-40s %s\n", p.Name, stamp)
	}
	return nil
}

// runPipelineGet implements `datuplet pipeline get <name>`. Default output
// hits the server's `?format=yaml` rendering (S6) and prints the response
// body verbatim, so callers can redirect straight to a file. `--json` hits
// the plain detail endpoint instead and prints its JSON body verbatim
// (including the `doc` field as a JSON object — never re-serialized).
func runPipelineGet(args []string) error {
	positional, remote, tokenFile, project, _, _, asJSON, _, err := parsePipelineFlags(args)
	if err != nil {
		return err
	}
	if len(positional) != 1 {
		return fmt.Errorf("usage: datuplet pipeline get <name>")
	}
	name := positional[0]

	resolved, err := loadRemoteArgs(remote, tokenFile, project)
	if err != nil {
		return err
	}
	if err := resolved.RequireAPIToken(); err != nil {
		return err
	}

	getURL := pipelineURL(resolved.Remote, resolved.ID, name)
	if !asJSON {
		getURL += "?format=yaml"
	}
	status, body, err := doAuthedRequest(context.Background(),
		http.MethodGet, getURL, resolved.APIToken, "", nil)
	if err != nil {
		return err
	}
	if status == http.StatusNotFound {
		return fmt.Errorf("pipeline %q not found in project %q", name, resolved.ProjectName)
	}
	if status != http.StatusOK {
		return fmt.Errorf("get pipeline: HTTP %d: %s", status, string(body))
	}

	if asJSON {
		// Pass through the server's detail JSON verbatim.
		fmt.Print(string(body))
		return nil
	}
	// Default view: the server's deterministic YAML rendering, verbatim.
	fmt.Print(string(body))
	return nil
}

// runPipelinePut implements `datuplet pipeline put [<name>] -f <file>`.
// When the positional <name> is omitted, falls back to the doc's
// top-level name. This makes simple "apply this file" usage one
// argument shorter and matches the kubectl-style ergonomic.
func runPipelinePut(args []string) error {
	positional, remote, tokenFile, project, file, _, _, _, err := parsePipelineFlags(args)
	if err != nil {
		return err
	}
	if file == "" {
		return fmt.Errorf("put: -f <file> is required (use '-' for stdin)\nusage: datuplet pipeline put [<name>] -f <file>")
	}
	if len(positional) > 1 {
		return fmt.Errorf("put takes at most one positional arg (name); got %q", positional)
	}

	body, err := readFileOrStdin(file)
	if err != nil {
		return err
	}

	// Resolve the name. Precedence: explicit positional > the doc's
	// top-level name. The API enforces they match; failing here gives a
	// nicer error than the server's 400 — and does so before any HTTP
	// call is made.
	docName, _ := extractDocName(body)
	var name string
	switch {
	case len(positional) == 1 && docName != "" && positional[0] != docName:
		return fmt.Errorf("pipeline name mismatch: arg=%q vs doc name=%q\n(omit the positional to use the doc's name, or update one to match the other)",
			positional[0], docName)
	case len(positional) == 1:
		name = positional[0]
	case docName != "":
		name = docName
	default:
		return fmt.Errorf("put: pipeline name required — pass it as the first positional or set name in the doc")
	}

	resolved, err := loadRemoteArgs(remote, tokenFile, project)
	if err != nil {
		return err
	}
	if err := resolved.RequireAPIToken(); err != nil {
		return err
	}

	status, respBody, err := doAuthedRequest(context.Background(),
		http.MethodPut, pipelineURL(resolved.Remote, resolved.ID, name),
		resolved.APIToken, sniffContentType(body), bytes.NewReader(body))
	if err != nil {
		return err
	}
	switch status {
	case http.StatusNoContent:
		fmt.Fprintf(os.Stderr, "pipeline %q upserted in project %q\n", name, resolved.ProjectName)
		return nil
	case http.StatusBadRequest:
		return fmt.Errorf("put pipeline: invalid request: %s", string(respBody))
	case http.StatusRequestEntityTooLarge:
		return fmt.Errorf("put pipeline: body exceeds API's 1 MiB cap")
	default:
		return fmt.Errorf("put pipeline: HTTP %d: %s", status, string(respBody))
	}
}

// runPipelineDelete implements `datuplet pipeline delete <name>`.
// Prompts for confirmation unless --yes is set, so an accidental
// fat-finger doesn't blow away a pipeline. Honors the API's 409
// (active runs) path with a clear hint.
func runPipelineDelete(args []string) error {
	positional, remote, tokenFile, project, _, _, _, yes, err := parsePipelineFlags(args)
	if err != nil {
		return err
	}
	if len(positional) != 1 {
		return fmt.Errorf("usage: datuplet pipeline delete <name>")
	}
	name := positional[0]

	resolved, err := loadRemoteArgs(remote, tokenFile, project)
	if err != nil {
		return err
	}
	if err := resolved.RequireAPIToken(); err != nil {
		return err
	}

	if !yes {
		fmt.Fprintf(os.Stderr, "delete pipeline %q from project %q? [y/N] ", name, resolved.ProjectName)
		reader := bufio.NewReader(os.Stdin)
		line, _ := reader.ReadString('\n')
		switch strings.ToLower(strings.TrimSpace(line)) {
		case "y", "yes":
			// proceed
		default:
			return fmt.Errorf("aborted")
		}
	}

	status, body, err := doAuthedRequest(context.Background(),
		http.MethodDelete, pipelineURL(resolved.Remote, resolved.ID, name),
		resolved.APIToken, "", nil)
	if err != nil {
		return err
	}
	switch status {
	case http.StatusNoContent, http.StatusOK:
		fmt.Fprintf(os.Stderr, "pipeline %q deleted\n", name)
		return nil
	case http.StatusNotFound:
		return fmt.Errorf("pipeline %q not found in project %q", name, resolved.ProjectName)
	case http.StatusConflict:
		return fmt.Errorf("delete pipeline: pipeline has active runs — cancel them first")
	default:
		return fmt.Errorf("delete pipeline: HTTP %d: %s", status, string(body))
	}
}

// exitCodeErr lets a subcommand's error carry a specific process exit code,
// overriding the default 1 that main.go uses for any returned error.
// validate uses this to distinguish "the pipeline is invalid" (plain error,
// default exit 1) from "the validate request itself failed" (exit >=2) —
// spec §7's "0/1 from findings, ≥2 from transport" contract. main.go's
// "pipeline" dispatch unwraps this via errors.As.
type exitCodeErr struct {
	code int
	err  error
}

func (e *exitCodeErr) Error() string { return e.err.Error() }
func (e *exitCodeErr) Unwrap() error { return e.err }

// validateFinding mirrors pkg/pipeline/validate.Finding's JSON shape —
// kept as a CLI-local copy rather than importing the validate package,
// matching this file's existing convention of hand-mirroring server
// response shapes (pipelineDetailJSON, pipelineRefJSON above).
type validateFinding struct {
	Path     string `json:"path"`
	Message  string `json:"message"`
	Severity string `json:"severity"`
}

// validateResponse mirrors the validate endpoint's body: {"findings":[...]}
// (pkg/pipelineapi/http/pipeline_handlers.go's handleValidatePipeline).
type validateResponse struct {
	Findings []validateFinding `json:"findings"`
}

// renderFindingsTable prints a human-readable SEVERITY / PATH / MESSAGE
// table. No existing findings renderer was found elsewhere in cmd/datuplet
// (trigger.go and pipeline.go's put path don't render findings — put's 400
// path prints the raw server body), so this is new.
func renderFindingsTable(findings []validateFinding) {
	if len(findings) == 0 {
		fmt.Println("no findings")
		return
	}
	fmt.Printf("%-8s %-40s %s\n", "SEVERITY", "PATH", "MESSAGE")
	for _, f := range findings {
		fmt.Printf("%-8s %-40s %s\n", strings.ToUpper(f.Severity), f.Path, f.Message)
	}
}

// runPipelineValidate implements
// `datuplet pipeline validate -f <file|-> [--name <n>] [--json]`.
// POSTs the body to POST …/pipelines/validate[?name=n] (S7) and translates
// the findings response into exit codes agents can script against (spec
// §7): exit 0 when no finding has severity=="error" (warnings still print);
// exit 1 (the default for any plain error return — see main.go) when at
// least one error-severity finding exists; exit >=2 (via exitCodeErr) when
// the validate request itself fails — non-2xx status or a transport-level
// error — since the endpoint's own contract (S7) means anything other than
// a 200 findings body is an infrastructure/transport problem, not "the
// pipeline is invalid".
func runPipelineValidate(args []string) error {
	positional, remote, tokenFile, project, file, name, asJSON, _, err := parsePipelineFlags(args)
	if err != nil {
		return err
	}
	if len(positional) > 0 {
		return fmt.Errorf("validate takes no positional args (use -f <file> and optionally --name <n>); got %q", positional)
	}
	if file == "" {
		return fmt.Errorf("validate: -f <file> is required (use '-' for stdin)\nusage: datuplet pipeline validate -f <file|-> [--name <n>] [--json]")
	}

	body, err := readFileOrStdin(file)
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

	validateURL := strings.TrimRight(resolved.Remote, "/") + "/api/v1/projects/" + url.PathEscape(resolved.ID) + "/pipelines/validate"
	if name != "" {
		validateURL += "?name=" + url.QueryEscape(name)
	}

	status, respBody, err := doAuthedRequest(context.Background(),
		http.MethodPost, validateURL, resolved.APIToken, sniffContentType(body), bytes.NewReader(body))
	if err != nil {
		return &exitCodeErr{code: 2, err: fmt.Errorf("validate: %w", err)}
	}
	// S7's status contract: 200 {"findings":[...]} for every readable body;
	// 400/413/5xx mean the request itself couldn't be processed (unreadable/
	// oversized body, or an infra failure), never "the pipeline is invalid".
	// All of those are transport failures here, not findings.
	if status != http.StatusOK {
		return &exitCodeErr{code: 2, err: fmt.Errorf("validate: HTTP %d: %s", status, string(respBody))}
	}

	if asJSON {
		// Pass through the server's findings JSON verbatim.
		fmt.Print(string(respBody))
	}

	var decoded validateResponse
	if jsonErr := json.Unmarshal(respBody, &decoded); jsonErr != nil {
		return &exitCodeErr{code: 2, err: fmt.Errorf("decode validate response: %w", jsonErr)}
	}
	if !asJSON {
		renderFindingsTable(decoded.Findings)
	}

	errCount := 0
	for _, f := range decoded.Findings {
		if f.Severity == "error" {
			errCount++
		}
	}
	if errCount > 0 {
		return fmt.Errorf("validate: %d error-severity finding(s)", errCount)
	}
	return nil
}

// readFileOrStdin reads the named file, or stdin if name == "-".
// Bounded at 1 MiB to match the API's MaxBytesReader cap; rejecting
// here gives a friendlier error than the server's 413 response.
func readFileOrStdin(path string) ([]byte, error) {
	if path == "-" {
		body, err := io.ReadAll(io.LimitReader(os.Stdin, 1<<20))
		if err != nil {
			return nil, fmt.Errorf("read stdin: %w", err)
		}
		return body, nil
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", path, err)
	}
	defer f.Close()
	body, err := io.ReadAll(io.LimitReader(f, 1<<20))
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	return body, nil
}

// extractDocName parses just enough of the pipeline doc to lift its
// top-level `name` (RFC 027 §5.1 — bodies are envelope-free; there is no
// metadata block to descend into). Tolerates JSON (yaml.v3 handles both)
// and a missing name. Caller decides what to do when the result is empty.
func extractDocName(body []byte) (string, error) {
	var doc struct {
		Name string `yaml:"name" json:"name"`
	}
	if err := yaml.Unmarshal(body, &doc); err != nil {
		return "", fmt.Errorf("parse doc name: %w", err)
	}
	return doc.Name, nil
}

// sniffContentType inspects the first non-whitespace byte of a PUT body to
// decide the Content-Type header: a leading '{' signals JSON syntax;
// anything else is sent as YAML (which the server's config.Parse accepts
// JSON as a subset of anyway). Matches the server's real negotiation in
// pipeline_handlers.go's PUT handler: Content-Type: application/json
// triggers a strict JSON-syntax check; any other Content-Type (or none)
// is parsed as YAML.
func sniffContentType(body []byte) string {
	for _, b := range body {
		switch b {
		case ' ', '\t', '\n', '\r':
			continue
		case '{':
			return "application/json"
		default:
			return "application/yaml"
		}
	}
	return "application/yaml"
}
