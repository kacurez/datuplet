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
// in CLI tests).
type pipelineDetailJSON struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	YAML      string `json:"yaml"`
	CreatedAt string `json:"created_at"`
	UpdatedAt string `json:"updated_at"`
}

// pipelineRefJSON mirrors the list-item shape from the API. CreatedAt /
// UpdatedAt are omitempty server-side (local-file mode doesn't stat).
type pipelineRefJSON struct {
	Name      string `json:"name"`
	CreatedAt string `json:"created_at,omitempty"`
	UpdatedAt string `json:"updated_at,omitempty"`
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
                                  (name is optional — defaults to metadata.name)
  delete <name>                   delete a pipeline (prompts unless -y)

common flags:
  --remote <url>     pipeline-api URL (defaults to logged-in cluster)
  --token-file <p>   override default ~/.datuplet/token path
  --project <name>   project to operate in (defaults to first project)
  --json             emit JSON output (list, get)
  -f, --file <path>  read pipeline body from a YAML/JSON file ('-' for stdin)
  -y, --yes          skip the interactive confirmation on delete

examples:
  datuplet pipeline list --json
  datuplet pipeline get gen-big-pipeline > backup.yaml
  datuplet pipeline put -f gen-big-pipeline.yaml
  datuplet pipeline delete gen-big-pipeline -y
`
}

// parsePipelineFlags extracts the standard --remote / --token-file /
// --project / -f / --json / -y flags from an arbitrary positional slice.
// Returns (positionalArgs, remote, tokenFile, project, file, asJSON, yes, err).
//
// Hand-rolled instead of flag.NewFlagSet because the CLI's other
// commands all do their own parsing for consistency (trigger.go,
// storage.go) and we want the same UX: flags in any order, single
// positional remains.
func parsePipelineFlags(args []string) (positional []string, remote, tokenFile, project, file string, asJSON, yes bool, err error) {
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
// in one place.
func doAuthedRequest(ctx context.Context, method, urlStr, apiToken string, body io.Reader) (status int, respBody []byte, err error) {
	req, err := http.NewRequestWithContext(ctx, method, urlStr, body)
	if err != nil {
		return 0, nil, fmt.Errorf("build %s %s: %w", method, urlStr, err)
	}
	req.Header.Set("Authorization", "Bearer "+apiToken)
	if body != nil {
		req.Header.Set("Content-Type", "application/yaml")
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
	positional, remote, tokenFile, project, _, asJSON, _, err := parsePipelineFlags(args)
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
		resolved.APIToken, nil)
	if err != nil {
		return err
	}
	if status != http.StatusOK {
		return fmt.Errorf("list pipelines: HTTP %d: %s", status, string(body))
	}

	if asJSON {
		// Pass through the server's JSON verbatim.
		fmt.Println(string(body))
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

// runPipelineGet implements `datuplet pipeline get <name>`.
func runPipelineGet(args []string) error {
	positional, remote, tokenFile, project, _, asJSON, _, err := parsePipelineFlags(args)
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

	status, body, err := doAuthedRequest(context.Background(),
		http.MethodGet, pipelineURL(resolved.Remote, resolved.ID, name),
		resolved.APIToken, nil)
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
		fmt.Println(string(body))
		return nil
	}
	var detail pipelineDetailJSON
	if err := json.Unmarshal(body, &detail); err != nil {
		return fmt.Errorf("decode get response: %w", err)
	}
	// Default view: just the YAML, so callers can redirect to a file.
	fmt.Print(detail.YAML)
	return nil
}

// runPipelinePut implements `datuplet pipeline put [<name>] -f <file>`.
// When the positional <name> is omitted, falls back to the YAML's
// metadata.name. This makes simple "apply this file" usage one
// argument shorter and matches the kubectl-style ergonomic.
func runPipelinePut(args []string) error {
	positional, remote, tokenFile, project, file, _, _, err := parsePipelineFlags(args)
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

	// Resolve the name. Precedence: explicit positional > YAML's
	// metadata.name. The API enforces they match; failing here gives a
	// nicer error than the server's 400.
	yamlName, _ := extractMetadataName(body)
	var name string
	switch {
	case len(positional) == 1 && yamlName != "" && positional[0] != yamlName:
		return fmt.Errorf("pipeline name mismatch: arg=%q vs metadata.name=%q\n(omit the positional to use metadata.name, or update one to match the other)",
			positional[0], yamlName)
	case len(positional) == 1:
		name = positional[0]
	case yamlName != "":
		name = yamlName
	default:
		return fmt.Errorf("put: pipeline name required — pass it as the first positional or set metadata.name in the YAML")
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
		resolved.APIToken, bytes.NewReader(body))
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
	positional, remote, tokenFile, project, _, _, yes, err := parsePipelineFlags(args)
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
		resolved.APIToken, nil)
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

// extractMetadataName parses just enough of the pipeline YAML to lift
// metadata.name. Tolerates JSON (yaml.v3 handles both) and missing
// metadata block. Caller decides what to do when the result is empty.
func extractMetadataName(body []byte) (string, error) {
	var doc struct {
		Metadata struct {
			Name string `yaml:"name" json:"name"`
		} `yaml:"metadata" json:"metadata"`
	}
	if err := yaml.Unmarshal(body, &doc); err != nil {
		return "", fmt.Errorf("parse metadata.name: %w", err)
	}
	return doc.Metadata.Name, nil
}
