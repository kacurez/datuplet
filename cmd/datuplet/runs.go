package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"
)

// runs.go — `datuplet runs <list|get>`: read-only observability over the
// project-scoped run endpoints pipeline-api already exposes
// (pkg/pipelineapi/http/run_handlers.go). No server changes: this is a thin
// client over GET /api/v1/projects/{pid}/runs and .../runs/{id}, using the
// same headless auth (loadRemoteArgs + RequireAPIToken) as trigger/storage.

// runSummary mirrors one entry of the runs list / the top of a run detail
// (runJSON in run_handlers.go).
type runSummary struct {
	ID           string `json:"id"`
	PipelineName string `json:"pipeline_name"`
	Phase        string `json:"phase"`
	CurrentStage string `json:"current_stage"`
	Message      string `json:"message"`
	CreatedAt    string `json:"created_at"`
	StartedAt    string `json:"started_at"`
	CompletedAt  string `json:"completed_at"`
}

// runsListResp mirrors runsPageJSON: runs + a nullable next_cursor.
type runsListResp struct {
	Runs       []runSummary `json:"runs"`
	NextCursor *string      `json:"next_cursor"`
}

// runTimelineStage mirrors the timelineStage entries on a run detail.
type runTimelineStage struct {
	Name        string `json:"name"`
	Phase       string `json:"phase"`
	StartedAt   string `json:"started_at"`
	CompletedAt string `json:"completed_at"`
	DurationMS  *int64 `json:"duration_ms"`
	Message     string `json:"message"`
}

// runDetailResp mirrors GET .../runs/{id}: a runSummary plus the timeline.
type runDetailResp struct {
	runSummary
	ProjectID  string             `json:"project_id"`
	PipelineID string             `json:"pipeline_id"`
	Timeline   []runTimelineStage `json:"timeline"`
}

// fetchRunsList calls GET /api/v1/projects/{pid}/runs and returns both the raw
// body (for --json passthrough) and the decoded page.
func fetchRunsList(ctx context.Context, remote, apiToken, projectID string, q url.Values) ([]byte, runsListResp, error) {
	reqURL := fmt.Sprintf("%s/api/v1/projects/%s/runs", strings.TrimRight(remote, "/"), url.PathEscape(projectID))
	if enc := q.Encode(); enc != "" {
		reqURL += "?" + enc
	}
	status, body, err := doAuthedRequest(ctx, http.MethodGet, reqURL, apiToken, "", nil)
	if err != nil {
		return nil, runsListResp{}, err
	}
	if status != http.StatusOK {
		return nil, runsListResp{}, fmt.Errorf("list runs: HTTP %d: %s", status, strings.TrimSpace(string(body)))
	}
	var resp runsListResp
	if err := json.Unmarshal(body, &resp); err != nil {
		return body, runsListResp{}, fmt.Errorf("decode list response: %w", err)
	}
	return body, resp, nil
}

// fetchRunDetail calls GET /api/v1/projects/{pid}/runs/{id}. It returns the raw
// body, the decoded detail, and the HTTP status so the caller can map 404 (run
// not found) and other non-200s to friendly errors. On a non-200 the decoded
// detail is zero-valued.
func fetchRunDetail(ctx context.Context, remote, apiToken, projectID, runID string) ([]byte, runDetailResp, int, error) {
	reqURL := fmt.Sprintf("%s/api/v1/projects/%s/runs/%s",
		strings.TrimRight(remote, "/"), url.PathEscape(projectID), url.PathEscape(runID))
	status, body, err := doAuthedRequest(ctx, http.MethodGet, reqURL, apiToken, "", nil)
	if err != nil {
		return nil, runDetailResp{}, 0, err
	}
	if status != http.StatusOK {
		return body, runDetailResp{}, status, nil
	}
	var d runDetailResp
	if err := json.Unmarshal(body, &d); err != nil {
		return body, runDetailResp{}, status, fmt.Errorf("decode run detail: %w", err)
	}
	return body, d, status, nil
}

// runRuns dispatches `datuplet runs <list|get>`.
func runRuns(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: datuplet runs <list|get> [args]\n%s", runsHelpText())
	}
	sub := args[0]
	rest := args[1:]
	switch sub {
	case "list", "ls":
		return runRunsList(rest)
	case "get", "show":
		return runRunsGet(rest)
	case "help", "-h", "--help":
		fmt.Println(runsHelpText())
		return nil
	default:
		return fmt.Errorf("unknown runs subcommand %q\n%s", sub, runsHelpText())
	}
}

func runsHelpText() string {
	return `runs subcommands:
  list                            list runs in the project (newest first)
  get <run-id>                    show one run's detail + stage timeline

list-only flags:
  --pipeline <name>  filter to runs whose pipeline name contains this substring
  --phase <phase>    filter by phase (Pending|Running|Succeeded|FailedUser|FailedApplication|Cancelled|Expired)
  --limit <n>        page size (1..200; server default 50)
  --cursor <c>       opaque cursor from a previous page's next-cursor hint

common flags:
  --remote <url>     pipeline-api URL (defaults to logged-in cluster)
  --token-file <p>   override default ~/.datuplet/token path
  --project <name>   project name (auto-defaulted if you have exactly one)
  --json             emit the raw JSON body

examples:
  datuplet runs list --pipeline daily-sales --json
  datuplet runs list --phase Running
  datuplet runs get 57298d31-2074-458b-9c7c-6e90cee14353`
}

// runsFlags holds the parsed flags shared by the runs subcommands. Hand-rolled
// like parseComponentsFlags/parsePipelineFlags so flags may appear in any order
// and a single positional (the run-id, for `get`) survives.
type runsFlags struct {
	positional        []string
	remote, tokenFile string
	project           string
	pipeline, phase   string
	cursor            string
	limit             string
	asJSON            bool
}

func parseRunsFlags(args []string) (runsFlags, error) {
	var f runsFlags
	i := 0
	// takeValue reads the value for a "--flag value" form, erroring if absent.
	takeValue := func(name string) (string, error) {
		if i+1 >= len(args) {
			return "", fmt.Errorf("%s requires a value", name)
		}
		v := args[i+1]
		i += 2
		return v, nil
	}
	for i < len(args) {
		a := args[i]
		var err error
		switch {
		case a == "--json":
			f.asJSON = true
			i++
		case a == "--remote":
			f.remote, err = takeValue("--remote")
		case strings.HasPrefix(a, "--remote="):
			f.remote = strings.TrimPrefix(a, "--remote=")
			i++
		case a == "--token-file":
			f.tokenFile, err = takeValue("--token-file")
		case strings.HasPrefix(a, "--token-file="):
			f.tokenFile = strings.TrimPrefix(a, "--token-file=")
			i++
		case a == "--project":
			f.project, err = takeValue("--project")
		case strings.HasPrefix(a, "--project="):
			f.project = strings.TrimPrefix(a, "--project=")
			i++
		case a == "--pipeline":
			f.pipeline, err = takeValue("--pipeline")
		case strings.HasPrefix(a, "--pipeline="):
			f.pipeline = strings.TrimPrefix(a, "--pipeline=")
			i++
		case a == "--phase":
			f.phase, err = takeValue("--phase")
		case strings.HasPrefix(a, "--phase="):
			f.phase = strings.TrimPrefix(a, "--phase=")
			i++
		case a == "--cursor":
			f.cursor, err = takeValue("--cursor")
		case strings.HasPrefix(a, "--cursor="):
			f.cursor = strings.TrimPrefix(a, "--cursor=")
			i++
		case a == "--limit":
			f.limit, err = takeValue("--limit")
		case strings.HasPrefix(a, "--limit="):
			f.limit = strings.TrimPrefix(a, "--limit=")
			i++
		case strings.HasPrefix(a, "-"):
			return f, fmt.Errorf("unknown flag %q", a)
		default:
			f.positional = append(f.positional, a)
			i++
		}
		if err != nil {
			return f, err
		}
	}
	return f, nil
}

// runRunsList implements `datuplet runs list`.
func runRunsList(args []string) error {
	f, err := parseRunsFlags(args)
	if err != nil {
		return err
	}
	if len(f.positional) > 0 {
		return fmt.Errorf("runs list takes no positional arguments (got %q); did you mean `runs get %s`?", f.positional[0], f.positional[0])
	}
	if f.limit != "" {
		if _, err := strconv.Atoi(f.limit); err != nil {
			return fmt.Errorf("--limit must be an integer, got %q", f.limit)
		}
	}
	resolved, err := loadRemoteArgs(f.remote, f.tokenFile, f.project)
	if err != nil {
		return err
	}
	if err := resolved.RequireAPIToken(); err != nil {
		return err
	}

	q := url.Values{}
	if f.pipeline != "" {
		q.Set("pipeline", f.pipeline)
	}
	if f.phase != "" {
		q.Set("phase", f.phase)
	}
	if f.limit != "" {
		q.Set("limit", f.limit)
	}
	if f.cursor != "" {
		q.Set("cursor", f.cursor)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	body, resp, err := fetchRunsList(ctx, resolved.Remote, resolved.APIToken, resolved.ID, q)
	if err != nil {
		return err
	}

	if f.asJSON {
		fmt.Println(string(body))
		return nil
	}
	if len(resp.Runs) == 0 {
		fmt.Println("No runs found.")
		return nil
	}
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "RUN ID\tPIPELINE\tPHASE\tCREATED\tMESSAGE")
	for _, r := range resp.Runs {
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\n",
			r.ID, dashIfEmpty(r.PipelineName), r.Phase, dashIfEmpty(r.CreatedAt), truncateMsg(r.Message))
	}
	_ = tw.Flush()
	// Paging hint to stderr so it never pollutes a piped table.
	if resp.NextCursor != nil && *resp.NextCursor != "" {
		fmt.Fprintf(os.Stderr, "\nmore results — next page: --cursor %s\n", *resp.NextCursor)
	}
	return nil
}

// runRunsGet implements `datuplet runs get <run-id>`.
func runRunsGet(args []string) error {
	f, err := parseRunsFlags(args)
	if err != nil {
		return err
	}
	if len(f.positional) != 1 {
		return fmt.Errorf("usage: datuplet runs get <run-id> [--json]")
	}
	runID := f.positional[0]
	if f.pipeline != "" || f.phase != "" || f.limit != "" || f.cursor != "" {
		return fmt.Errorf("--pipeline/--phase/--limit/--cursor are list-only flags")
	}
	resolved, err := loadRemoteArgs(f.remote, f.tokenFile, f.project)
	if err != nil {
		return err
	}
	if err := resolved.RequireAPIToken(); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	body, d, status, err := fetchRunDetail(ctx, resolved.Remote, resolved.APIToken, resolved.ID, runID)
	if err != nil {
		return err
	}
	if status == http.StatusNotFound {
		return fmt.Errorf("run %q not found in project %q", runID, resolved.ProjectName)
	}
	if status != http.StatusOK {
		return fmt.Errorf("get run: HTTP %d: %s", status, strings.TrimSpace(string(body)))
	}

	if f.asJSON {
		fmt.Println(string(body))
		return nil
	}
	fmt.Printf("Run:      %s\n", d.ID)
	fmt.Printf("Pipeline: %s\n", dashIfEmpty(d.PipelineName))
	fmt.Printf("Phase:    %s\n", d.Phase)
	if d.CurrentStage != "" {
		fmt.Printf("Stage:    %s\n", d.CurrentStage)
	}
	fmt.Printf("Created:  %s\n", dashIfEmpty(d.CreatedAt))
	if d.StartedAt != "" {
		fmt.Printf("Started:  %s\n", d.StartedAt)
	}
	if d.CompletedAt != "" {
		fmt.Printf("Ended:    %s\n", d.CompletedAt)
	}
	if d.Message != "" {
		fmt.Printf("Message:  %s\n", d.Message)
	}
	if len(d.Timeline) > 0 {
		fmt.Println("\nTimeline:")
		tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(tw, "  STAGE\tPHASE\tSTARTED\tDURATION\tMESSAGE")
		for _, s := range d.Timeline {
			fmt.Fprintf(tw, "  %s\t%s\t%s\t%s\t%s\n",
				s.Name, dashIfEmpty(s.Phase), dashIfEmpty(s.StartedAt), durationStr(s.DurationMS), truncateMsg(s.Message))
		}
		_ = tw.Flush()
	}
	return nil
}

func dashIfEmpty(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

// truncateMsg keeps the table readable; the full message is in --json.
func truncateMsg(s string) string {
	s = strings.ReplaceAll(s, "\n", " ")
	const max = 60
	if len(s) > max {
		return s[:max-1] + "…"
	}
	return dashIfEmpty(s)
}

func durationStr(ms *int64) string {
	if ms == nil {
		return "-"
	}
	return (time.Duration(*ms) * time.Millisecond).Round(time.Millisecond).String()
}
