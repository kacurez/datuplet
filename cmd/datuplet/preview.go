package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	"gopkg.in/yaml.v3"
)

// Preview GETs may assemble the result document on first terminal read
// (a query-worker sample per table), so they get a generous client timeout.
// DELETEs purge synchronously server-side (up to ~90 s per preview); a client
// disconnect would abort the purge mid-way, so they get 10 minutes.
var (
	previewGetClient    = &http.Client{Timeout: 2 * time.Minute}
	previewDeleteClient = &http.Client{Timeout: 10 * time.Minute}
	previewPollInterval = 2 * time.Second
)

const previewTimeoutSlack = 2 * time.Minute

const (
	exitPreviewUser     = 1
	exitPreviewPlatform = 20
)

type previewCreateResp struct {
	ID string `json:"id"`
}

type previewResult struct {
	ID          string `json:"id"`
	Phase       string `json:"phase"`
	Message     string `json:"message"`
	ResultReady bool   `json:"result_ready"`
	Stages      []struct {
		Name       string `json:"name"`
		Phase      string `json:"phase"`
		DurationMS int64  `json:"duration_ms"`
		Message    string `json:"message"`
		Components []struct {
			Name           string  `json:"name"`
			Phase          string  `json:"phase"`
			LogTail        *string `json:"log_tail"`
			GatewayLogTail *string `json:"gateway_log_tail"`
		} `json:"components"`
	} `json:"stages"`
	Outputs []struct {
		Bucket       string `json:"bucket"`
		Table        string `json:"table"`
		ScratchTable string `json:"scratch_table"`
		Rows         int64  `json:"rows"`
		AtCap        bool   `json:"at_cap"`
		Schema       []struct {
			Name     string `json:"name"`
			Type     string `json:"type"`
			Nullable bool   `json:"nullable"`
		} `json:"schema"`
		Sample *struct {
			Columns []struct {
				Name string `json:"name"`
			} `json:"columns"`
			Rows      [][]any `json:"rows"`
			Truncated bool    `json:"truncated"`
		} `json:"sample"`
	} `json:"outputs"`
	Warnings []string `json:"warnings"`
}

type previewListResp struct {
	Previews []struct {
		ID         string  `json:"id"`
		Workspace  string  `json:"workspace"`
		Phase      string  `json:"phase"`
		DocName    string  `json:"doc_name"`
		UntilStage *string `json:"until_stage"`
		CreatedAt  string  `json:"created_at"`
		ExpiresAt  string  `json:"expires_at"`
	} `json:"previews"`
}

func previewHelpText() string {
	return `preview subcommands:
  [run] -f <file|-> [flags]       create a preview, wait for the result document, print it
  get <id> [--json]               print a preview's result document
  list [--workspace W|'*']        list previews
  delete <id>                     purge one preview
  delete --workspace W --all      purge every preview in a workspace
  cancel <id>                     cancel a running preview

run flags:
  -f, --file <path>   PipelineDoc YAML/JSON ('-' for stdin)
  --workspace <w>     scratch workspace (default $DATUPLET_WORKSPACE, else "default")
  --rows <n>          row cap per table (server default 1000)
  --until-stage <s>   stop after this stage
  --timeout <dur>     preview timeout (server default 5m)
  --ttl <dur>         how long the preview is kept (server default 1h)
  --no-wait           print the created id and return
  --json              print the result document verbatim

common flags: --remote, --token-file, --project

exit codes: 0 Succeeded; 1 FailedUser/Cancelled/Expired/timed out/validation/409;
            20 FailedApplication or transport/5xx
`
}

// runPreview dispatches `datuplet preview ...`.
func runPreview(args []string) error {
	if len(args) == 0 || strings.HasPrefix(args[0], "-") {
		if len(args) > 0 && (args[0] == "-h" || args[0] == "--help" || args[0] == "-help") {
			fmt.Print(previewHelpText())
			return nil
		}
		return runPreviewRun(args)
	}
	switch args[0] {
	case "run":
		return runPreviewRun(args[1:])
	case "get", "show":
		return runPreviewGet(args[1:])
	case "list", "ls":
		return runPreviewList(args[1:])
	case "delete", "del", "rm":
		return runPreviewDelete(args[1:])
	case "cancel":
		return runPreviewCancel(args[1:])
	case "help":
		fmt.Print(previewHelpText())
		return nil
	default:
		return fmt.Errorf("unknown preview subcommand %q\n%s", args[0], previewHelpText())
	}
}

type previewCommon struct {
	remote, tokenFile, project string
}

func (c *previewCommon) register(fs *flag.FlagSet) {
	fs.StringVar(&c.remote, "remote", "", "pipeline-api URL")
	fs.StringVar(&c.tokenFile, "token-file", "", "override default ~/.datuplet/token path")
	fs.StringVar(&c.project, "project", "", "project to operate in")
}

func newPreviewFlagSet(name string) *flag.FlagSet {
	fs := flag.NewFlagSet("preview "+name, flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	return fs
}

func (c *previewCommon) resolve() (*remoteArgs, string, error) {
	resolved, err := loadRemoteArgs(c.remote, c.tokenFile, c.project)
	if err != nil {
		return nil, "", err
	}
	if err := resolved.RequireAPIToken(); err != nil {
		return nil, "", err
	}
	base := strings.TrimRight(resolved.Remote, "/") + "/api/v1/projects/" + url.PathEscape(resolved.ID) + "/previews"
	return resolved, base, nil
}

func previewDefaultWorkspace() string {
	if w := os.Getenv("DATUPLET_WORKSPACE"); w != "" {
		return w
	}
	return "default"
}

func previewDo(ctx context.Context, client *http.Client, method, urlStr, token string, body []byte) (int, []byte, error) {
	var rd io.Reader
	if body != nil {
		rd = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, urlStr, rd)
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
	if err != nil {
		return resp.StatusCode, nil, fmt.Errorf("read response: %w", err)
	}
	return resp.StatusCode, b, nil
}

// previewHTTPErr maps a non-2xx response to the exit-code contract:
// 5xx is a platform failure (20), anything else is the caller's (1).
func previewHTTPErr(op string, status int, body []byte) error {
	code := exitPreviewUser
	if status >= 500 {
		code = exitPreviewPlatform
	}
	return &exitCodeErr{code: code, err: fmt.Errorf("%s: HTTP %d: %s", op, status, strings.TrimSpace(string(body)))}
}

func previewTransportErr(op string, err error) error {
	return &exitCodeErr{code: exitPreviewPlatform, err: fmt.Errorf("%s: %w", op, err)}
}

func previewExitForPhase(phase string) int {
	switch phase {
	case phaseSucceeded:
		return 0
	case phaseFailedApplication:
		return exitPreviewPlatform
	}
	return exitPreviewUser
}

func runPreviewRun(args []string) error {
	fs := newPreviewFlagSet("run")
	var c previewCommon
	c.register(fs)
	var file, workspace, untilStage string
	var rows int
	var timeout, ttl time.Duration
	var noWait, asJSON bool
	fs.StringVar(&file, "f", "", "PipelineDoc file ('-' for stdin)")
	fs.StringVar(&file, "file", "", "PipelineDoc file ('-' for stdin)")
	fs.StringVar(&workspace, "workspace", previewDefaultWorkspace(), "scratch workspace")
	fs.IntVar(&rows, "rows", 0, "row cap per table")
	fs.StringVar(&untilStage, "until-stage", "", "stop after this stage")
	fs.DurationVar(&timeout, "timeout", 0, "preview timeout")
	fs.DurationVar(&ttl, "ttl", 0, "preview retention")
	fs.BoolVar(&noWait, "no-wait", false, "return after creating")
	fs.BoolVar(&asJSON, "json", false, "print the result document verbatim")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() > 0 {
		return fmt.Errorf("unexpected arguments %q (flags go before positionals)", fs.Args())
	}
	if file == "" {
		return fmt.Errorf("-f <file> is required (use '-' for stdin)")
	}
	raw, err := readFileOrStdin(file)
	if err != nil {
		return err
	}
	var doc map[string]any
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		return fmt.Errorf("parse %s: %w", file, err)
	}
	if doc == nil {
		return fmt.Errorf("%s: empty document", file)
	}

	req := map[string]any{"doc": doc, "workspace": workspace}
	if rows > 0 {
		req["row_limit"] = rows
	}
	if untilStage != "" {
		req["until_stage"] = untilStage
	}
	if timeout > 0 {
		req["timeout_seconds"] = int(timeout.Round(time.Second).Seconds())
	}
	if ttl > 0 {
		req["ttl_seconds"] = int(ttl.Round(time.Second).Seconds())
	}
	reqBody, err := json.Marshal(req)
	if err != nil {
		return err
	}

	resolved, base, err := c.resolve()
	if err != nil {
		return err
	}
	ctx := context.Background()
	status, body, err := previewDo(ctx, previewGetClient, http.MethodPost, base, resolved.APIToken, reqBody)
	if err != nil {
		return previewTransportErr("create preview", err)
	}
	switch {
	case status == http.StatusBadRequest:
		var v struct {
			Error    string            `json:"error"`
			Findings []validateFinding `json:"findings"`
		}
		if json.Unmarshal(body, &v) == nil && len(v.Findings) > 0 {
			if asJSON {
				fmt.Print(string(body))
			} else {
				renderFindingsTable(v.Findings)
			}
			return &exitCodeErr{code: exitPreviewUser, err: fmt.Errorf("preview: %s", v.Error)}
		}
		return previewHTTPErr("create preview", status, body)
	case status == http.StatusConflict:
		var v struct {
			Error              string `json:"error"`
			RunningInWorkspace int    `json:"running_in_workspace"`
			RunningInProject   int    `json:"running_in_project"`
			LiveInProject      int    `json:"live_in_project"`
		}
		if json.Unmarshal(body, &v) == nil && v.Error != "" {
			return &exitCodeErr{code: exitPreviewUser, err: fmt.Errorf(
				"preview: %s (running_in_workspace=%d running_in_project=%d live_in_project=%d); delete or wait for existing previews",
				v.Error, v.RunningInWorkspace, v.RunningInProject, v.LiveInProject)}
		}
		return previewHTTPErr("create preview", status, body)
	case status/100 != 2:
		return previewHTTPErr("create preview", status, body)
	}
	var created previewCreateResp
	if err := json.Unmarshal(body, &created); err != nil || created.ID == "" {
		return previewTransportErr("create preview", fmt.Errorf("decode response: %v", err))
	}
	if noWait {
		if asJSON {
			fmt.Print(string(body))
		} else {
			fmt.Printf("preview %s created\n", created.ID)
		}
		return nil
	}

	effective := timeout
	if effective <= 0 {
		effective = 5 * time.Minute
	}
	pollCtx, cancel := context.WithTimeout(ctx, effective+previewTimeoutSlack)
	defer cancel()
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigs)
	go func() {
		select {
		case <-sigs:
			cancel()
		case <-pollCtx.Done():
		}
	}()

	final, finalBody, pollErr := pollPreview(pollCtx, base, created.ID, resolved.APIToken)
	if pollErr != nil {
		if errors.Is(pollErr, context.DeadlineExceeded) || errors.Is(pollErr, context.Canceled) {
			cctx, ccancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer ccancel()
			if cerr := cancelPreview(cctx, base, created.ID, resolved.APIToken); cerr != nil {
				fmt.Fprintf(os.Stderr, "WARN: cancel POST failed for preview %s: %v (preview may still be running)\n", created.ID, cerr)
			}
			return &exitCodeErr{code: exitPreviewUser, err: fmt.Errorf("preview %s timed out or was interrupted; cancel requested", created.ID)}
		}
		return pollErr
	}

	if asJSON {
		fmt.Print(string(finalBody))
	} else {
		renderPreviewSummary(final)
	}
	if code := previewExitForPhase(final.Phase); code != 0 {
		return &exitCodeErr{code: code, err: fmt.Errorf("preview %s ended in phase %s", final.ID, final.Phase)}
	}
	return nil
}

func getPreview(ctx context.Context, base, id, token, query string) (*previewResult, []byte, error) {
	u := base + "/" + url.PathEscape(id) + query
	status, body, err := previewDo(ctx, previewGetClient, http.MethodGet, u, token, nil)
	if err != nil {
		return nil, nil, previewTransportErr("get preview", err)
	}
	if status/100 != 2 {
		return nil, nil, previewHTTPErr("get preview", status, body)
	}
	var r previewResult
	if err := json.Unmarshal(body, &r); err != nil {
		return nil, nil, previewTransportErr("get preview", fmt.Errorf("decode response: %w", err))
	}
	return &r, body, nil
}

// pollPreview waits until the phase is terminal and the result document is
// cached. Transport errors and 5xx are retried on the next tick (a GET that
// times out while assembling is simply asked again); other 4xx are permanent.
func pollPreview(ctx context.Context, base, id, token string) (*previewResult, []byte, error) {
	t := time.NewTicker(previewPollInterval)
	defer t.Stop()
	var lastErr error
	for {
		select {
		case <-ctx.Done():
			if lastErr != nil {
				return nil, nil, errors.Join(ctx.Err(), lastErr)
			}
			return nil, nil, ctx.Err()
		case <-t.C:
			r, body, err := getPreview(ctx, base, id, token, "")
			if err != nil {
				var ece *exitCodeErr
				if errors.As(err, &ece) && ece.code == exitPreviewUser {
					return nil, nil, err
				}
				lastErr = err
				continue
			}
			lastErr = nil
			if isTerminalPhase(r.Phase) && r.ResultReady {
				return r, body, nil
			}
		}
	}
}

func cancelPreview(ctx context.Context, base, id, token string) error {
	status, body, err := previewDo(ctx, previewGetClient, http.MethodPost, base+"/"+url.PathEscape(id)+"/cancel", token, nil)
	if err != nil {
		return err
	}
	if status/100 != 2 {
		return previewHTTPErr("cancel preview", status, body)
	}
	return nil
}

func renderPreviewSummary(r *previewResult) {
	fmt.Printf("preview %s phase=%s\n", r.ID, r.Phase)
	if r.Message != "" {
		fmt.Printf("message: %s\n", r.Message)
	}
	for _, s := range r.Stages {
		fmt.Printf("stage %s %s %s\n", s.Name, s.Phase, (time.Duration(s.DurationMS) * time.Millisecond).Round(100*time.Millisecond))
		if s.Message != "" {
			fmt.Printf("  %s\n", s.Message)
		}
		for _, c := range s.Components {
			if c.LogTail != nil && *c.LogTail != "" {
				fmt.Printf("  --- %s log ---\n%s\n", c.Name, *c.LogTail)
			}
			if c.GatewayLogTail != nil && *c.GatewayLogTail != "" {
				fmt.Printf("  --- %s gateway log ---\n%s\n", c.Name, *c.GatewayLogTail)
			}
		}
	}
	for _, o := range r.Outputs {
		fmt.Printf("\n%s.%s rows=%d at_cap=%t → %s\n", o.Bucket, o.Table, o.Rows, o.AtCap, o.ScratchTable)
		if len(o.Schema) > 0 {
			tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
			fmt.Fprintln(tw, "  COLUMN\tTYPE\tNULLABLE")
			for _, f := range o.Schema {
				fmt.Fprintf(tw, "  %s\t%s\t%t\n", f.Name, f.Type, f.Nullable)
			}
			tw.Flush()
		}
		if o.Sample != nil && len(o.Sample.Rows) > 0 {
			tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
			names := make([]string, len(o.Sample.Columns))
			for i, c := range o.Sample.Columns {
				names[i] = c.Name
			}
			fmt.Fprintln(tw, "  "+strings.Join(names, "\t"))
			for i, row := range o.Sample.Rows {
				if i >= 10 {
					break
				}
				cells := make([]string, len(row))
				for j, v := range row {
					cells[j] = fmt.Sprint(v)
				}
				fmt.Fprintln(tw, "  "+strings.Join(cells, "\t"))
			}
			tw.Flush()
		}
	}
	if len(r.Warnings) > 0 {
		fmt.Println()
		for _, w := range r.Warnings {
			fmt.Printf("warning: %s\n", w)
		}
	}
}

func runPreviewGet(args []string) error {
	fs := newPreviewFlagSet("get")
	var c previewCommon
	c.register(fs)
	var asJSON, resample bool
	fs.BoolVar(&asJSON, "json", false, "print the result document verbatim")
	fs.BoolVar(&resample, "resample", false, "retry sampling for tables whose sample is missing")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return fmt.Errorf("usage: datuplet preview get <id> [--json] [--resample]")
	}
	resolved, base, err := c.resolve()
	if err != nil {
		return err
	}
	query := ""
	if resample {
		query = "?resample=true"
	}
	r, body, err := getPreview(context.Background(), base, fs.Arg(0), resolved.APIToken, query)
	if err != nil {
		return err
	}
	if asJSON {
		fmt.Print(string(body))
		return nil
	}
	renderPreviewSummary(r)
	return nil
}

func runPreviewList(args []string) error {
	fs := newPreviewFlagSet("list")
	var c previewCommon
	c.register(fs)
	var asJSON bool
	workspace := previewDefaultWorkspace()
	fs.StringVar(&workspace, "workspace", workspace, "workspace ('*' for all)")
	fs.BoolVar(&asJSON, "json", false, "print JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() > 0 {
		return fmt.Errorf("list takes no positional args; got %q", fs.Args())
	}
	resolved, base, err := c.resolve()
	if err != nil {
		return err
	}
	status, body, err := previewDo(context.Background(), previewGetClient, http.MethodGet,
		base+"?workspace="+url.QueryEscape(workspace), resolved.APIToken, nil)
	if err != nil {
		return previewTransportErr("list previews", err)
	}
	if status/100 != 2 {
		return previewHTTPErr("list previews", status, body)
	}
	if asJSON {
		fmt.Print(string(body))
		return nil
	}
	var v previewListResp
	if err := json.Unmarshal(body, &v); err != nil {
		return previewTransportErr("list previews", fmt.Errorf("decode response: %w", err))
	}
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "ID\tWORKSPACE\tPHASE\tPIPELINE\tUNTIL\tCREATED\tEXPIRES")
	for _, p := range v.Previews {
		until := ""
		if p.UntilStage != nil {
			until = *p.UntilStage
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\t%s\n", p.ID, p.Workspace, p.Phase, p.DocName, until, p.CreatedAt, p.ExpiresAt)
	}
	return tw.Flush()
}

func runPreviewDelete(args []string) error {
	fs := newPreviewFlagSet("delete")
	var c previewCommon
	c.register(fs)
	var all bool
	var workspace string
	fs.BoolVar(&all, "all", false, "delete every preview in --workspace")
	fs.StringVar(&workspace, "workspace", "", "workspace for --all")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if all {
		if fs.NArg() > 0 {
			return fmt.Errorf("--all takes no preview id; got %q", fs.Args())
		}
		if workspace == "" || workspace == "*" {
			return fmt.Errorf("--all requires an explicit --workspace W (not empty or '*')")
		}
	} else if fs.NArg() != 1 {
		return fmt.Errorf("usage: datuplet preview delete <id> | --workspace W --all")
	}
	resolved, base, err := c.resolve()
	if err != nil {
		return err
	}
	ctx := context.Background()
	if !all {
		status, body, err := previewDo(ctx, previewDeleteClient, http.MethodDelete, base+"/"+url.PathEscape(fs.Arg(0)), resolved.APIToken, nil)
		if err != nil {
			return previewTransportErr("delete preview", err)
		}
		if status/100 != 2 {
			return previewHTTPErr("delete preview", status, body)
		}
		fmt.Printf("deleted %s\n", fs.Arg(0))
		return nil
	}
	status, body, err := previewDo(ctx, previewDeleteClient, http.MethodDelete, base+"?workspace="+url.QueryEscape(workspace), resolved.APIToken, nil)
	if err != nil {
		return previewTransportErr("delete previews", err)
	}
	if status/100 != 2 {
		return previewHTTPErr("delete previews", status, body)
	}
	var v struct {
		Deleted []string          `json:"deleted"`
		Failed  map[string]string `json:"failed"`
	}
	if err := json.Unmarshal(body, &v); err != nil {
		return previewTransportErr("delete previews", fmt.Errorf("decode response: %w", err))
	}
	fmt.Printf("deleted %d preview(s) in workspace %s\n", len(v.Deleted), workspace)
	if len(v.Failed) > 0 {
		for id, msg := range v.Failed {
			fmt.Fprintf(os.Stderr, "failed to delete %s: %s\n", id, msg)
		}
		return &exitCodeErr{code: exitPreviewPlatform, err: fmt.Errorf("%d preview(s) failed to delete", len(v.Failed))}
	}
	return nil
}

func runPreviewCancel(args []string) error {
	fs := newPreviewFlagSet("cancel")
	var c previewCommon
	c.register(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return fmt.Errorf("usage: datuplet preview cancel <id>")
	}
	resolved, base, err := c.resolve()
	if err != nil {
		return err
	}
	if err := cancelPreview(context.Background(), base, fs.Arg(0), resolved.APIToken); err != nil {
		var ece *exitCodeErr
		if errors.As(err, &ece) {
			return err
		}
		return previewTransportErr("cancel preview", err)
	}
	fmt.Printf("cancel requested for %s\n", fs.Arg(0))
	return nil
}
