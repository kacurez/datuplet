package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

// runJSONOut mirrors pipelineapi/http/run_handlers.go::runJSON for the
// GET-run shape, with ended_at + wallclock_seconds derived client-side
// at terminal phase. Used both for the GET response and for emitted
// JSON output.
type runJSONOut struct {
	ID               string `json:"id"`
	ProjectID        string `json:"project_id,omitempty"`
	PipelineID       string `json:"pipeline_id,omitempty"`
	PipelineName     string `json:"pipeline_name,omitempty"`
	Phase            string `json:"phase"`
	CurrentStage     string `json:"current_stage,omitempty"`
	Message          string `json:"message,omitempty"`
	CreatedAt        string `json:"created_at"`
	EndedAt          string `json:"ended_at,omitempty"`
	WallclockSeconds int    `json:"wallclock_seconds,omitempty"`
}

// createRunResp matches POST /pipelines/{name}/runs response, which is a
// thinner shape — see run_handlers.go:95.
type createRunResp struct {
	ID     string `json:"id"`
	Status string `json:"status"`
	K8sNS  string `json:"k8s_ns"`
}

// Phases — pkg/k8s/api/v1/pipelinerun_types.go + runbackend/k8s.go.
const (
	phasePending           = "Pending"
	phaseRunning           = "Running"
	phaseSucceeded         = "Succeeded"
	phaseFailedUser        = "FailedUser"
	phaseFailedApplication = "FailedApplication"
	phaseCancelled         = "Cancelled"
	phaseExpired           = "Expired"
)

func isTerminalPhase(p string) bool {
	switch p {
	case phaseSucceeded, phaseFailedUser, phaseFailedApplication,
		phaseCancelled, phaseExpired:
		return true
	}
	return false
}

func exitCodeForPhase(p string) int {
	if p == phaseSucceeded {
		return 0
	}
	return 1
}

func computeWallclockSeconds(start, end time.Time) int {
	return int(end.Sub(start).Round(time.Second).Seconds())
}

// runTrigger implements `datuplet trigger ... <pipeline-name>`.
func runTrigger(remoteFlag, tokenFileFlag, projectFlag, pipelineName string, wait bool, timeout time.Duration, asJSON bool) error {
	if pipelineName == "" {
		return fmt.Errorf("pipeline name is required (positional arg)")
	}
	resolved, err := loadRemoteArgs(remoteFlag, tokenFileFlag, projectFlag)
	if err != nil {
		return err
	}

	// 1. Trigger (no per-call timeout; the parent context governs).
	ctx := context.Background()
	runID, err := triggerRun(ctx, resolved.Remote, resolved.LakekeeperProjectID, pipelineName, resolved.Token)
	if err != nil {
		return fmt.Errorf("trigger run: %w", err)
	}

	// Immediately fetch the full run record so we have created_at + phase.
	first, err := getRun(ctx, resolved.Remote, resolved.LakekeeperProjectID, runID, resolved.Token)
	if err != nil {
		return fmt.Errorf("fetch run after trigger: %w", err)
	}

	if !wait {
		return emitResult(*first, asJSON)
	}

	// 2. Poll with --timeout + signal cancel.
	pollCtx, cancel := context.WithTimeout(context.Background(), timeout)
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

	final, pollErr := pollUntilTerminal(pollCtx, resolved.Remote, resolved.LakekeeperProjectID, runID, resolved.Token)

	// 3. On timeout/interrupt, best-effort cancel cluster-side and wait
	// up to 30s for the Cancelled phase to materialise.
	if pollErr != nil && pollCtx.Err() != nil {
		// Use a *fresh* context for the cancel post + follow-up poll;
		// the parent context is already cancelled.
		ccx, ccancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer ccancel()
		_ = cancelRun(ccx, resolved.Remote, resolved.LakekeeperProjectID, runID, resolved.Token)
		if cancelled, _ := pollUntilTerminal(ccx, resolved.Remote, resolved.LakekeeperProjectID, runID, resolved.Token); cancelled != nil {
			final = cancelled
		}
	}
	if final == nil {
		if pollErr != nil {
			return pollErr
		}
		return fmt.Errorf("poll ended without terminal phase")
	}

	// 4. Derive ended_at + wallclock from created_at + poll exit time.
	startT, perr := time.Parse(time.RFC3339, final.CreatedAt)
	endT := time.Now().UTC()
	final.EndedAt = endT.Format(time.RFC3339)
	if perr == nil {
		final.WallclockSeconds = computeWallclockSeconds(startT, endT)
	}

	if err := emitResult(*final, asJSON); err != nil {
		return err
	}
	if rc := exitCodeForPhase(final.Phase); rc != 0 {
		return fmt.Errorf("run %s ended in phase %s", final.ID, final.Phase)
	}
	return nil
}

// --- HTTP helpers (all ctx-aware) ---

func triggerRun(ctx context.Context, remote, projectID, pipelineName, token string) (runID string, err error) {
	url := fmt.Sprintf("%s/api/v1/projects/%s/pipelines/%s/runs",
		strings.TrimRight(remote, "/"), projectID, pipelineName)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader([]byte(`{}`)))
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("trigger HTTP %d: %s", resp.StatusCode, string(body))
	}
	var out createRunResp
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", err
	}
	return out.ID, nil
}

func getRun(ctx context.Context, remote, projectID, runID, token string) (*runJSONOut, error) {
	url := fmt.Sprintf("%s/api/v1/projects/%s/runs/%s",
		strings.TrimRight(remote, "/"), projectID, runID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("get-run HTTP %d: %s", resp.StatusCode, string(body))
	}
	var out runJSONOut
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	return &out, nil
}

func cancelRun(ctx context.Context, remote, projectID, runID, token string) error {
	url := fmt.Sprintf("%s/api/v1/projects/%s/runs/%s/cancel",
		strings.TrimRight(remote, "/"), projectID, runID)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("cancel HTTP %d: %s", resp.StatusCode, string(body))
	}
	return nil
}

// isPermanentHTTPErr classifies HTTP errors. 4xx (except 408/429) is
// permanent — auth/not-found/bad request won't fix itself by retrying.
// Network errors and 5xx are transient.
func isPermanentHTTPErr(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	if !strings.Contains(s, "HTTP ") {
		// network / decode error — transient.
		return false
	}
	for _, code := range []string{"400", "401", "403", "404", "405", "409", "410", "411", "413", "414", "415", "422"} {
		if strings.Contains(s, "HTTP "+code) {
			return true
		}
	}
	return false
}

func pollUntilTerminal(ctx context.Context, remote, projectID, runID, token string) (*runJSONOut, error) {
	const pollInterval = 2 * time.Second
	t := time.NewTicker(pollInterval)
	defer t.Stop()
	var lastErr error
	for {
		select {
		case <-ctx.Done():
			if lastErr != nil {
				return nil, errors.Join(ctx.Err(), lastErr)
			}
			return nil, ctx.Err()
		case <-t.C:
			run, err := getRun(ctx, remote, projectID, runID, token)
			if err != nil {
				if isPermanentHTTPErr(err) {
					return nil, err
				}
				lastErr = err // retry transient
				continue
			}
			lastErr = nil
			if isTerminalPhase(run.Phase) {
				return run, nil
			}
		}
	}
}

func emitResult(r runJSONOut, asJSON bool) error {
	if asJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(r)
	}
	fmt.Printf("run %s (pipeline=%s) phase=%s wallclock=%ds\n",
		r.ID, r.PipelineName, r.Phase, r.WallclockSeconds)
	if r.Message != "" {
		fmt.Printf("message: %s\n", r.Message)
	}
	return nil
}
