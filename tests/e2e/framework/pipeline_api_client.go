// Package framework — minimal pipeline-api HTTP client for FGA-grant-matrix assertions.
//
// The FGA-matrix negative-path tests (charlie/dora) need to POST to
// pipeline-api's trigger endpoint and assert a 403. The K8sBackend's
// RunPipeline goes directly via kubectl apply, bypassing pipeline-api HTTP
// auth entirely — so these tests need a separate HTTP path.
//
// This file is intentionally minimal: one function (TriggerRunHTTP) that
// POSTs to /api/v1/projects/:pid/pipelines/:name/runs with a bearer token
// and returns the HTTP status code. The caller asserts the code; no run
// polling or cleanup is needed for 403 cases because pipeline-api rejects
// the request before creating any K8s resources.
package framework

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"time"
)

const (
	// pipelineAPINodePort is the OrbStack NodePort for pipeline-api.
	// Matches utils/deploy/k8s/pipeline-api.yaml.
	pipelineAPINodePort = "30081"
	pipelineAPIAddress  = "localhost:" + pipelineAPINodePort
)

// PipelineAPIBaseURL returns the base URL for pipeline-api, reading from
// DATUPLET_PIPELINE_API_URL env first, then falling back to the OrbStack
// NodePort default.
func PipelineAPIBaseURL() string {
	if u := os.Getenv("DATUPLET_PIPELINE_API_URL"); u != "" {
		return strings.TrimRight(u, "/")
	}
	return "http://" + pipelineAPIAddress
}

// PipelineAPIReachable reports whether pipeline-api is reachable via its
// NodePort. Used by FGA-matrix tests to skip when the stack isn't up.
func PipelineAPIReachable() bool {
	base := PipelineAPIBaseURL()
	host := base
	if strings.HasPrefix(host, "http://") {
		host = strings.TrimPrefix(host, "http://")
	} else if strings.HasPrefix(host, "https://") {
		host = strings.TrimPrefix(host, "https://")
	}
	// Strip path component if present.
	if i := strings.Index(host, "/"); i >= 0 {
		host = host[:i]
	}
	conn, err := net.DialTimeout("tcp", host, 2*time.Second)
	if err != nil {
		return false
	}
	conn.Close()
	return true
}

// TriggerRunHTTP POSTs to
// /api/v1/projects/:projectID/pipelines/:pipelineName/runs
// with the given bearer token and returns the HTTP status code.
//
// A 403 means pipeline-api denied the request (FGA check failed — the
// user lacks data_admin on the project). The caller should not poll for
// run completion after a 403; no K8s resources are created.
//
// The token is expected to be an impersonation JWT minted via
// MintTestUserImpersonation for the user under test. pipeline-api accepts
// bearer tokens on its REST endpoints.
func TriggerRunHTTP(ctx context.Context, projectID, pipelineName, bearerToken string) (int, error) {
	url := fmt.Sprintf("%s/api/v1/projects/%s/pipelines/%s/runs",
		PipelineAPIBaseURL(), projectID, pipelineName)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url,
		strings.NewReader("{}"))
	if err != nil {
		return 0, fmt.Errorf("build trigger request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if bearerToken != "" {
		req.Header.Set("Authorization", "Bearer "+bearerToken)
	}

	cli := &http.Client{Timeout: 15 * time.Second}
	resp, err := cli.Do(req)
	if err != nil {
		return 0, fmt.Errorf("POST %s: %w", url, err)
	}
	defer resp.Body.Close()
	// Drain body so connections are reused.
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode, nil
}
