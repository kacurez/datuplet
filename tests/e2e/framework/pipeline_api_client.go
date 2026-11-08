// Package framework — pipeline-api endpoint-discovery helpers.
//
// The K8s scenarios that talk to pipeline-api over HTTP (query, secrets,
// resource-gate, local-query) need the service base URL and a cheap
// reachability probe so they can skip cleanly when the NodePort / port-forward
// isn't up. Those two helpers live here; the per-endpoint request builders
// live alongside the tests that use them.
package framework

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"time"
)

const (
	// pipelineAPINodePort is the OrbStack NodePort for pipeline-api.
	// Matches the datuplet-app chart's pipeline-api NodePort (pipelineApi.service.nodePort).
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

// CompactResponse is the decoded answer of the on-demand compaction route.
// Status is the HTTP status (202 accepted, 409 compaction_running, ...); RunID
// is set on 202 and on a 409 compaction_running; Code is the error code on a
// non-202 answer.
type CompactResponse struct {
	Status int
	RunID  string
	Code   string
	Body   string
}

// CompactTable POSTs /api/v1/storage/projects/{pid}/tables/{ns}/{t}/compact as
// the backend's RunAsUser and returns the raw outcome without judging the
// status, so scenarios can assert both 202 and 409.
func (k *K8sBackend) CompactTable(ctx context.Context, ns, table string) (*CompactResponse, error) {
	projectID, err := k.ensureDatupletProject(ctx)
	if err != nil {
		return nil, fmt.Errorf("resolve Datuplet project: %w", err)
	}
	session, err := k.apiSessionFor(ctx)
	if err != nil {
		return nil, err
	}
	u := fmt.Sprintf("%s/api/v1/storage/projects/%s/tables/%s/%s/compact", PipelineAPIBaseURL(), projectID, ns, table)
	status, body, err := apiDo(ctx, http.MethodPost, u, session, "application/json", []byte("{}"))
	if err != nil {
		return nil, err
	}
	var v struct {
		RunID string `json:"run_id"`
		Error string `json:"error"`
		Code  string `json:"code"`
	}
	_ = json.Unmarshal(body, &v)
	code := v.Code
	if code == "" {
		code = v.Error
	}
	return &CompactResponse{Status: status, RunID: v.RunID, Code: code, Body: string(body)}, nil
}

// WaitRunSucceeded polls a run (by id) until it reaches a terminal phase and
// returns an error unless that phase is Succeeded.
func (k *K8sBackend) WaitRunSucceeded(ctx context.Context, runID string) error {
	projectID, err := k.ensureDatupletProject(ctx)
	if err != nil {
		return err
	}
	session, err := k.apiSessionFor(ctx)
	if err != nil {
		return err
	}
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	var last apiRunView
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("timeout waiting for run %s: last phase=%q", runID, last.Phase)
		case <-ticker.C:
			v, err := apiGetRun(ctx, PipelineAPIBaseURL(), session, projectID, runID)
			if err != nil {
				continue
			}
			last = v
			switch v.Phase {
			case "Succeeded":
				return nil
			case "", "Pending", "Running":
			default:
				return fmt.Errorf("run %s ended %s: %s", runID, v.Phase, v.Message)
			}
		}
	}
}
