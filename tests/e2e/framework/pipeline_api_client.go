// Package framework — pipeline-api endpoint-discovery helpers.
//
// The K8s scenarios that talk to pipeline-api over HTTP (query, secrets,
// resource-gate, local-query) need the service base URL and a cheap
// reachability probe so they can skip cleanly when the NodePort / port-forward
// isn't up. Those two helpers live here; the per-endpoint request builders
// live alongside the tests that use them.
package framework

import (
	"net"
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
