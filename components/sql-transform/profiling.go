//go:build duckdb_arrow

package main

import (
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/grafana/pyroscope-go"
)

// startProfilingIfEnabled boots Pyroscope continuous profiling when
// DATUPLET_COMPONENT_PROFILING is truthy. Returns a stop function (or
// nil) the caller is responsible for invoking on graceful shutdown.
//
// Mirrors pkg/datagateway/profiling.go::StartProfilingIfEnabled in
// shape and env contract so an operator that already wires Pyroscope
// for the gateway can drop the same PYROSCOPE_* + POD_NAMESPACE +
// RUN_ID envs onto the component container and get the matching
// per-run profiles for it. ApplicationName is "datuplet-sql-transform"
// so profiles land on a distinct service in Grafana Cloud / Pyroscope.
//
// Off by default. When on, requires:
//
//   - PYROSCOPE_SERVER_ADDRESS — Pyroscope endpoint. Required.
//   - PYROSCOPE_USERNAME       — basic-auth user. Optional.
//   - PYROSCOPE_PASSWORD       — basic-auth pass. Optional.
//
// Tags surfaced (when the matching env is present):
//   pod, namespace, run_id, iteration_id.
func startProfilingIfEnabled() (stop func() error) {
	v := strings.ToLower(strings.TrimSpace(os.Getenv("DATUPLET_COMPONENT_PROFILING")))
	if v != "1" && v != "true" && v != "yes" && v != "on" {
		return nil
	}

	serverAddr := strings.TrimSpace(os.Getenv("PYROSCOPE_SERVER_ADDRESS"))
	if serverAddr == "" {
		log.Printf("WARN sql-transform: DATUPLET_COMPONENT_PROFILING=true but PYROSCOPE_SERVER_ADDRESS empty; profiling disabled")
		return nil
	}

	user := os.Getenv("PYROSCOPE_USERNAME")
	pass := os.Getenv("PYROSCOPE_PASSWORD")

	tags := map[string]string{}
	if v := os.Getenv("HOSTNAME"); v != "" {
		tags["pod"] = v
	}
	if v := os.Getenv("POD_NAMESPACE"); v != "" {
		tags["namespace"] = v
	}
	if v := os.Getenv("RUN_ID"); v != "" {
		tags["run_id"] = v
	}
	if v := os.Getenv("DATUPLET_ITERATION_ID"); v != "" {
		tags["iteration_id"] = v
	}

	cfg := pyroscope.Config{
		ApplicationName:   "datuplet-sql-transform",
		ServerAddress:     serverAddr,
		BasicAuthUser:     user,
		BasicAuthPassword: pass,
		Tags:              tags,
		ProfileTypes: []pyroscope.ProfileType{
			pyroscope.ProfileCPU,
			pyroscope.ProfileAllocObjects,
			pyroscope.ProfileAllocSpace,
			pyroscope.ProfileInuseObjects,
			pyroscope.ProfileInuseSpace,
			pyroscope.ProfileGoroutines,
		},
	}

	profiler, err := pyroscope.Start(cfg)
	if err != nil {
		log.Printf("WARN sql-transform: failed to start Pyroscope profiling: %v", err)
		return nil
	}

	log.Printf("sql-transform: Pyroscope profiling started: server=%s app=%s tags=%v",
		serverAddr, cfg.ApplicationName, tagKeysSorted(tags))
	return profiler.Stop
}

// tagKeysSorted returns sorted tag keys (not values) for the boot log line.
func tagKeysSorted(m map[string]string) string {
	if len(m) == 0 {
		return "(none)"
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	for i := 1; i < len(keys); i++ {
		for j := i; j > 0 && keys[j] < keys[j-1]; j-- {
			keys[j], keys[j-1] = keys[j-1], keys[j]
		}
	}
	return fmt.Sprintf("[%s]", strings.Join(keys, ","))
}
