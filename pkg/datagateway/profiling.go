package datagateway

import (
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/grafana/pyroscope-go"
)

// StartProfilingIfEnabled boots Pyroscope continuous profiling when
// DATUPLET_GATEWAY_PROFILING is truthy. Returns a stop function (or nil)
// the caller is responsible for invoking on graceful shutdown.
//
// Off by default. When on, requires:
//
//   - PYROSCOPE_SERVER_ADDRESS   — Grafana Cloud Profiles endpoint
//     (e.g. https://profiles-prod-XXX.grafana.net). Required.
//   - PYROSCOPE_USERNAME          — Grafana Cloud stack ID. Optional;
//     omit when sending to an unauthenticated in-cluster Pyroscope.
//   - PYROSCOPE_PASSWORD          — Grafana Cloud access policy token.
//     Optional in lockstep with USERNAME.
//
// The application name carries pod / namespace / run-id labels via
// DATUPLET_RUN_ID and standard K8s downward-API env (HOSTNAME,
// POD_NAMESPACE). Labels make the profiles searchable by run in the
// Grafana Cloud Profiles UI.
//
// Profile types enabled — all six that pyroscope-go ships: CPU,
// AllocObjects, AllocSpace, InuseObjects, InuseSpace, Goroutines. The
// AllocSpace + InuseSpace pair is what makes this useful for the memory
// work — they show byte-level flame graphs of where allocations happen
// (Alloc) and what's currently retained (Inuse).
//
// Profiling overhead is small (Pyroscope's design goal) but non-zero;
// keep it gated so production deployments default to "off" and only
// enable when investigating something.
func StartProfilingIfEnabled() (stop func() error) {
	v := strings.ToLower(strings.TrimSpace(os.Getenv("DATUPLET_GATEWAY_PROFILING")))
	if v != "1" && v != "true" && v != "yes" && v != "on" {
		return nil
	}

	serverAddr := strings.TrimSpace(os.Getenv("PYROSCOPE_SERVER_ADDRESS"))
	if serverAddr == "" {
		log.Printf("WARN datuplet-gateway: DATUPLET_GATEWAY_PROFILING=true but PYROSCOPE_SERVER_ADDRESS empty; profiling disabled")
		return nil
	}

	// Optional credentials — when both are set the SDK uses HTTP Basic
	// auth. When either is empty, the SDK sends unauthenticated (suitable
	// for an in-cluster open Pyroscope deployment).
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
		ApplicationName:   "datuplet-gateway",
		ServerAddress:     serverAddr,
		BasicAuthUser:     user,
		BasicAuthPassword: pass,
		Tags:              tags,
		// All six profile types. AllocSpace + InuseSpace are the headline
		// signals for memory debugging — see package doc.
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
		log.Printf("WARN datuplet-gateway: failed to start Pyroscope profiling: %v", err)
		return nil
	}

	log.Printf("datuplet-gateway: Pyroscope profiling started: server=%s app=%s tags=%v",
		serverAddr, cfg.ApplicationName, tagKeysSorted(tags))
	return profiler.Stop
}

// tagKeysSorted returns the tag keys (NOT values) for the boot log line.
// Values may carry identifiers operators don't want grepped against; keys
// alone are enough to confirm "yes, labels are wired".
func tagKeysSorted(m map[string]string) string {
	if len(m) == 0 {
		return "(none)"
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	// Inline sort to avoid pulling "sort" for a 3-element slice.
	for i := 1; i < len(keys); i++ {
		for j := i; j > 0 && keys[j] < keys[j-1]; j-- {
			keys[j], keys[j-1] = keys[j-1], keys[j]
		}
	}
	return fmt.Sprintf("[%s]", strings.Join(keys, ","))
}
