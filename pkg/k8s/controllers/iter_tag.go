package controllers

import "strings"

// iterTagFromImage extracts the iteration ID from an image reference of the form
//
//	ttl.sh/datuplet-<service>-iter-<id>:<ttl>
//
// where <id> is the git short SHA (optionally with a "-dirty" suffix).
// Returns "" for any reference that does not contain "-iter-" before the
// tag separator. Used by the PipelineRun controller to plumb
// DATUPLET_ITERATION_ID onto per-run gateway sidecars so Pyroscope tags
// every iteration distinctly.
func iterTagFromImage(img string) string {
	// Strip optional "@<digest>" first (digest-pinned refs like
	// "ttl.sh/foo-iter-abc@sha256:...") so the subsequent colon-strip
	// pass sees the tag colon only.
	if at := strings.Index(img, "@"); at >= 0 {
		img = img[:at]
	}
	// Strip optional ":<tag>" suffix — but only if the colon comes AFTER
	// the last "/" (otherwise it's a registry port like "localhost:5000"
	// and we'd corrupt the path).
	if colon := strings.LastIndex(img, ":"); colon > strings.LastIndex(img, "/") {
		img = img[:colon]
	}
	// Find the last "-iter-" marker (last so a service name containing
	// "iter" earlier in the path does not false-positive).
	const marker = "-iter-"
	if i := strings.LastIndex(img, marker); i >= 0 {
		return img[i+len(marker):]
	}
	return ""
}
