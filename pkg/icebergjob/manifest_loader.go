// Package icebergjob — files.json manifest reader.
//
// DataGateway writes ONE manifest per (namespace, table) at end-of-stream
// at `<table-base>/.run-state/<run-id>/files.json`. Per-table placement
// keeps the manifest inside the per-table STS scope DG holds.
// See pkg/datagateway/files_manifest.go for the writer side and the
// canonical wire shape. This file is the inverse: CommitTable reads the
// per-table manifest, picks up the parquet path list, and feeds it into
// iceberg-go's `txn.AddFiles(ctx, paths, nil, false)`.
//
// The wire shape is deliberately the simplest thing that works: the
// catalog (lakekeeper) re-reads each parquet's footer to extract column
// statistics on its side, so the manifest itself only carries paths.
package icebergjob

import (
	"strings"
)

// FilesManifest is the per-table on-the-wire shape. One manifest blob =
// one (namespace, table); no `tables` array. Mirrors
// pkg/datagateway/tableManifestJSON exactly — the two are kept
// structurally identical on purpose; if the wire shape ever needs to
// change, update both packages in lockstep. Owning a local copy here
// (rather than importing from pkg/datagateway) keeps CommitTable's
// import graph independent of DG — the two services are deployed as
// separate binaries.
type FilesManifest struct {
	RunID     string   `json:"run_id"`
	Namespace string   `json:"namespace"`
	Table     string   `json:"table"`
	Paths     []string `json:"paths"`
}

// maxFilesManifestBytes caps how much we'll read from the manifest
// blob. files.json is normally a few KB even for a fat run (paths are
// S3 URLs of bounded length). 16 MiB is generous and bounds memory if
// a misbehaving run produces a runaway file.
const maxFilesManifestBytes = 16 << 20

// FilesManifestPath builds the canonical per-table manifest URL from a
// table-base path + run-id. Mirrors the placement chosen by
// pkg/datagateway.ResolveTableManifestPath.
//
// `tableBase` is the iceberg-go Table.Location() value (e.g.
// `s3://warehouse/<uuid>/` or `file:///warehouse/ns.db/table`). Only a
// trailing slash is stripped; the `/data` suffix is NOT stripped because
// iceberg-go table locations already represent the table root — a table
// named "data" would otherwise have its path corrupted.
//
// Returns "" when either argument is empty so callers can validate up
// front.
func FilesManifestPath(tableBase, runID string) string {
	if tableBase == "" || runID == "" {
		return ""
	}
	root := strings.TrimSuffix(tableBase, "/")
	return root + "/.run-state/" + runID + "/files.json"
}
