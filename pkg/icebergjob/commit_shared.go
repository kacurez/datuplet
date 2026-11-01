// Package icebergjob — shared CommitTableFiles function.
//
// CommitTableFiles applies a caller-supplied list of data-file paths to an
// Iceberg table (APPEND or FULL_LOAD) with an idempotency key, audit
// snapshot properties and 409 retry. The Data Gateway commit pool is its
// only production caller.
package icebergjob

import (
	"context"
	"errors"
	"fmt"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	icebergtable "github.com/apache/iceberg-go/table"

	"github.com/datuplet/datuplet/pkg/catalogwriter"
)

// WriteMode specifies how data should be written to the table.
//
// Both APPEND and FULL_LOAD are implemented end-to-end against
// lakekeeper: APPEND uses iceberg-go's `txn.AddFiles`, FULL_LOAD uses
// `txn.ReplaceDataFiles`. CommitTableFiles owns the actual dispatch.
//
// UPSERT is a future addition and would extend this type.
type WriteMode string

const (
	// WriteModeAppend adds new data files to the table.
	WriteModeAppend WriteMode = "APPEND"
	// WriteModeFullLoad replaces all existing data in the table —
	// implemented via iceberg-go's `txn.ReplaceDataFiles` against
	// the table's current snapshot.
	WriteModeFullLoad WriteMode = "FULL_LOAD"
)

// CommitResult captures observability data for one CommitTableFiles call.
// Empty fields (e.g. SnapshotIDBefore on a fresh table) are valid; the
// caller surfaces this in structured logs.
type CommitResult struct {
	// SnapshotIDBefore is the table's current snapshot ID before the
	// commit, formatted as a decimal string. Empty when the table had
	// no current snapshot (initial commit).
	SnapshotIDBefore string

	// SnapshotIDAfter is the table's current snapshot ID after the
	// commit, formatted as a decimal string. Empty on success-zero
	// (no commit happened).
	SnapshotIDAfter string

	// DataFilesAdded is the number of parquet paths added by this
	// commit (len(dataPaths)). Zero on success-zero.
	DataFilesAdded int

	// WriteMode echoes the mode the caller selected, preserved for
	// downstream observability.
	WriteMode WriteMode

	// IdempotencyHit is true when the commit was skipped because a
	// snapshot with the same commit-key already existed.
	IdempotencyHit bool
}

// errIdempotencyHit is an internal sentinel that short-circuits the
// RetryOnConflict envelope when a prior attempt's snapshot is found via
// commit-key. It is NOT a commit conflict, so RetryOnConflict returns it
// immediately without retrying (pkg/catalogwriter/retry.go: `if
// !IsCommitConflict(err) { return err }`). Do not change this coupling
// without updating the retry predicate.
var errIdempotencyHit = errors.New("icebergjob: idempotency hit")

// CommitTableFiles commits dataPaths to the table at ident in the given
// mode. Used by Data Gateway's inline commit path (RFC 021). Empty
// dataPaths is success-zero.
//
// snapshotProps is forwarded to txn.AddFiles / txn.ReplaceDataFiles; pass
// the result of BuildSnapshotSummary to emit the datuplet.* audit keys.
//
// idempotencyKey, when non-empty, is (a) checked against existing
// snapshot summaries (key "datuplet.commit-key") on every attempt — a
// match short-circuits to success-zero (protects against
// committed-server-side-but-client-missed-the-response), and (b) written
// into the new snapshot summary. Callers must NOT also place
// "datuplet.commit-key" in snapshotProps.
func CommitTableFiles(
	ctx context.Context,
	cat catalog.Catalog,
	ident icebergtable.Identifier,
	dataPaths []string,
	mode WriteMode,
	snapshotProps iceberg.Properties,
	idempotencyKey string,
) (*CommitResult, error) {
	if len(dataPaths) == 0 {
		return &CommitResult{WriteMode: mode}, nil // success-zero
	}
	switch mode {
	case WriteModeAppend, WriteModeFullLoad:
	default:
		return nil, fmt.Errorf("CommitTableFiles: unsupported write mode %q", mode)
	}

	props := iceberg.Properties{}
	for k, v := range snapshotProps {
		if k == "datuplet.commit-key" {
			return nil, fmt.Errorf("CommitTableFiles: caller must not set snapshotProps[datuplet.commit-key]; pass idempotencyKey")
		}
		props[k] = v
	}
	if idempotencyKey != "" {
		props["datuplet.commit-key"] = idempotencyKey
	}

	tbl, err := cat.LoadTable(ctx, ident)
	if err != nil {
		return nil, fmt.Errorf("CommitTableFiles: load table %v: %w", ident, err)
	}
	result := CommitResult{WriteMode: mode, SnapshotIDBefore: snapshotIDOrEmpty(tbl), DataFilesAdded: len(dataPaths)}

	runErr := catalogwriter.RetryOnConflict(ctx, catalogwriter.RetryOpts{}, func(ctx context.Context) error {
		fresh, err := cat.LoadTable(ctx, ident)
		if err != nil {
			return err
		}
		if idempotencyKey != "" {
			if sid := findSnapshotByCommitKey(fresh, idempotencyKey); sid != "" {
				result.SnapshotIDAfter = sid
				return errIdempotencyHit
			}
		}
		txn := fresh.NewTransaction()
		switch mode {
		case WriteModeAppend:
			if err := txn.AddFiles(ctx, dataPaths, props, false); err != nil {
				return err
			}
		case WriteModeFullLoad:
			oldPaths, err := listCurrentSnapshotFilePaths(ctx, fresh)
			if err != nil {
				return fmt.Errorf("list current snapshot files: %w", err)
			}
			if err := txn.ReplaceDataFiles(ctx, oldPaths, dataPaths, props); err != nil {
				return err
			}
		}
		committed, err := txn.Commit(ctx)
		if err != nil {
			return err
		}
		result.SnapshotIDAfter = snapshotIDOrEmpty(committed)
		return nil
	})
	if runErr != nil {
		if errors.Is(runErr, errIdempotencyHit) {
			result.IdempotencyHit = true
			return &result, nil // success-zero, SnapshotIDAfter populated
		}
		return nil, runErr
	}
	return &result, nil
}

// findSnapshotByCommitKey scans ALL snapshots newest-to-oldest for a
// matching "datuplet.commit-key" summary value. Returns the snapshot ID
// (decimal string) or "". Full scan — no windowing. POC tables have few
// snapshots; bound it later if it ever becomes hot.
func findSnapshotByCommitKey(tbl *icebergtable.Table, key string) string {
	snaps := tbl.Metadata().Snapshots()
	for i := len(snaps) - 1; i >= 0; i-- {
		s := snaps[i]
		if s.Summary != nil && s.Summary.Properties != nil && s.Summary.Properties["datuplet.commit-key"] == key {
			return fmt.Sprintf("%d", s.SnapshotID)
		}
	}
	return ""
}

// listCurrentSnapshotFilePaths returns absolute file paths from the
// table's current snapshot manifests. Used as the `filesToDelete`
// argument to txn.ReplaceDataFiles for FULL_LOAD mode.
//
// Returns an empty slice when the table has no current snapshot
// (initial FULL_LOAD into a fresh table) — iceberg-go's
// ReplaceDataFiles handles the empty-delete + non-empty-add case by
// delegating to AddFiles internally.
func listCurrentSnapshotFilePaths(ctx context.Context, tbl *icebergtable.Table) ([]string, error) {
	if tbl == nil {
		return nil, errors.New("listCurrentSnapshotFilePaths: nil table")
	}
	snap := tbl.CurrentSnapshot()
	if snap == nil {
		return nil, nil
	}
	fs, err := tbl.FS(ctx)
	if err != nil {
		return nil, fmt.Errorf("open table FS: %w", err)
	}
	manifests, err := snap.Manifests(fs)
	if err != nil {
		return nil, fmt.Errorf("read manifest list: %w", err)
	}
	var paths []string
	for _, mf := range manifests {
		// discardDeleted=true: skip manifest entries marked DELETED so a
		// previous overwrite-style commit's tombstones don't get
		// surfaced as live data files. Mirrors iceberg-go's own
		// existingManifests() walk in snapshot_producers.go.
		entries, err := mf.FetchEntries(fs, true)
		if err != nil {
			return nil, fmt.Errorf("fetch manifest entries: %w", err)
		}
		for _, entry := range entries {
			paths = append(paths, entry.DataFile().FilePath())
		}
	}
	return paths, nil
}

// snapshotIDOrEmpty returns the table's current snapshot ID as a
// decimal string, or "" when the table has no current snapshot.
func snapshotIDOrEmpty(tbl *icebergtable.Table) string {
	if tbl == nil {
		return ""
	}
	if s := tbl.CurrentSnapshot(); s != nil {
		return fmt.Sprintf("%d", s.SnapshotID)
	}
	return ""
}
