package icebergjob

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/rest"
	icebergtable "github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/table/compaction"

	"github.com/datuplet/datuplet/pkg/catalogwriter"
)

const (
	commitKeyProp = "datuplet.commit-key"

	defaultCompactTargetFileSize = 128 << 20
	defaultCompactMaxInputFiles  = 2000
	compactArrowBatchSize        = 32768
	compactReloadTimeout         = 30 * time.Second
)

// CompactOpts tunes CompactTable. Zero values take the defaults: 128 MiB
// target, 5 input files per group, 2000 input files per run.
type CompactOpts struct {
	TargetFileSizeBytes int64
	MinInputFiles       uint
	MaxInputFiles       int
	DryRun              bool
}

// CompactStatus is the outcome of a CompactTable call that returned no error.
type CompactStatus string

const (
	CompactCompacted        CompactStatus = "COMPACTED"
	CompactAlreadyCommitted CompactStatus = "ALREADY_COMMITTED"
	CompactNothingToDo      CompactStatus = "NOTHING_TO_DO"
	CompactSuperseded       CompactStatus = "SUPERSEDED"
	CompactDryRun           CompactStatus = "DRY_RUN"
)

// CompactResult reports one CompactTable call. Files* and Bytes* before/after
// count the table's live data files; in a dry run FilesAdded, FilesAfter and
// BytesAfter are the planner's estimates.
type CompactResult struct {
	Status                                            CompactStatus
	FilesBefore, FilesRemoved, FilesAdded, FilesAfter int
	FilesSkipped, FilesDeferred, Groups               int
	BytesBefore, BytesAfter                           int64
	SnapshotBefore, SnapshotAfter                     int64
	Duration                                          time.Duration
}

var (
	// ErrHistoryUnverifiable: the snapshots committed since planning could
	// not be read, so whether the rewrite is still safe is unknown. Nothing
	// was committed and the written files were deleted.
	ErrHistoryUnverifiable = errors.New("icebergjob: could not verify concurrent changes")
	// ErrCommitOutcomeUnknown: a commit was submitted, failed without a
	// definitive rejection, and the operation key is not on the table.
	// The written files are kept.
	ErrCommitOutcomeUnknown = errors.New("icebergjob: commit outcome unknown; written files kept")
	// ErrUnsupportedFile: a data file selected for rewrite is in a format
	// iceberg-go cannot read (e.g. ORC, Avro). Nothing was committed.
	ErrUnsupportedFile = errors.New("icebergjob: unsupported data file format")

	errSuperseded = errors.New("icebergjob: superseded")
)

// CompactOperationKey is the idempotency key of one compaction: stable
// across Job retries of a run, distinct per component and table.
func CompactOperationKey(runID, componentName, bucket, tableName string) string {
	h := sha256.New()
	for _, s := range []string{runID, componentName, bucket, tableName} {
		h.Write([]byte(strconv.Itoa(len(s))))
		h.Write([]byte{':'})
		h.Write([]byte(s))
	}
	return hex.EncodeToString(h.Sum(nil))
}

// CompactTable bin-packs the small live data files of ident's current
// snapshot per partition and commits them as one replace snapshot carrying
// auditProps, operationKey (as datuplet.commit-key) and the
// datuplet.compaction.* counters (spec 2026-11-08-table-compaction §5, §6).
//
// A snapshot already carrying operationKey yields ALREADY_COMMITTED. Before
// every commit attempt the table is reloaded and every snapshot since the
// planning baseline must be a pure append; otherwise the result is
// SUPERSEDED. Written files are deleted only when no commit was submitted
// or every submission was rejected with a 409.
func CompactTable(ctx context.Context, cat catalog.Catalog, ident icebergtable.Identifier, opts CompactOpts, auditProps iceberg.Properties, operationKey string) (*CompactResult, error) {
	start := time.Now()
	res, err := compactTable(ctx, cat, ident, opts, auditProps, operationKey)
	if res != nil {
		res.Duration = time.Since(start)
	}
	return res, err
}

func compactTable(ctx context.Context, cat catalog.Catalog, ident icebergtable.Identifier, opts CompactOpts, auditProps iceberg.Properties, operationKey string) (*CompactResult, error) {
	if operationKey == "" {
		return nil, errors.New("CompactTable: empty operation key")
	}
	if _, ok := auditProps[commitKeyProp]; ok {
		return nil, fmt.Errorf("CompactTable: auditProps must not set %s", commitKeyProp)
	}
	target := opts.TargetFileSizeBytes
	if target <= 0 {
		target = defaultCompactTargetFileSize
	}
	minInputs := opts.MinInputFiles
	if minInputs == 0 {
		minInputs = compaction.DefaultMinInputFiles
	}
	maxInputs := opts.MaxInputFiles
	if maxInputs <= 0 {
		maxInputs = defaultCompactMaxInputFiles
	}

	tbl, err := cat.LoadTable(ctx, ident)
	if err != nil {
		return nil, fmt.Errorf("CompactTable: load table %v: %w", ident, err)
	}
	if s := snapshotByCommitKey(tbl, operationKey); s != nil {
		return alreadyCommitted(s), nil
	}
	base := tbl.CurrentSnapshot()
	if base == nil {
		return &CompactResult{Status: CompactNothingToDo}, nil
	}
	res := &CompactResult{SnapshotBefore: base.SnapshotID, SnapshotAfter: base.SnapshotID}

	tasks, err := tbl.Scan().PlanFiles(ctx)
	if err != nil {
		return nil, fmt.Errorf("CompactTable: plan files: %w", err)
	}
	res.FilesBefore = len(tasks)
	for _, t := range tasks {
		res.BytesBefore += t.File.FileSizeBytes()
	}
	cfg := compaction.Config{
		TargetFileSizeBytes:  target,
		MinFileSizeBytes:     target * 3 / 4,
		MaxFileSizeBytes:     target * 9 / 5,
		MinInputFiles:        minInputs,
		DeleteFileThreshold:  math.MaxInt,
		DeleteRatioThreshold: 0,
		PackingLookback:      compaction.DefaultPackingLookback,
	}
	plan, err := cfg.PlanCompaction(tasks)
	if err != nil {
		return nil, fmt.Errorf("CompactTable: plan compaction: %w", err)
	}
	res.FilesSkipped = plan.SkippedFiles
	groups, deferred := capGroups(plan.Groups, maxInputs, int(minInputs))
	res.FilesDeferred = deferred
	res.Groups = len(groups)
	res.FilesAfter, res.BytesAfter = res.FilesBefore, res.BytesBefore
	if len(groups) == 0 {
		res.Status = CompactNothingToDo
		return res, nil
	}
	if opts.DryRun {
		res.Status = CompactDryRun
		for _, g := range groups {
			res.FilesRemoved += len(g.Tasks)
			res.FilesAdded += int((g.TotalSizeBytes + target - 1) / target)
		}
		res.FilesAfter = res.FilesBefore - res.FilesRemoved + res.FilesAdded
		return res, nil
	}

	var results []icebergtable.CompactionGroupResult
	cleanup := func() { deleteWritten(ctx, tbl, results) }
	for _, g := range groups {
		gr, err := icebergtable.ExecuteCompactionGroup(ctx, tbl,
			icebergtable.CompactionTaskGroup{PartitionKey: g.PartitionKey, Tasks: g.Tasks, TotalSizeBytes: g.TotalSizeBytes},
			icebergtable.WithCompactionTargetFileSize(target),
			icebergtable.WithCompactionScanConcurrency(1),
			icebergtable.WithCompactionArrowBatchSize(compactArrowBatchSize))
		// v1 removes no delete files: iceberg-go's safe sets include
		// position-delete files that also cover files outside the rewrite.
		gr.SafePosDeletes, gr.SafeDeletionVectors = nil, nil
		results = append(results, gr)
		if err != nil {
			cleanup()
			return nil, fmt.Errorf("CompactTable: %w", classifyUnsupported(err))
		}
	}
	var removed, added int
	var bytesRemoved, bytesAdded int64
	for _, gr := range results {
		removed += len(gr.OldDataFiles)
		added += len(gr.NewDataFiles)
		bytesRemoved += gr.BytesBefore
		bytesAdded += gr.BytesAfter
	}
	res.FilesRemoved, res.FilesAdded = removed, added

	var (
		submitted, all409 = false, true
		hit, committed    *icebergtable.Snapshot
		supersededAt      int64
	)
	runErr := catalogwriter.RetryOnConflict(ctx, catalogwriter.RetryOpts{}, func(ctx context.Context) error {
		fresh, err := cat.LoadTable(ctx, ident)
		if err != nil {
			return fmt.Errorf("%w: reload table: %w", ErrHistoryUnverifiable, err)
		}
		if s := snapshotByCommitKey(fresh, operationKey); s != nil {
			hit = s
			return errIdempotencyHit
		}
		ok, err := appendOnlySince(ctx, fresh, base.SnapshotID)
		if err != nil {
			return fmt.Errorf("%w: %w", ErrHistoryUnverifiable, err)
		}
		if !ok {
			supersededAt = snapshotIDOf(fresh)
			return errSuperseded
		}
		filesBefore, bytesBefore := liveTotals(fresh, res.FilesBefore, res.BytesBefore)
		props := iceberg.Properties{}
		for k, v := range auditProps {
			props[k] = v
		}
		props[commitKeyProp] = operationKey
		res.FilesBefore, res.BytesBefore = filesBefore, bytesBefore
		res.FilesAfter, res.BytesAfter = filesBefore-removed+added, bytesBefore-bytesRemoved+bytesAdded
		props["datuplet.compaction.files-before"] = strconv.Itoa(res.FilesBefore)
		props["datuplet.compaction.files-after"] = strconv.Itoa(res.FilesAfter)
		props["datuplet.compaction.bytes-before"] = strconv.FormatInt(res.BytesBefore, 10)
		props["datuplet.compaction.bytes-after"] = strconv.FormatInt(res.BytesAfter, 10)

		txn := noReplay(fresh, cat).NewTransaction()
		rw := txn.NewRewrite(props)
		for _, gr := range results {
			rw.ApplyResult(gr)
		}
		if err := rw.Commit(ctx); err != nil {
			if isMissingInput(err) {
				supersededAt = snapshotIDOf(fresh)
				return errSuperseded
			}
			return fmt.Errorf("stage rewrite: %w", err)
		}
		submitted = true
		out, err := txn.Commit(ctx)
		if err != nil {
			if !catalogwriter.IsCommitConflict(err) {
				all409 = false
			}
			return err
		}
		committed = out.CurrentSnapshot()
		return nil
	})

	switch {
	case runErr == nil:
		res.Status = CompactCompacted
		res.SnapshotAfter = committed.SnapshotID
		return res, nil
	case errors.Is(runErr, errIdempotencyHit):
		cleanup()
		return alreadyCommitted(hit), nil
	case errors.Is(runErr, errSuperseded):
		cleanup()
		res.Status = CompactSuperseded
		res.SnapshotAfter = supersededAt
		res.FilesRemoved, res.FilesAdded = 0, 0
		res.FilesAfter, res.BytesAfter = res.FilesBefore, res.BytesBefore
		return res, nil
	case !submitted || all409:
		cleanup()
		return nil, fmt.Errorf("CompactTable: %w", runErr)
	}

	// A submission ended without a definitive answer: keep the files and
	// look for the operation key.
	rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), compactReloadTimeout)
	defer cancel()
	fresh, err := cat.LoadTable(rctx, ident)
	if err != nil {
		return nil, fmt.Errorf("CompactTable: %w: %w (reload: %v)", ErrCommitOutcomeUnknown, runErr, err)
	}
	s := snapshotByCommitKey(fresh, operationKey)
	if s == nil {
		return nil, fmt.Errorf("CompactTable: %w: %w", ErrCommitOutcomeUnknown, runErr)
	}
	res.Status = CompactCompacted
	res.SnapshotAfter = s.SnapshotID
	return res, nil
}

// capGroups takes whole groups until maxInputs input files, truncating the
// group that crosses the cap when the rest still has minInputs files.
func classifyUnsupported(err error) error {
	if errors.Is(err, iceberg.ErrNotImplemented) {
		return fmt.Errorf("%w: %w", ErrUnsupportedFile, err)
	}
	return err
}

func capGroups(groups []compaction.Group, maxInputs, minInputs int) ([]compaction.Group, int) {
	var out []compaction.Group
	taken, deferred := 0, 0
	for _, g := range groups {
		room := maxInputs - taken
		if room < len(g.Tasks) {
			if room < minInputs {
				deferred += len(g.Tasks)
				continue
			}
			deferred += len(g.Tasks) - room
			g.Tasks = g.Tasks[:room]
			g.TotalSizeBytes = 0
			for _, t := range g.Tasks {
				g.TotalSizeBytes += t.File.FileSizeBytes()
			}
		}
		out = append(out, g)
		taken += len(g.Tasks)
	}
	return out, deferred
}

// appendOnlySince reports whether every snapshot from tbl's current snapshot
// back to (excluding) baseline is a pure append. A baseline outside the
// current ancestry (rollback, branch reset, expired snapshots) is not.
func appendOnlySince(ctx context.Context, tbl *icebergtable.Table, baseline int64) (bool, error) {
	cur := tbl.CurrentSnapshot()
	if cur == nil {
		return false, nil
	}
	between, found := icebergtable.AncestorsBetween(cur.SnapshotID, baseline, tbl.Metadata().SnapshotByID)
	if !found {
		return false, nil
	}
	if len(between) == 0 {
		return true, nil
	}
	fs, err := tbl.FS(ctx)
	if err != nil {
		return false, fmt.Errorf("open table FS: %w", err)
	}
	for _, s := range between {
		if s.Summary == nil || s.Summary.Operation != icebergtable.OpAppend {
			return false, nil
		}
		mfs, err := s.Manifests(fs)
		if err != nil {
			return false, fmt.Errorf("read manifests of snapshot %d: %w", s.SnapshotID, err)
		}
		if !pureAppend(s, mfs) {
			return false, nil
		}
	}
	return true, nil
}

// pureAppend decides from the manifests, not the summary counters (iceberg-go
// omits zero counters): an append adding no deletes manifest.
func pureAppend(s icebergtable.Snapshot, mfs []iceberg.ManifestFile) bool {
	if s.Summary == nil || s.Summary.Operation != icebergtable.OpAppend {
		return false
	}
	for _, mf := range mfs {
		if mf.SnapshotID() == s.SnapshotID && mf.ManifestContent() == iceberg.ManifestContentDeletes {
			return false
		}
	}
	return true
}

// isMissingInput recognises iceberg-go's staging error for a file to delete
// that is no longer in the table (table/transaction.go, v0.7.0).
func isMissingInput(err error) bool {
	return strings.Contains(err.Error(), "do not belong to the table")
}

// liveTotals returns the data file count and size of tbl's current snapshot
// from its summary, or the fallbacks when the summary lacks them.
func liveTotals(tbl *icebergtable.Table, files int, bytes int64) (int, int64) {
	s := tbl.CurrentSnapshot()
	if s == nil || s.Summary == nil {
		return files, bytes
	}
	if v, err := strconv.Atoi(s.Summary.Properties["total-data-files"]); err == nil {
		files = v
	}
	if v, err := strconv.ParseInt(s.Summary.Properties["total-files-size"], 10, 64); err == nil {
		bytes = v
	}
	return files, bytes
}

func alreadyCommitted(s *icebergtable.Snapshot) *CompactResult {
	res := &CompactResult{Status: CompactAlreadyCommitted, SnapshotAfter: s.SnapshotID}
	if s.ParentSnapshotID != nil {
		res.SnapshotBefore = *s.ParentSnapshotID
	}
	if s.Summary == nil {
		return res
	}
	p := s.Summary.Properties
	res.FilesBefore, _ = strconv.Atoi(p["datuplet.compaction.files-before"])
	res.FilesAfter, _ = strconv.Atoi(p["datuplet.compaction.files-after"])
	res.BytesBefore, _ = strconv.ParseInt(p["datuplet.compaction.bytes-before"], 10, 64)
	res.BytesAfter, _ = strconv.ParseInt(p["datuplet.compaction.bytes-after"], 10, 64)
	res.FilesRemoved, _ = strconv.Atoi(p["deleted-data-files"])
	res.FilesAdded, _ = strconv.Atoi(p["added-data-files"])
	return res
}

// deleteWritten removes the files the executed groups wrote, also after
// cancellation. Failures leave orphans and are only logged.
func deleteWritten(ctx context.Context, tbl *icebergtable.Table, results []icebergtable.CompactionGroupResult) {
	var paths []string
	for _, gr := range results {
		for _, df := range gr.NewDataFiles {
			paths = append(paths, df.FilePath())
		}
	}
	if len(paths) == 0 {
		return
	}
	fs, err := tbl.FS(context.WithoutCancel(ctx))
	if err != nil {
		slog.Warn("compaction: cannot open table FS to delete written files", "files", len(paths), "err", err)
		return
	}
	for _, p := range paths {
		if err := fs.Remove(p); err != nil {
			slog.Warn("compaction: delete written file", "path", p, "err", err)
		}
	}
}

func snapshotIDOf(tbl *icebergtable.Table) int64 {
	if s := tbl.CurrentSnapshot(); s != nil {
		return s.SnapshotID
	}
	return 0
}

// noReplay rebinds tbl to a catalog that turns iceberg-go's internal
// refresh-and-replay after a 409 into the 409 itself, whatever the table's
// commit.retry.num-retries: every retry must go through the history check.
func noReplay(tbl *icebergtable.Table, cat catalog.Catalog) *icebergtable.Table {
	return icebergtable.New(tbl.Identifier(), tbl.Metadata(), tbl.MetadataLocation(), tbl.FS, &noReplayCatalog{Catalog: cat})
}

type noReplayCatalog struct {
	catalog.Catalog
	conflicted bool
}

func (c *noReplayCatalog) LoadTable(ctx context.Context, ident icebergtable.Identifier) (*icebergtable.Table, error) {
	if c.conflicted {
		return nil, fmt.Errorf("%w: replay disabled for compaction", rest.ErrCommitFailed)
	}
	return c.Catalog.LoadTable(ctx, ident)
}

func (c *noReplayCatalog) CommitTable(ctx context.Context, ident icebergtable.Identifier, reqs []icebergtable.Requirement, updates []icebergtable.Update) (icebergtable.Metadata, string, error) {
	md, loc, err := c.Catalog.CommitTable(ctx, ident, reqs, updates)
	if errors.Is(err, rest.ErrCommitFailed) {
		c.conflicted = true
	}
	return md, loc, err
}
