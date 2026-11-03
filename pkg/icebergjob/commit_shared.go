// Package icebergjob — shared CommitTable function.
//
// CommitTable is the manifest-path variant retained for out-of-band
// files.json consumers; it has no in-repo production caller since RFC 021
// (the DG commit pool calls CommitTableFiles directly).
//
// CommitTable is a *pure consumer* of the `files.json` wire shape — it
// does not assume the writer was DG, and does not couple to any
// caller-side mode plumbing beyond the WriteMode argument.
//
// Manifest-reading responsibility lives INSIDE CommitTable: the
// signature accepts an opaque `manifestPath string`, and CommitTable
// reads through iceberg-go's per-table `Table.FS()` handle so reads use
// the same lakekeeper-vended STS creds the writer used. (Production
// `manifestPath` is an absolute URL — `s3://…` or `file://…` — derived
// by the caller from `Table.Location()` via FilesManifestPath.)
package icebergjob

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	iofs "io/fs"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/parquet/file"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	iceio "github.com/apache/iceberg-go/io"
	icebergtable "github.com/apache/iceberg-go/table"

	"github.com/datuplet/datuplet/pkg/catalogwriter"
)

// WriteMode specifies how data should be written to the table.
//
// Both APPEND and FULL_LOAD are implemented end-to-end against
// lakekeeper: APPEND uses iceberg-go's `txn.AddFiles`, FULL_LOAD uses
// `txn.ReplaceDataFiles`. CommitTable / CommitTableFiles own the
// actual dispatch.
//
// UPSERT is a future addition and would extend this type plus the
// files.json schema (delete_paths) without changing the locked signature.
type WriteMode string

const (
	// WriteModeAppend adds new data files to the table.
	WriteModeAppend WriteMode = "APPEND"
	// WriteModeFullLoad replaces all existing data in the table —
	// implemented via iceberg-go's `txn.ReplaceDataFiles` against
	// the table's current snapshot.
	WriteModeFullLoad WriteMode = "FULL_LOAD"
)

// ErrManifestEmpty signals that the manifest exists but its DataPaths
// list is empty. CommitTable catches this internally and returns nil
// (success-zero) — exposed here for callers that want to introspect a
// returned wrapped error if they ever bypass the success-zero behavior.
var ErrManifestEmpty = errors.New("icebergjob: manifest empty")

// ErrManifestMissing signals that the manifest file does not exist.
// CommitTable catches this internally and returns nil (success-zero) —
// matches the existing post-stage behavior where a stage that produced
// no parquet for a target table is a no-op rather than an error.
var ErrManifestMissing = errors.New("icebergjob: manifest missing")

// CommitResult captures observability data for one CommitTable call.
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
	// commit (manifest.DataPaths len). Zero on success-zero.
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

// filesManifest is the on-the-wire shape of the per-table `files.json`
// manifest, supporting both v1 (`paths`) and v2 (`data_paths` +
// `delete_paths` for future UPSERT).
//
// The reader auto-migrates v1 to v2 internally: when SchemaVersion is
// absent (v1), Paths is copied into DataPaths. Downstream callers see
// only the v2 shape via DataPaths / DeletePaths.
type filesManifest struct {
	SchemaVersion int      `json:"schema_version,omitempty"`
	RunID         string   `json:"run_id"`
	Namespace     string   `json:"namespace"`
	Table         string   `json:"table"`
	Paths         []string `json:"paths,omitempty"`        // v1
	DataPaths     []string `json:"data_paths,omitempty"`   // v2
	DeletePaths   []string `json:"delete_paths,omitempty"` // v2 (UPSERT)
}

// CommitTable reads files.json from manifestPath, opens an iceberg-go
// transaction against (cat, ident), applies AddFiles/ReplaceDataFiles
// according to mode (and AddDeletes for UPSERT once that lands), and
// commits via the catalog. Pure consumer of the manifest format —
// indifferent to which writer produced it.
//
// snapshotProps is forwarded verbatim as the snapshotProps argument to
// txn.AddFiles / txn.ReplaceDataFiles. Pass nil to write no extra keys.
// Pass the result of BuildSnapshotSummary to emit the four datuplet.*
// audit keys.
//
// Errors:
//   - ErrManifestEmpty (manifestPath exists but data_paths==[]) →
//     CommitTable returns nil (success-zero, no-op commit).
//   - ErrManifestMissing (manifestPath not present) → CommitTable
//     returns nil (success-zero; the run produced no parquet for this
//     target).
//   - All other errors propagate.
//
// The retry envelope: catalogwriter.RetryOnConflict wraps the txn open
// + commit so iceberg-go's REST 409 (rest.ErrCommitFailed, surfaced
// when a concurrent commit lands between attempts) is retried with
// exponential backoff.
func CommitTable(
	ctx context.Context,
	cat catalog.Catalog,
	ident icebergtable.Identifier,
	manifestPath string,
	mode WriteMode,
	snapshotProps iceberg.Properties,
) (*CommitResult, error) {
	tbl, err := cat.LoadTable(ctx, ident)
	if err != nil {
		return nil, fmt.Errorf("CommitTable: load table %v: %w", ident, err)
	}
	m, err := readManifestFromTableFS(ctx, tbl, manifestPath)
	if errors.Is(err, ErrManifestMissing) || errors.Is(err, ErrManifestEmpty) {
		return &CommitResult{WriteMode: mode}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("CommitTable: read manifest %s: %w", manifestPath, err)
	}
	return CommitTableFiles(ctx, cat, ident, m.DataPaths, nil, mode, snapshotProps, "")
}

// PartitionedFiles carries the partition tuples of files written to a
// partitioned table (spec decision 11): the spec id they were written under
// and, parallel to the data paths, each file's tuple (partition field id ->
// value, nil for a null; an entry for every field of the spec).
type PartitionedFiles struct {
	SpecID int
	Tuples []map[int]any
}

// txnAddFiles is the unpartitioned APPEND commit (a seam for tests).
var txnAddFiles = func(ctx context.Context, txn *icebergtable.Transaction, paths []string, props iceberg.Properties) error {
	return txn.AddFiles(ctx, paths, props, false)
}

// assignFirstRowIDs sets each file's first_row_id to next plus the record
// counts of the files before it, in CommitJob path order (a seam for tests).
var assignFirstRowIDs = func(builders []*iceberg.DataFileBuilder, files []iceberg.DataFile, next int64) {
	for i, b := range builders {
		b.FirstRowID(next)
		next += files[i].Count()
	}
}

// CommitTableFiles is the in-memory-paths variant of CommitTable: the
// caller supplies dataPaths directly instead of via a files.json URL.
// Used by Data Gateway's inline commit path (RFC 021).
//
// part is nil for files of an unpartitioned spec: APPEND uses AddFiles and
// FULL_LOAD ReplaceDataFiles. For a partitioned spec the DataFiles are built
// once from each file's footer plus its explicit tuple, and every attempt
// checks that the table's default spec is still part.SpecID before
// AddDataFiles / ReplaceDataFilesWithDataFiles. On a v3 table every attempt
// then fails, unretried, when the fresh table's commit.retry.num-retries is
// above 0, and otherwise assigns each file first_row_id from the fresh
// table's next-row-id. FULL_LOAD replaces the current snapshot's data files
// (never its delete files) on both paths.
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
	part *PartitionedFiles,
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

	var dataFiles []iceberg.DataFile
	var builders []*iceberg.DataFileBuilder
	if part != nil {
		if dataFiles, builders, err = buildDataFiles(ctx, tbl, dataPaths, part); err != nil {
			return nil, err
		}
	}

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
		if part != nil {
			if cur := fresh.Metadata().DefaultPartitionSpec(); cur != part.SpecID {
				return fmt.Errorf("partition spec changed during the run (spec id %d → %d)", part.SpecID, cur)
			}
			if fresh.Metadata().Version() >= 3 {
				if n := fresh.Properties().GetUInt64(icebergtable.CommitNumRetriesKey, icebergtable.CommitNumRetriesDefault); n > 0 {
					return fmt.Errorf("table %v has %s=%d: partitioned v3 commits assign explicit first_row_ids, which iceberg-go's internal retry does not re-base; set it to 0",
						ident, icebergtable.CommitNumRetriesKey, n)
				}
				assignFirstRowIDs(builders, dataFiles, fresh.Metadata().NextRowID())
			}
		}
		txn := fresh.NewTransaction()
		switch mode {
		case WriteModeAppend:
			if part != nil {
				if err := txn.AddDataFiles(ctx, dataFiles, props); err != nil {
					return err
				}
			} else if err := txnAddFiles(ctx, txn, dataPaths, props); err != nil {
				return err
			}
		case WriteModeFullLoad:
			old, err := currentSnapshotDataFiles(ctx, fresh)
			if err != nil {
				return fmt.Errorf("list current snapshot files: %w", err)
			}
			if part != nil {
				if err := txn.ReplaceDataFilesWithDataFiles(ctx, old, dataFiles, props); err != nil {
					return err
				}
				break
			}
			oldPaths := make([]string, len(old))
			for i, df := range old {
				oldPaths[i] = df.FilePath()
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

// isNotFoundErr is a best-effort detector for "object not found" errors
// surfaced by iceberg-go's FS abstraction. The iceberg-go library
// doesn't expose a sentinel for s3 NoSuchKey or filesystem
// os.ErrNotExist — both bubble up as wrapped errors with provider-
// specific messages. We match on substring rather than type because
// that's what the surface gives us today; if iceberg-go grows a real
// sentinel in the future this is the one place to switch over.
//
// Backends seen in the wild:
//   - s3:        `NoSuchKey`, `404`, "key does not exist"
//   - GCS:       "object doesn't exist" (note the contraction — doesn't,
//     so `strings.Contains(msg, "not exist")` does NOT match),
//     and a gocloud `(code=NotFound)` suffix on the wrapped error
//   - localfs:   "no such file or directory"
//   - iceberg-go's own paths: "not found"
func isNotFoundErr(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "not exist") ||
		strings.Contains(msg, "doesn't exist") || // GCS via gocloud.dev
		strings.Contains(msg, "code=notfound") || // gocloud.dev wrapping
		strings.Contains(msg, "not found") ||
		strings.Contains(msg, "nosuchkey") ||
		strings.Contains(msg, "404") ||
		strings.Contains(msg, "no such file")
}

// readManifestFromTableFS reads the per-table files.json blob via the
// table's iceberg-go FS handle (so the read uses lakekeeper-vended
// STS creds, mirroring the writer side). Returns ErrManifestMissing
// when the path is not found (best-effort detection via isNotFoundErr,
// which matches the legacy commitTable behavior). Returns
// ErrManifestEmpty when the manifest's DataPaths slice is empty after
// v1→v2 migration.
func readManifestFromTableFS(ctx context.Context, tbl *icebergtable.Table, manifestPath string) (*filesManifest, error) {
	if tbl == nil {
		return nil, errors.New("readManifestFromTableFS: nil table")
	}
	if manifestPath == "" {
		return nil, errors.New("readManifestFromTableFS: empty manifestPath")
	}
	fs, err := tbl.FS(ctx)
	if err != nil {
		return nil, fmt.Errorf("open table FS: %w", err)
	}
	rdr, err := fs.Open(manifestPath)
	if err != nil {
		if isNotFoundErr(err) {
			return nil, ErrManifestMissing
		}
		return nil, fmt.Errorf("open manifest %s: %w", manifestPath, err)
	}
	defer rdr.Close()
	body, err := io.ReadAll(io.LimitReader(rdr, maxFilesManifestBytes))
	if err != nil {
		return nil, fmt.Errorf("read manifest body: %w", err)
	}
	return parseFilesManifest(body)
}

// parseFilesManifest decodes the bytes into a v2-shaped filesManifest,
// auto-migrating v1 (`paths`) to v2 (`data_paths`).
// Returns ErrManifestEmpty when the resulting DataPaths is empty so
// the caller can short-circuit to success-zero.
func parseFilesManifest(body []byte) (*filesManifest, error) {
	m := &filesManifest{}
	if err := json.Unmarshal(body, m); err != nil {
		return nil, fmt.Errorf("decode files manifest: %w", err)
	}
	// v1 → v2 migration: SchemaVersion absent (==0) and Paths populated
	// → copy Paths into DataPaths so the rest of the function sees only
	// the v2 shape. This is a no-op for v2 manifests where Paths is
	// already absent.
	if m.SchemaVersion == 0 && len(m.Paths) > 0 && len(m.DataPaths) == 0 {
		m.DataPaths = m.Paths
	}
	if len(m.DataPaths) == 0 {
		return nil, ErrManifestEmpty
	}
	return m, nil
}

// buildDataFiles builds the DataFile of each path once, before any commit
// attempt: metrics from the file's footer (table.FileToDataFile with the
// unpartitioned spec, as AddFiles computes them) plus the file's explicit
// tuple, under the spec the files were written with. Tuples are never
// inferred from statistics (FileToDataFile panics for bucket; truncated
// string bounds are unsafe). The builders are returned too: Build returns the
// builder's own DataFile, so a v3 commit sets first_row_id on each attempt.
func buildDataFiles(ctx context.Context, tbl *icebergtable.Table, paths []string, part *PartitionedFiles) ([]iceberg.DataFile, []*iceberg.DataFileBuilder, error) {
	if len(part.Tuples) != len(paths) {
		return nil, nil, fmt.Errorf("CommitTableFiles: %d partition tuples for %d files", len(part.Tuples), len(paths))
	}
	spec := tbl.Metadata().PartitionSpecByID(part.SpecID)
	if spec == nil {
		return nil, nil, fmt.Errorf("partition spec changed during the run (spec id %d → %d)", part.SpecID, tbl.Metadata().DefaultPartitionSpec())
	}
	sc := tbl.Schema()
	fs, err := tbl.FS(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("open table FS: %w", err)
	}
	props := iceberg.Properties{}
	for k, v := range tbl.Properties() {
		props[k] = v
	}
	if _, ok := props[icebergtable.DefaultNameMappingKey]; !ok {
		// As AddFiles: footers without field ids map by name.
		m, err := json.Marshal(sc.NameMapping())
		if err != nil {
			return nil, nil, err
		}
		props[icebergtable.DefaultNameMappingKey] = string(m)
	}
	logical := partitionLogicalTypes(*spec, sc)

	out := make([]iceberg.DataFile, len(paths))
	builders := make([]*iceberg.DataFileBuilder, len(paths))
	errs := make([]error, len(paths))
	sem := make(chan struct{}, runtime.GOMAXPROCS(0))
	var wg sync.WaitGroup
	for i, path := range paths {
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer func() { <-sem; wg.Done() }()
			stats, size, err := footerMetrics(ctx, fs, path, sc, props)
			if err != nil {
				errs[i] = fmt.Errorf("read footer of %s: %w", path, err)
				return
			}
			b, err := iceberg.NewDataFileBuilder(*spec, iceberg.EntryContentData, path, iceberg.ParquetFile,
				part.Tuples[i], logical, nil, stats.Count(), size)
			if err != nil {
				errs[i] = fmt.Errorf("data file %s: %w", path, err)
				return
			}
			if lb := stats.LowerBoundValues(); len(lb) > 0 {
				b.LowerBoundValues(lb)
			}
			if ub := stats.UpperBoundValues(); len(ub) > 0 {
				b.UpperBoundValues(ub)
			}
			b.ColumnSizes(stats.ColumnSizes()).ValueCounts(stats.ValueCounts()).
				NullValueCounts(stats.NullValueCounts()).NaNValueCounts(stats.NaNValueCounts()).
				SplitOffsets(stats.SplitOffsets())
			out[i] = b.Build()
			builders[i] = b
		}()
	}
	wg.Wait()
	if err := errors.Join(errs...); err != nil {
		return nil, nil, err
	}
	return out, builders, nil
}

// footerMetrics returns table.FileToDataFile's metrics (unpartitioned spec)
// for the Parquet file at path, computed over its non-empty row groups only,
// and the file's size. A 0-row row group (the gateway's partitioned writer
// can end a file with one after a budget flush) has no column statistics,
// and iceberg-go would then drop every column's bounds and null counts for
// the whole file. The footer is read once; FileToDataFile runs on a copy of
// it, without the empty row groups, served from memory.
func footerMetrics(ctx context.Context, fs iceio.IO, path string, sc *iceberg.Schema, props iceberg.Properties) (df iceberg.DataFile, size int64, err error) {
	// A malformed footer must fail this file, never the commit worker.
	defer func() {
		if r := recover(); r != nil {
			df, size, err = nil, 0, fmt.Errorf("parquet footer: %v", r)
		}
	}()
	f, err := fs.Open(path)
	if err != nil {
		return nil, 0, err
	}
	// Closed once, also when the reader constructor panics (the reader's
	// Close only closes f).
	defer f.Close()
	rdr, err := file.NewParquetReader(f)
	if err != nil {
		return nil, 0, err
	}
	md := rdr.MetaData()
	// An encrypted file with a plaintext footer opens without keys; its
	// footer cannot be re-serialized without an encryptor.
	if md.IsSetEncryptionAlgorithm() || md.FileDecryptor != nil {
		return nil, 0, errors.New("unsupported encrypted parquet file")
	}
	size = md.GetSourceFileSize()
	keep := make([]int, 0, md.NumRowGroups())
	for i := 0; i < md.NumRowGroups(); i++ {
		if md.RowGroup(i).NumRows() > 0 {
			keep = append(keep, i)
		}
	}
	if len(keep) < md.NumRowGroups() {
		if md, err = md.Subset(keep); err != nil {
			return nil, 0, err
		}
	}
	var footer bytes.Buffer
	footer.WriteString("PAR1")
	n, err := md.WriteTo(&footer, nil)
	if err != nil {
		return nil, 0, fmt.Errorf("serialize footer: %w", err)
	}
	footer.Write(binary.LittleEndian.AppendUint32(nil, uint32(n)))
	footer.WriteString("PAR1")
	stats, err := icebergtable.FileToDataFile(ctx, footerIO{path: path, data: footer.Bytes()}, path, sc, *iceberg.UnpartitionedSpec, 0, props)
	if err != nil {
		return nil, 0, err
	}
	return stats, size, nil
}

// footerIO serves one Parquet footer (no column data) as the file at path;
// FileToDataFile reads nothing else.
type footerIO struct {
	path string
	data []byte
}

func (o footerIO) Open(name string) (iceio.File, error) {
	if name != o.path {
		return nil, &iofs.PathError{Op: "open", Path: name, Err: iofs.ErrNotExist}
	}
	return &footerFile{Reader: bytes.NewReader(o.data), name: name, size: int64(len(o.data))}, nil
}

func (o footerIO) Remove(name string) error {
	return &iofs.PathError{Op: "remove", Path: name, Err: iofs.ErrPermission}
}

type footerFile struct {
	*bytes.Reader
	name string
	size int64
}

func (f *footerFile) Stat() (iofs.FileInfo, error) { return footerInfo{f.name, f.size}, nil }
func (f *footerFile) Close() error                 { return nil }

type footerInfo struct {
	name string
	size int64
}

func (i footerInfo) Name() string        { return i.name }
func (i footerInfo) Size() int64         { return i.size }
func (i footerInfo) Mode() iofs.FileMode { return 0o444 }
func (i footerInfo) ModTime() time.Time  { return time.Time{} }
func (i footerInfo) IsDir() bool         { return false }
func (i footerInfo) Sys() any            { return nil }

// partitionLogicalTypes maps each partition field whose result type is
// encoded as an Avro logical type to that type, as iceberg-go's own
// DataFile construction does.
func partitionLogicalTypes(spec iceberg.PartitionSpec, sc *iceberg.Schema) map[int]string {
	out := map[int]string{}
	for _, f := range spec.Fields() {
		src, ok := sc.FindFieldByID(f.SourceID())
		if !ok {
			continue
		}
		switch f.Transform.ResultType(src.Type).(type) {
		case iceberg.DateType:
			out[f.FieldID] = "date"
		case iceberg.TimeType:
			out[f.FieldID] = "time-micros"
		case iceberg.TimestampType, iceberg.TimestampTzType:
			out[f.FieldID] = "timestamp-micros"
		case iceberg.DecimalType:
			out[f.FieldID] = "decimal"
		case iceberg.UUIDType:
			out[f.FieldID] = "uuid"
		}
	}
	return out
}

// currentSnapshotDataFiles returns the data files (never position or
// equality delete files) of the table's current snapshot: FULL_LOAD's
// files to replace.
//
// Returns an empty slice when the table has no current snapshot
// (initial FULL_LOAD into a fresh table) — iceberg-go's
// ReplaceDataFiles / ReplaceDataFilesWithDataFiles handle the
// empty-delete + non-empty-add case by delegating to an append.
func currentSnapshotDataFiles(ctx context.Context, tbl *icebergtable.Table) ([]iceberg.DataFile, error) {
	if tbl == nil {
		return nil, errors.New("currentSnapshotDataFiles: nil table")
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
	var files []iceberg.DataFile
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
			if df := entry.DataFile(); df.ContentType() == iceberg.EntryContentData {
				files = append(files, df)
			}
		}
	}
	return files, nil
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
