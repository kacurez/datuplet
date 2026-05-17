package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"

	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"

	"github.com/datuplet/datuplet/pkg/lib/datalake"
)

// ErrNoMetadata is returned by ResolveCurrentMetadata when a table
// directory has no loadable metadata file. Callers treat this as a
// signal to omit the table from catalog listings.
var ErrNoMetadata = errors.New("no committed metadata")

// TableRef identifies a committed Iceberg table inside a Datuplet
// warehouse. It carries both the catalog coordinate (namespace +
// name) and the absolute URI of the current metadata.json plus its
// current-snapshot ID for quick display.
type TableRef struct {
	Namespace         string
	Name              string
	MetadataLocation  string
	CurrentSnapshotID int64
}

// metadataVersionRe matches v{N}.metadata.json[.gz] with a numeric
// group so we can sort by integer version (v10 beats v9).
var metadataVersionRe = regexp.MustCompile(`^v(\d+)\.metadata\.json(\.gz)?$`)

// ResolveCurrentMetadata picks the committed-current metadata.json for
// the table whose directory lives at tablePrefix, a path RELATIVE to
// the warehouse root (e.g. "orgs/myorg/projects/<uuid>/tables/public/simple").
// Resolution order:
//
//  1. If metadata/version-hint.text exists and parses as an integer N,
//     try metadata/v{N}.metadata.json; if it loads, return its
//     absolute URI (scheme-prefixed, joined from warehouseURI).
//  2. Otherwise list files matching ^v(\d+)\.metadata\.json(\.gz)?$ and
//     try them in descending numeric-version order; return the first
//     that loads via table.NewFromLocation.
//
// "Loadable" means iceberg-go can parse the metadata.json header — it
// does NOT require data files to be readable. A directory with no
// loadable candidates returns ErrNoMetadata.
//
// warehouseURI is the full scheme-prefixed root ("file:///.." or
// "s3://bucket") and is used solely to build the returned absolute
// metadata URI and to pass to iceberg-go's table.NewFromLocation for
// load-verification. s3Props is the iceberg-go property map needed
// for S3 warehouses; nil for file://.
func ResolveCurrentMetadata(ctx context.Context, dl datalake.DataLake, warehouseURI, tablePrefix string, s3Props map[string]string) (string, error) {
	if dl == nil {
		return "", fmt.Errorf("ResolveCurrentMetadata: nil DataLake")
	}
	if tablePrefix == "" {
		return "", fmt.Errorf("ResolveCurrentMetadata: empty tablePrefix")
	}
	metaPrefix := path.Join(tablePrefix, "metadata")

	// Strategy 1: version-hint.text.
	if hint, ok := readVersionHint(ctx, dl, metaPrefix); ok {
		name := fmt.Sprintf("v%d.metadata.json", hint)
		candidate := joinURI(warehouseURI, path.Join(metaPrefix, name))
		if loadable(ctx, candidate, s3Props) {
			return candidate, nil
		}
	}

	// Strategy 2: numeric-sorted scan, first loadable wins.
	files, err := listMetadataFiles(ctx, dl, metaPrefix)
	if err != nil {
		return "", err
	}
	for _, f := range files {
		candidate := joinURI(warehouseURI, path.Join(metaPrefix, f.name))
		if loadable(ctx, candidate, s3Props) {
			return candidate, nil
		}
	}
	return "", ErrNoMetadata
}

// ListTables walks orgs/<org>/projects/<pid>/tables/<ns>/<name>/ under
// the DataLake, resolves the current metadata for each candidate
// table, and returns those with loadable metadata. Tables that fail
// identifier validation or return ErrNoMetadata are silently omitted.
//
// warehouseURI is the full scheme-prefixed warehouse root; it is used
// only to build the absolute metadata_location URIs on the returned
// TableRef and to feed iceberg-go's table.NewFromLocation for metadata
// loading. Directory enumeration happens through the DataLake
// abstraction, which is backend-agnostic (file:// + s3://).
func ListTables(ctx context.Context, dl datalake.DataLake, warehouseURI, org, projectID string, s3Props map[string]string) ([]TableRef, error) {
	if dl == nil {
		return nil, fmt.Errorf("ListTables: nil DataLake")
	}
	if !ValidIdentifier(org) {
		return nil, fmt.Errorf("ListTables: invalid org %q", org)
	}
	// projectID is a UUID — not required to pass ValidIdentifier, but
	// must not contain path separators or traversal.
	if projectID == "" || strings.ContainsAny(projectID, "/\\") || strings.Contains(projectID, "..") {
		return nil, fmt.Errorf("ListTables: invalid projectID %q", projectID)
	}

	tablesPrefix := path.Join("orgs", org, "projects", projectID, "tables")
	// Append a trailing "/" before calling List so S3/MinIO's prefix
	// match doesn't pick up sibling paths that happen to share the
	// tablesPrefix stem (e.g. ".../tables_backup/…" or ".../tables2/…").
	// TrimPrefix below uses the same slashed prefix so the key parsing
	// stays aligned.
	listPrefix := tablesPrefix + "/"

	// List everything under tablesPrefix; both MinIO + filesystem
	// backends list recursively and return object paths. We derive
	// (namespace, table) pairs from the first two segments after the
	// prefix. Using a set dedupes multiple files per (ns, table) dir.
	objs, err := dl.List(ctx, listPrefix)
	if err != nil {
		return nil, fmt.Errorf("list tables root: %w", err)
	}

	type nsTable struct {
		ns   string
		name string
	}
	seen := map[nsTable]struct{}{}
	for _, o := range objs {
		if o.IsDir {
			continue
		}
		// FilesystemDataLake returns paths relative to the warehouse
		// root; MinIODataLake returns object keys (no bucket prefix).
		// Both start with the same (slashed) listPrefix we passed in.
		rel, ok := strings.CutPrefix(o.Path, listPrefix)
		if !ok {
			// Shouldn't happen — List contract is prefix-matched keys —
			// but a defensive skip beats a panic on mis-aligned backends.
			continue
		}
		if rel == "" {
			continue
		}
		parts := strings.SplitN(rel, "/", 3)
		if len(parts) < 2 {
			continue
		}
		ns, name := parts[0], parts[1]
		if !ValidIdentifier(ns) || !ValidIdentifier(name) {
			continue
		}
		seen[nsTable{ns: ns, name: name}] = struct{}{}
	}

	var out []TableRef
	for k := range seen {
		tablePrefix := path.Join(tablesPrefix, k.ns, k.name)
		metaURI, err := ResolveCurrentMetadata(ctx, dl, warehouseURI, tablePrefix, s3Props)
		if err != nil {
			if errors.Is(err, ErrNoMetadata) {
				continue
			}
			return nil, fmt.Errorf("resolve %s.%s: %w", k.ns, k.name, err)
		}
		var snapID int64
		if tbl, err := loadTable(ctx, metaURI, s3Props); err == nil {
			if snap := tbl.CurrentSnapshot(); snap != nil {
				snapID = snap.SnapshotID
			}
		}
		out = append(out, TableRef{
			Namespace:         k.ns,
			Name:              k.name,
			MetadataLocation:  metaURI,
			CurrentSnapshotID: snapID,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Namespace != out[j].Namespace {
			return out[i].Namespace < out[j].Namespace
		}
		return out[i].Name < out[j].Name
	})
	return out, nil
}

// loadTable parses metadataURI as an Iceberg table via iceberg-go.
// Only the metadata JSON header is validated — data files do not need
// to be readable.
func loadTable(ctx context.Context, metadataURI string, s3Props map[string]string) (*table.Table, error) {
	return table.NewFromLocation(
		ctx,
		table.Identifier{path.Base(metadataURI)},
		metadataURI,
		func(ctx context.Context) (iceio.IO, error) {
			return LoadFS(ctx, metadataURI, s3Props)
		},
		nil,
	)
}

// loadable reports whether metadataURI parses as an Iceberg table.
func loadable(ctx context.Context, metadataURI string, s3Props map[string]string) bool {
	_, err := loadTable(ctx, metadataURI, s3Props)
	return err == nil
}

// readVersionHint returns the integer N parsed from
// <metaPrefix>/version-hint.text, or (_, false) if the file is
// missing, unreadable, or not an integer. Whitespace is trimmed.
// metaPrefix is a path relative to the DataLake root.
func readVersionHint(ctx context.Context, dl datalake.DataLake, metaPrefix string) (int, bool) {
	rc, err := dl.Read(ctx, path.Join(metaPrefix, "version-hint.text"), -1, -1)
	if err != nil {
		return 0, false
	}
	defer rc.Close()
	// version-hint.text is at most a few bytes — cap at 32 to keep
	// a malformed file from blowing up memory while staying generous
	// enough for comments/whitespace.
	data, err := io.ReadAll(io.LimitReader(rc, 32))
	if err != nil {
		return 0, false
	}
	s := strings.TrimSpace(string(data))
	n, err := strconv.Atoi(s)
	if err != nil || n <= 0 {
		return 0, false
	}
	return n, true
}

// metadataCandidate is one v{N}.metadata.json[.gz] file, kept with its
// parsed version so we can sort descending without reparsing.
type metadataCandidate struct {
	name    string
	version int
}

// listMetadataFiles returns v{N}.metadata.json[.gz] entries in
// descending numeric-version order. metaPrefix is a path relative to
// the DataLake root.
func listMetadataFiles(ctx context.Context, dl datalake.DataLake, metaPrefix string) ([]metadataCandidate, error) {
	// Trailing slash so S3's prefix match doesn't accidentally include
	// sibling paths like "<table>/metadata_backup/..." next to the real
	// "<table>/metadata/..." directory. No-op for FilesystemDataLake
	// (toLocalPath strips/normalises it) but keeps S3 correct.
	entries, err := dl.List(ctx, metaPrefix+"/")
	if err != nil {
		return nil, fmt.Errorf("read metadata dir: %w", err)
	}
	var out []metadataCandidate
	for _, e := range entries {
		if e.IsDir {
			continue
		}
		// Both backends return the full path (relative to root or to
		// bucket); we only care about the file name.
		name := path.Base(e.Path)
		m := metadataVersionRe.FindStringSubmatch(name)
		if m == nil {
			continue
		}
		n, err := strconv.Atoi(m[1])
		if err != nil {
			continue
		}
		// Dedupe: MinIO can in theory return the same key twice under
		// concurrent edits. Nothing in our list flow depends on strict
		// ordering, so we just skip duplicates.
		duplicate := false
		for _, existing := range out {
			if existing.name == name {
				duplicate = true
				break
			}
		}
		if !duplicate {
			out = append(out, metadataCandidate{name: name, version: n})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].version > out[j].version
	})
	return out, nil
}

// joinURI appends segment to base with exactly one separator.
// base must be a scheme://... URI; never use filepath.Join on URIs —
// it corrupts scheme://host into scheme:/host on systems with / as
// separator when base starts with scheme:// (macOS/Linux case is ok
// but the invariant is clearer if we keep URL joining to string work).
func joinURI(base, segment string) string {
	if segment == "" {
		return base
	}
	if strings.HasSuffix(base, "/") {
		return base + segment
	}
	return base + "/" + segment
}
