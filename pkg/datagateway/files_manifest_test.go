package datagateway

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/datuplet/datuplet/pkg/datagateway/backend"
)

// TestFilesManifest_AppendConcurrent exercises the concurrent-append
// contract. N goroutines × M paths each, spread across two tables.
// After the dust settles every (ns, tbl) bucket must surface its full
// path set via MarshalTableJSON, the per-table doc must round-trip,
// and Tables() must return entries sorted by "<ns>.<tbl>".
func TestFilesManifest_AppendConcurrent(t *testing.T) {
	const goroutines = 32
	const perGoroutine = 50

	m := NewFilesManifest("run-abc")

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		g := g
		go func() {
			defer wg.Done()
			ns, tbl := "events", "users"
			if g%2 == 1 {
				ns, tbl = "logs", "audit"
			}
			for i := 0; i < perGoroutine; i++ {
				path := fmt.Sprintf("s3://datuplet/%s/%s/data/g%02d-p%03d.parquet", ns, tbl, g, i)
				m.Append(ns, tbl, path)
			}
		}()
	}
	wg.Wait()

	tables := m.Tables()
	if len(tables) != 2 {
		t.Fatalf("Tables(): got %d, want 2 (got: %+v)", len(tables), tables)
	}
	// Sorted by "<ns>.<tbl>": "events.users" < "logs.audit"
	if tables[0].Namespace != "events" || tables[1].Namespace != "logs" {
		t.Errorf("table order not sorted: got [%s.%s, %s.%s]",
			tables[0].Namespace, tables[0].Table,
			tables[1].Namespace, tables[1].Table)
	}

	// Per-table: round-trip MarshalTableJSON.
	totalPaths := 0
	for _, want := range tables {
		body, ok, err := m.MarshalTableJSON(want.Namespace, want.Table)
		if err != nil || !ok {
			t.Fatalf("MarshalTableJSON(%s.%s) ok=%v err=%v", want.Namespace, want.Table, ok, err)
		}
		var doc tableManifestJSON
		if err := json.Unmarshal(body, &doc); err != nil {
			t.Fatalf("decode %s.%s: %v\n%s", want.Namespace, want.Table, err, body)
		}
		if doc.RunID != "run-abc" {
			t.Errorf("run_id = %q, want run-abc", doc.RunID)
		}
		if doc.Namespace != want.Namespace || doc.Table != want.Table {
			t.Errorf("doc identifiers = %s.%s, want %s.%s", doc.Namespace, doc.Table, want.Namespace, want.Table)
		}
		// Every path must carry the right (ns, tbl) prefix.
		wantPrefix := fmt.Sprintf("s3://datuplet/%s/%s/", want.Namespace, want.Table)
		for _, p := range doc.Paths {
			if !strings.HasPrefix(p, wantPrefix) {
				t.Errorf("path %q does not match expected prefix %q", p, wantPrefix)
			}
		}
		totalPaths += len(doc.Paths)
	}
	if totalPaths != goroutines*perGoroutine {
		t.Errorf("total paths = %d, want %d", totalPaths, goroutines*perGoroutine)
	}
}

// Tables come back from Tables() alphabetically by "<ns>.<tbl>" regardless of
// append order, paths within a table preserve append order, and an empty path
// is ignored.
func TestFilesManifest_Tables(t *testing.T) {
	type app struct{ ns, tbl, path string }
	tests := []struct {
		name      string
		appends   []app
		wantKeys  []string
		wantPaths map[string][]string // by "<ns>.<tbl>"
	}{
		{
			name: "deterministic table order, append order within a table",
			appends: []app{
				{"zeta", "z", "s3://b/zeta/z/data/p1.parquet"},
				{"alpha", "a", "s3://b/alpha/a/data/p1.parquet"},
				{"alpha", "a", "s3://b/alpha/a/data/p2.parquet"},
				{"alpha", "b", "s3://b/alpha/b/data/p1.parquet"},
			},
			wantKeys: []string{"alpha.a", "alpha.b", "zeta.z"},
			wantPaths: map[string][]string{
				"alpha.a": {"s3://b/alpha/a/data/p1.parquet", "s3://b/alpha/a/data/p2.parquet"},
			},
		},
		{
			name:     "empty path is skipped",
			appends:  []app{{"ns", "tbl", ""}},
			wantKeys: []string{},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := NewFilesManifest("run-1")
			for _, a := range tc.appends {
				m.Append(a.ns, a.tbl, a.path)
			}
			tables := m.Tables()
			gotKeys := make([]string, 0, len(tables))
			for _, tf := range tables {
				gotKeys = append(gotKeys, tf.Namespace+"."+tf.Table)
				if want, ok := tc.wantPaths[tf.Namespace+"."+tf.Table]; ok && !equalStrings(tf.Paths, want) {
					t.Errorf("%s.%s paths = %v, want %v", tf.Namespace, tf.Table, tf.Paths, want)
				}
			}
			if !equalStrings(gotKeys, tc.wantKeys) {
				t.Errorf("table key order = %v, want %v", gotKeys, tc.wantKeys)
			}
		})
	}
}

// TestFilesManifest_WriteJSONForTable_RoundTrip_LocalFS exercises the
// per-table write path: one (namespace, table) → one manifest blob
// inside the table prefix. Mirrors what the gateway does at
// end-of-stream.
func TestFilesManifest_WriteJSONForTable_RoundTrip_LocalFS(t *testing.T) {
	dir := t.TempDir()
	b := backend.NewLocalBackend(backend.LocalConfig{}) // DataDir empty: absolute paths from URLs

	m := NewFilesManifest("75cc54f6-dfe7-4246-b4a2-4254dd5b2c36")
	// events.users: two parquets under its own table base.
	usersBase := "file://" + filepath.Join(dir, "warehouse/events/users")
	m.Append("events", "users", usersBase+"/data/p1.parquet")
	m.Append("events", "users", usersBase+"/data/p2.parquet")
	// logs.audit: one parquet under a different table base.
	auditBase := "file://" + filepath.Join(dir, "warehouse/logs/audit")
	m.Append("logs", "audit", auditBase+"/data/p1.parquet")

	// Write each per-table manifest at its own table prefix.
	cases := []struct {
		ns, tbl, base string
	}{
		{"events", "users", usersBase},
		{"logs", "audit", auditBase},
	}
	for _, tc := range cases {
		manifestURL, ok := ResolveTableManifestPath(tc.base+"/data/", m.RunID())
		if !ok {
			t.Fatalf("ResolveTableManifestPath(%s) returned !ok", tc.base)
		}
		wrote, err := m.WriteJSONForTable(context.Background(), b, tc.ns, tc.tbl, manifestURL)
		if err != nil {
			t.Fatalf("WriteJSONForTable %s.%s: %v", tc.ns, tc.tbl, err)
		}
		if !wrote {
			t.Fatalf("WriteJSONForTable %s.%s: wrote=false (expected true)", tc.ns, tc.tbl)
		}

		// Read back via the same conversion the backend uses (toLocalPath
		// strips the file:// prefix).
		onDisk := strings.TrimPrefix(manifestURL, "file://")
		raw, err := os.ReadFile(onDisk)
		if err != nil {
			t.Fatalf("read back %s.%s: %v (path=%s)", tc.ns, tc.tbl, err, onDisk)
		}
		var doc tableManifestJSON
		if err := json.Unmarshal(raw, &doc); err != nil {
			t.Fatalf("decode %s.%s: %v\n%s", tc.ns, tc.tbl, err, raw)
		}
		if doc.RunID != m.RunID() {
			t.Errorf("%s.%s: run_id = %q, want %q", tc.ns, tc.tbl, doc.RunID, m.RunID())
		}
		if doc.Namespace != tc.ns || doc.Table != tc.tbl {
			t.Errorf("%s.%s: doc identifiers = %s.%s", tc.ns, tc.tbl, doc.Namespace, doc.Table)
		}
		if len(doc.Paths) == 0 {
			t.Errorf("%s.%s: paths empty", tc.ns, tc.tbl)
		}
		// The manifest path itself must sit inside the table prefix —
		// load-bearing because the per-table STS scope only covers
		// that prefix.
		if !strings.HasPrefix(manifestURL, tc.base+"/") {
			t.Errorf("manifest URL %q is outside table prefix %q", manifestURL, tc.base)
		}
	}
}

// TestFilesManifest_WriteJSONForTable_EdgeCases: a (ns, tbl) with zero Append
// calls returns wrote=false, no error (TableCommit's "missing manifest entry →
// nothing to commit" path handles that downstream); a nil backend or empty
// path is an error.
func TestFilesManifest_WriteJSONForTable_EdgeCases(t *testing.T) {
	tests := []struct {
		name      string
		nilBack   bool
		ns, tbl   string
		path      string
		wantError bool
	}{
		{name: "missing entry is not an error", ns: "other", tbl: "missing", path: "file:///tmp/manifest.json"},
		{name: "nil backend errors", nilBack: true, ns: "ns", tbl: "tbl", path: "s3://b/ns/tbl/.run-state/r/files.json", wantError: true},
		{name: "empty path errors", ns: "ns", tbl: "tbl", path: "", wantError: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := NewFilesManifest("r")
			m.Append("ns", "tbl", "s3://b/ns/tbl/data/p.parquet")

			var b backend.StorageBackend
			if !tc.nilBack {
				b = backend.NewLocalBackend(backend.LocalConfig{})
			}
			wrote, err := m.WriteJSONForTable(context.Background(), b, tc.ns, tc.tbl, tc.path)
			if tc.wantError {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("err=%v want nil", err)
			}
			if wrote {
				t.Error("wrote=true want false")
			}
		})
	}
}

func TestResolveTableManifestPath(t *testing.T) {
	cases := []struct {
		name     string
		basePath string
		runID    string
		want     string
		wantOK   bool
	}{
		{
			// Lakekeeper-managed: trailing /data/ is stripped to recover
			// the table base.
			name:     "lakekeeper s3 with /data/ suffix",
			basePath: "s3://datuplet/019dceed-aaaa/019dceed-bbbb/data/",
			runID:    "run-1",
			want:     "s3://datuplet/019dceed-aaaa/019dceed-bbbb/.run-state/run-1/files.json",
			wantOK:   true,
		},
		{
			// Same lakekeeper layout without trailing slash.
			name:     "lakekeeper s3 with /data suffix (no trailing slash)",
			basePath: "s3://datuplet/019dceed-aaaa/019dceed-bbbb/data",
			runID:    "run-2",
			want:     "s3://datuplet/019dceed-aaaa/019dceed-bbbb/.run-state/run-2/files.json",
			wantOK:   true,
		},
		{
			// Caller passed the table base directly (no /data suffix
			// to strip). The path lands under <base>/.run-state/.
			name:     "tableBase passed directly",
			basePath: "s3://datuplet/019dceed-aaaa/019dceed-bbbb",
			runID:    "run-3",
			want:     "s3://datuplet/019dceed-aaaa/019dceed-bbbb/.run-state/run-3/files.json",
			wantOK:   true,
		},
		{
			name:     "filesystem layout",
			basePath: "file:///var/lib/lakekeeper/wh/<storage-uuid>/<table-uuid>/data/",
			runID:    "r",
			want:     "file:///var/lib/lakekeeper/wh/<storage-uuid>/<table-uuid>/.run-state/r/files.json",
			wantOK:   true,
		},
		{
			name:     "empty basePath",
			basePath: "",
			runID:    "r",
			want:     "",
			wantOK:   false,
		},
		{
			name:     "empty runID",
			basePath: "s3://b/<wh>/<tbl>/data/",
			runID:    "",
			want:     "",
			wantOK:   false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := ResolveTableManifestPath(tc.basePath, tc.runID)
			if ok != tc.wantOK {
				t.Errorf("ok = %v, want %v", ok, tc.wantOK)
			}
			if got != tc.want {
				t.Errorf("path = %q, want %q", got, tc.want)
			}
		})
	}
}

// equalStrings is a small assertion helper. We avoid reflect.DeepEqual
// here so a failing test prints the slices in the expected order rather
// than the reflect formatter.
func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
