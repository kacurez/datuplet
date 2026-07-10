package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/google/uuid"

	"github.com/datuplet/datuplet/pkg/pipelineapi/auth"
	"github.com/datuplet/datuplet/pkg/pipelineapi/projectgate"
	"github.com/datuplet/datuplet/pkg/pipelineapi/storage/testdata"
)

// newSnapshotsTestServerFull builds an httptest.Server that wires all five
// storage routes (the four existing ones plus Snapshots) with stubUser
// always injected. Mirrors newTestServer from handlers_test.go but adds
// the fifth route.
func newSnapshotsTestServerFull(t *testing.T, svc *Service) *httptest.Server {
	t.Helper()
	authzr := allowedFake()
	// Gate is built from the same stubs (svc.LakekeeperProjectIDFor + authzr)
	// that resolveProject used to call directly. WarehouseFor is left nil —
	// these fixtures use the fixture-walker path (LakekeeperURL == ""), so
	// resolveWarehouse is never invoked.
	h := &HTTPHandlers{
		Svc: svc,
		Gate: &projectgate.Gate{
			LakekeeperProjectIDFor: svc.LakekeeperProjectIDFor,
			Authorizer:             authzr,
		},
	}
	injectUser := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := auth.WithCtxUser(r.Context(), stubUser)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
	mux := http.NewServeMux()
	mux.Handle("GET /api/v1/storage/projects/{pid}/tables",
		injectUser(http.HandlerFunc(h.ListTables)))
	mux.Handle("GET /api/v1/storage/projects/{pid}/tables/{ns}/{t}/info",
		injectUser(http.HandlerFunc(h.TableInfo)))
	mux.Handle("GET /api/v1/storage/projects/{pid}/tables/{ns}/{t}/schema",
		injectUser(http.HandlerFunc(h.TableSchema)))
	mux.Handle("GET /api/v1/storage/projects/{pid}/tables/{ns}/{t}/preview",
		injectUser(http.HandlerFunc(h.Preview)))
	mux.Handle("GET /api/v1/storage/projects/{pid}/tables/{ns}/{t}/snapshots",
		injectUser(http.HandlerFunc(h.Snapshots)))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

// makeFixtureServiceWithAuditKeys generates a warehouse that contains the
// standard fixture tables PLUS a new "audited" table in the public namespace.
// The audited table has two snapshots:
//   - snapshot 1 (older): no datuplet.* keys (legacy snapshot)
//   - snapshot 2 (newer): full datuplet.* audit keys
func makeFixtureServiceWithAuditKeys(t *testing.T) *Service {
	t.Helper()
	warehouse := filepath.Join(t.TempDir(), "warehouse")
	testdata.GenerateAll(t, warehouse)
	if err := generateAuditedTable(warehouse); err != nil {
		t.Fatalf("generateAuditedTable: %v", err)
	}
	return &Service{
		WarehouseURI: "file://" + warehouse,
		OrgName:      "myorg",
		AllowLocal:   true,
		LakekeeperProjectIDFor: func(_ context.Context, _ uuid.UUID) (string, error) {
			return fixtureLakekeeperProjectID, nil
		},
	}
}

// generateAuditedTable creates a minimal two-snapshot Iceberg table at
// <warehouse>/orgs/myorg/projects/<pid>/tables/public/audited. The first
// snapshot has no datuplet.* keys; the second has all four plus added-records.
func generateAuditedTable(warehouse string) error {
	tableDir := filepath.Join(warehouse, testdata.ProjectRoot(), "public", "audited")
	metadataDir := filepath.Join(tableDir, "metadata")
	if err := os.MkdirAll(metadataDir, 0o755); err != nil {
		return err
	}

	iceSchema := iceberg.NewSchema(
		0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	)
	tableLocation := "file://" + tableDir
	metadataBase := "file://" + metadataDir + "/"
	partSpec := iceberg.UnpartitionedSpec

	// Snapshot timestamps must be >= last-updated-ms set by iceberg-go's
	// NewMetadataWithUUID (which uses time.Now()).  Use time.Now() + a
	// small offset so both snapshots are reliably in the future relative
	// to the base metadata timestamp.
	now := time.Now().UTC()
	snap1ID := now.Add(100 * time.Millisecond).UnixNano() / int64(time.Millisecond)
	snap2ID := now.Add(2 * time.Minute).UnixNano() / int64(time.Millisecond)

	ml1Name := fmt.Sprintf("snap-%d-a.avro", snap1ID)
	ml2Name := fmt.Sprintf("snap-%d-b.avro", snap2ID)
	ml1URI := metadataBase + ml1Name
	ml2URI := metadataBase + ml2Name

	seq1 := int64(1)
	seq2 := int64(2)

	// Write empty manifest lists (no data files needed for snapshot
	// history tests).
	for _, args := range []struct {
		snapID   int64
		seq      *int64
		uri      string
		filename string
	}{
		{snap1ID, &seq1, ml1URI, ml1Name},
		{snap2ID, &seq2, ml2URI, ml2Name},
	} {
		var buf bytes.Buffer
		if err := iceberg.WriteManifestList(2, &buf, args.snapID, nil, args.seq, 0, nil); err != nil {
			return fmt.Errorf("write manifest list %s: %w", args.filename, err)
		}
		if err := os.WriteFile(filepath.Join(metadataDir, args.filename), buf.Bytes(), 0o644); err != nil {
			return err
		}
	}

	schID := iceSchema.ID
	snap1 := table.Snapshot{
		SnapshotID:     snap1ID,
		SequenceNumber: seq1,
		TimestampMs:    now.Add(100 * time.Millisecond).UnixMilli(),
		ManifestList:   ml1URI,
		Summary: &table.Summary{
			Operation:  table.OpAppend,
			Properties: map[string]string{"added-records": "0"},
		},
		SchemaID: &schID,
	}
	snap2 := table.Snapshot{
		SnapshotID:       snap2ID,
		ParentSnapshotID: &snap1ID,
		SequenceNumber:   seq2,
		TimestampMs:      now.Add(2 * time.Minute).UnixMilli(),
		ManifestList:     ml2URI,
		Summary: &table.Summary{
			Operation: table.OpAppend,
			Properties: map[string]string{
				"added-records":         "3",
				"datuplet.actor":        "user-uuid-abc",
				"datuplet.run-id":       "run-001",
				"datuplet.run-mode":     "cluster",
				"datuplet.pipeline-api": "datuplet-api",
			},
		},
		SchemaID: &schID,
	}

	v1URI := metadataBase + "v1.metadata.json"
	base, err := table.NewMetadataWithUUID(iceSchema, partSpec, table.UnsortedSortOrder, tableLocation, iceberg.Properties{}, uuid.New())
	if err != nil {
		return fmt.Errorf("new metadata: %w", err)
	}
	meta1, err := table.UpdateTableMetadata(base, []table.Update{
		table.NewAddSnapshotUpdate(&snap1),
		table.NewSetSnapshotRefUpdate("main", snap1ID, table.BranchRef, 0, 0, 0),
	}, v1URI)
	if err != nil {
		return fmt.Errorf("update v1: %w", err)
	}
	if err := writeAuditedMetadataJSON(filepath.Join(metadataDir, "v1.metadata.json"), meta1); err != nil {
		return err
	}

	v2URI := metadataBase + "v2.metadata.json"
	meta2, err := table.UpdateTableMetadata(meta1, []table.Update{
		table.NewAddSnapshotUpdate(&snap2),
		table.NewSetSnapshotRefUpdate("main", snap2ID, table.BranchRef, 0, 0, 0),
	}, v2URI)
	if err != nil {
		return fmt.Errorf("update v2: %w", err)
	}
	if err := writeAuditedMetadataJSON(filepath.Join(metadataDir, "v2.metadata.json"), meta2); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(metadataDir, "version-hint.text"), []byte("2"), 0o644)
}

func writeAuditedMetadataJSON(p string, meta table.Metadata) error {
	data, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	return os.WriteFile(p, data, 0o644)
}

// ----- Tests -----

// TestSnapshots_NotFound verifies that requesting snapshots for an unknown
// table returns 404.
func TestSnapshots_NotFound(t *testing.T) {
	svc := makeFixtureServiceWithLK(t)
	srv := newSnapshotsTestServerFull(t, svc)

	url := srv.URL + "/api/v1/storage/projects/" + fixtureProjectID + "/tables/public/nonexistent/snapshots"
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
}

// TestSnapshots_LegacySnapshot verifies that a table with one snapshot and no
// datuplet.* keys still returns a row (legacy snapshot, contiguous
// history requirement).
func TestSnapshots_LegacySnapshot(t *testing.T) {
	svc := makeFixtureServiceWithLK(t)
	srv := newSnapshotsTestServerFull(t, svc)

	url := srv.URL + "/api/v1/storage/projects/" + fixtureProjectID + "/tables/public/simple/snapshots"
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	var rows []snapshotHistoryEntry
	if err := json.NewDecoder(resp.Body).Decode(&rows); err != nil {
		t.Fatalf("decode: %v", err)
	}
	// Fixture writes one snapshot (v1 onwards has no new snapshots —
	// only property updates), so expect exactly one row.
	if len(rows) != 1 {
		t.Fatalf("got %d rows, want 1", len(rows))
	}
	if rows[0].SnapshotID == 0 {
		t.Error("SnapshotID should be non-zero")
	}
	// Legacy snapshot: audit keys should be empty strings.
	if rows[0].Actor != "" {
		t.Errorf("Actor = %q, want empty (no datuplet.actor key)", rows[0].Actor)
	}
	if rows[0].RunMode != "" {
		t.Errorf("RunMode = %q, want empty (no datuplet.run-mode key)", rows[0].RunMode)
	}
	// added-records is present in the simple fixture summary ("3").
	if rows[0].AddedRecords == nil {
		t.Error("AddedRecords should be non-nil for the simple fixture snapshot (has added-records key)")
	} else if *rows[0].AddedRecords != 3 {
		t.Errorf("AddedRecords = %d, want 3", *rows[0].AddedRecords)
	}
}

// TestSnapshots_WithAuditKeys verifies that a table whose snapshot summary
// carries datuplet.* keys returns them correctly, and that multiple
// snapshots are sorted newest-first.
func TestSnapshots_WithAuditKeys(t *testing.T) {
	svc := makeFixtureServiceWithAuditKeys(t)
	srv := newSnapshotsTestServerFull(t, svc)

	url := srv.URL + "/api/v1/storage/projects/" + fixtureProjectID + "/tables/public/audited/snapshots"
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	var rows []snapshotHistoryEntry
	if err := json.NewDecoder(resp.Body).Decode(&rows); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("got %d rows, want 2", len(rows))
	}
	// Sorted newest-first: rows[0] is the second (newer) snapshot.
	if !rows[0].CommittedAt.After(rows[1].CommittedAt) {
		t.Errorf("rows not sorted newest-first: [0]=%v [1]=%v", rows[0].CommittedAt, rows[1].CommittedAt)
	}
	// Newer snapshot has audit keys.
	newer := rows[0]
	if newer.Actor != "user-uuid-abc" {
		t.Errorf("Actor = %q, want %q", newer.Actor, "user-uuid-abc")
	}
	if newer.RunMode != "cluster" {
		t.Errorf("RunMode = %q, want %q", newer.RunMode, "cluster")
	}
	if newer.RunID != "run-001" {
		t.Errorf("RunID = %q, want %q", newer.RunID, "run-001")
	}
	if newer.PipelineAPI != "datuplet-api" {
		t.Errorf("PipelineAPI = %q, want %q", newer.PipelineAPI, "datuplet-api")
	}
	if newer.AddedRecords == nil {
		t.Error("AddedRecords should be non-nil for audited newer snapshot")
	} else if *newer.AddedRecords != 3 {
		t.Errorf("AddedRecords = %d, want 3", *newer.AddedRecords)
	}
	// Older snapshot has added-records: "0" (real zero commit) — pointer must be
	// non-nil pointing to 0, distinguishable from a missing key (nil).
	older := rows[1]
	if older.Actor != "" {
		t.Errorf("older Actor = %q, want empty", older.Actor)
	}
	if older.AddedRecords == nil {
		t.Error("older AddedRecords should be non-nil (added-records: '0' is present — real zero commit)")
	} else if *older.AddedRecords != 0 {
		t.Errorf("older AddedRecords = %d, want 0", *older.AddedRecords)
	}
}

// TestAddedRecordsZeroIsDistinctFromMissing pins the distinguishability
// invariant introduced by the P3 fix: a snapshot whose summary carries
// added-records: "0" must produce a non-nil *int64 pointer (real zero commit),
// while a snapshot whose summary lacks the key entirely must produce nil.
func TestAddedRecordsZeroIsDistinctFromMissing(t *testing.T) {
	svc := makeFixtureServiceWithAuditKeys(t)
	srv := newSnapshotsTestServerFull(t, svc)

	url := srv.URL + "/api/v1/storage/projects/" + fixtureProjectID + "/tables/public/audited/snapshots"
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	// Use raw JSON decode so we can distinguish null from absent/0.
	var rawRows []map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&rawRows); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(rawRows) != 2 {
		t.Fatalf("got %d rows, want 2", len(rawRows))
	}

	// rows are sorted newest-first.
	// newer (index 0): added-records: "3" → JSON number 3, not null/absent.
	newer := rawRows[0]
	newerVal, newerPresent := newer["added_records"]
	if !newerPresent {
		t.Error("newer snapshot: added_records key missing from JSON (want numeric 3)")
	} else if newerVal == nil {
		t.Error("newer snapshot: added_records is null (want numeric 3)")
	}

	// older (index 1): added-records: "0" → JSON number 0, NOT null/absent.
	// This distinguishes a real zero-row commit from a missing summary property.
	older := rawRows[1]
	olderVal, olderPresent := older["added_records"]
	if !olderPresent {
		t.Error("older snapshot: added_records key missing (want numeric 0 — real zero commit)")
	} else if olderVal == nil {
		t.Error("older snapshot: added_records is null (want numeric 0 — real zero, distinguishable from missing)")
	} else if n, ok := olderVal.(float64); !ok || n != 0 {
		t.Errorf("older snapshot: added_records = %v, want 0", olderVal)
	}
}
