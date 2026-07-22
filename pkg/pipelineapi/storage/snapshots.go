package storage

import (
	"net/http"
	"sort"
	"strconv"
	"time"

	"github.com/google/uuid"
)

// snapshotHistoryEntry is one row in the GET …/snapshots response.
// Snapshots without datuplet.* summary keys still get a row with
// empty audit fields; history is always contiguous.
//
// Actor is the user UUID from the JWT actor claim — stable across
// email changes, the canonical audit-trail identifier. ActorEmail is
// resolved at response time via EmailLookup against the users table;
// it reflects the user's CURRENT email and may differ from what they
// had when the commit landed. UI prefers ActorEmail for display when
// non-empty and falls back to a truncated Actor UUID otherwise.
// AddedFilesSize is the on-storage byte size added by this snapshot,
// read straight from the Iceberg summary's `added-files-size` total
// (nil = absent, e.g. a foreign writer that didn't populate it → the
// UI renders "—").
type snapshotHistoryEntry struct {
	SnapshotID     int64     `json:"snapshot_id"`
	CommittedAt    time.Time `json:"committed_at"`
	Actor          string    `json:"actor"`
	ActorEmail     string    `json:"actor_email,omitempty"`
	RunID          string    `json:"run_id"`
	PipelineAPI    string    `json:"pipeline_api"`
	AddedRecords   *int64    `json:"added_records,omitempty"`
	AddedFilesSize *int64    `json:"added_files_size,omitempty"`
}

// Snapshots handles GET /api/v1/storage/projects/{pid}/tables/{ns}/{t}/snapshots.
// Returns the table's snapshot list sorted by committed_at descending.
// Snapshots without datuplet.* summary keys are included with empty
// audit fields rather than dropped — history must be contiguous.
func (h *HTTPHandlers) Snapshots(w http.ResponseWriter, r *http.Request) {
	tbl, _, ok := h.loadRequestedTable(w, r)
	if !ok {
		return
	}

	snaps := tbl.Metadata().Snapshots()
	out := make([]snapshotHistoryEntry, 0, len(snaps))
	for _, s := range snaps {
		e := snapshotHistoryEntry{
			SnapshotID:  s.SnapshotID,
			CommittedAt: time.UnixMilli(s.TimestampMs).UTC(),
		}
		if s.Summary != nil && s.Summary.Properties != nil {
			e.Actor = s.Summary.Properties["datuplet.actor"]
			e.RunID = s.Summary.Properties["datuplet.run-id"]
			e.PipelineAPI = s.Summary.Properties["datuplet.pipeline-api"]
			if v, ok := s.Summary.Properties["added-records"]; ok && v != "" {
				if n, err := strconv.ParseInt(v, 10, 64); err == nil {
					e.AddedRecords = &n
				}
			}
			if v, ok := s.Summary.Properties["added-files-size"]; ok && v != "" {
				if n, err := strconv.ParseInt(v, 10, 64); err == nil {
					e.AddedFilesSize = &n
				}
			}
		}
		out = append(out, e)
	}

	// Sort descending by committed_at (most-recent first).
	sort.Slice(out, func(i, j int) bool {
		return out[i].CommittedAt.After(out[j].CommittedAt)
	})

	// Resolve actor UUIDs → emails for display. We dedupe first so a long
	// table history with a few distinct actors only costs a handful of DB
	// hits, not one per snapshot. EmailByID returns "" on miss / DB error
	// so a stale UUID (deleted user) just keeps the canonical actor UUID
	// in the response — the UI gracefully falls back to the truncated UUID.
	if h.Emails != nil {
		seen := make(map[string]string)
		for _, e := range out {
			if e.Actor == "" {
				continue
			}
			if _, ok := seen[e.Actor]; ok {
				continue
			}
			id, err := uuid.Parse(e.Actor)
			if err != nil {
				seen[e.Actor] = ""
				continue
			}
			seen[e.Actor] = h.Emails.EmailByID(r.Context(), id)
		}
		for i := range out {
			if email, ok := seen[out[i].Actor]; ok && email != "" {
				out[i].ActorEmail = email
			}
		}
	}

	writeJSONResp(w, http.StatusOK, out)
}
