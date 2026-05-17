package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/datuplet/datuplet/pkg/pipelineapi/authz"
)

// RunTupleRecord is the read-back shape of a run_tuples row. Used by the
// run-completion path and the reaper to reconstruct the FGA tuples that
// need DELETing.
type RunTupleRecord struct {
	RunID     uuid.UUID
	Tuples    []authz.Tuple
	Committed bool
}

// RecordRunTuples performs Step 1 of the four-step crash-recovery
// ordering — INSERT a run_tuples row recording intent, with
// committed=false. The caller proceeds to (2) WriteTuples in FGA, then
// (3) inserts the runs row, then (4) MarkRunTuplesCommitted.
//
// The tuples slice is JSON-encoded into the jsonb column. Callers MUST
// invoke this BEFORE writing tuples to FGA so a crash between this
// INSERT and the FGA write leaves a recovery breadcrumb (committed=false
// row + no FGA tuples → reaper deletes the row).
//
// Idempotent on (run_id) — duplicate INSERTs return an error since
// run_id is the primary key. The trigger flow generates a fresh UUID
// per call so this is unreachable in normal operation; the constraint
// is a safety net.
func RecordRunTuples(ctx context.Context, pool *pgxpool.Pool, runID uuid.UUID, tuples []authz.Tuple) error {
	if pool == nil {
		return fmt.Errorf("RecordRunTuples: pool is required")
	}
	encoded, err := encodeAuthzTuples(tuples)
	if err != nil {
		return fmt.Errorf("RecordRunTuples: encode: %w", err)
	}
	if _, err := pool.Exec(ctx,
		`INSERT INTO run_tuples (run_id, tuples, committed) VALUES ($1, $2, false)`,
		runID, encoded,
	); err != nil {
		return fmt.Errorf("RecordRunTuples: insert: %w", err)
	}
	return nil
}

// MarkRunTuplesCommitted performs Step 4 — flip the recovery row to
// committed=true once the runs row + K8s resources have been created.
// A run_tuples row that stays at committed=false through reaper-deadline
// is a sign of a crashed trigger flow; the reaper's job is to clean
// both halves.
//
// Idempotent. Returns nil even if the row is missing — a deleted row
// after a successful reap is fine, the trigger flow already finished
// its other side-effects.
func MarkRunTuplesCommitted(ctx context.Context, pool *pgxpool.Pool, runID uuid.UUID) error {
	if pool == nil {
		return fmt.Errorf("MarkRunTuplesCommitted: pool is required")
	}
	if _, err := pool.Exec(ctx,
		`UPDATE run_tuples SET committed = true WHERE run_id = $1`,
		runID,
	); err != nil {
		return fmt.Errorf("MarkRunTuplesCommitted: update: %w", err)
	}
	return nil
}

// GetRunTuples reads back the recovery row for runID. Used by the
// run-completion path to know which FGA tuples to DELETE before
// dropping the row.
//
// Returns (nil, nil) when no row exists — a no-tuples run (the trigger
// derived an empty intent) doesn't even insert a row.
func GetRunTuples(ctx context.Context, pool *pgxpool.Pool, runID uuid.UUID) (*RunTupleRecord, error) {
	if pool == nil {
		return nil, fmt.Errorf("GetRunTuples: pool is required")
	}
	var raw []byte
	var committed bool
	err := pool.QueryRow(ctx,
		`SELECT tuples, committed FROM run_tuples WHERE run_id = $1`, runID,
	).Scan(&raw, &committed)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("GetRunTuples: select: %w", err)
	}
	tuples, decErr := decodeAuthzTuples(raw)
	if decErr != nil {
		return nil, fmt.Errorf("GetRunTuples: decode: %w", decErr)
	}
	return &RunTupleRecord{RunID: runID, Tuples: tuples, Committed: committed}, nil
}

// DeleteRunTuples removes the run_tuples row. Called after the FGA
// tuples have been DELETEd at run-completion.
//
// Best-effort: a non-existent row returns nil. The reaper sweeps any
// orphan rows on its own schedule.
func DeleteRunTuples(ctx context.Context, pool *pgxpool.Pool, runID uuid.UUID) error {
	if pool == nil {
		return fmt.Errorf("DeleteRunTuples: pool is required")
	}
	if _, err := pool.Exec(ctx, `DELETE FROM run_tuples WHERE run_id = $1`, runID); err != nil {
		return fmt.Errorf("DeleteRunTuples: delete: %w", err)
	}
	return nil
}

// tupleJSON is the on-wire shape of a single FGA tuple in the run_tuples
// jsonb column. Mirrors authz.Tuple but uses JSON-friendly tags.
type tupleJSON struct {
	User     string `json:"user"`
	Relation string `json:"relation"`
	Object   string `json:"object"` // canonical "<type>:<id>"
}

func encodeAuthzTuples(tuples []authz.Tuple) ([]byte, error) {
	out := make([]tupleJSON, 0, len(tuples))
	for _, t := range tuples {
		out = append(out, tupleJSON{
			User:     t.User,
			Relation: t.Relation,
			Object:   t.Object.String(),
		})
	}
	return json.Marshal(out)
}

func decodeAuthzTuples(raw []byte) ([]authz.Tuple, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	var arr []tupleJSON
	if err := json.Unmarshal(raw, &arr); err != nil {
		return nil, err
	}
	out := make([]authz.Tuple, 0, len(arr))
	for _, t := range arr {
		obj, err := authz.ParseObject(t.Object)
		if err != nil {
			return nil, fmt.Errorf("parse object %q: %w", t.Object, err)
		}
		out = append(out, authz.Tuple{
			User:     t.User,
			Relation: t.Relation,
			Object:   obj,
		})
	}
	return out, nil
}
