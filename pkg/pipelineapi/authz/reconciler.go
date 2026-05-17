package authz

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

// WriteAdminTuple writes the canonical "user is project_admin on lakekeeper
// Project" tuple in an idempotent shape: a duplicate-tuple error from
// OpenFGA is swallowed, every other error propagates.
//
// `userSubject` is the bare OIDC sub (e.g. UUID for cluster mode,
// localmode.UserID for local mode); the `oidc~` prefix is applied by
// UserObject. `lakekeeperProjectID` is the UUID lakekeeper allocated at
// POST /management/v1/project time and that's stored in
// projects.lakekeeper_project_id.
//
// `project_admin` is the lock-out-protected lakekeeper builtin. Using
// bare "admin" or "data_admin" would NOT give the user lakekeeper-side
// write access to the project; `project_admin` is the strongest grant
// for the project creator.
func WriteAdminTuple(ctx context.Context, a Authorizer, userSubject, lakekeeperProjectID string) error {
	if userSubject == "" {
		return errors.New("WriteAdminTuple: userSubject is required")
	}
	if lakekeeperProjectID == "" {
		return errors.New("WriteAdminTuple: lakekeeperProjectID is required")
	}
	tuple := Tuple{
		User:     UserObject(userSubject).String(),
		Relation: "project_admin",
		Object:   ProjectObject(lakekeeperProjectID),
	}
	if err := a.WriteTuples(ctx, []Tuple{tuple}); err != nil {
		if isAlreadyExistsErr(err) {
			return nil
		}
		return fmt.Errorf("write project_admin tuple: %w", err)
	}
	return nil
}

// DeleteProjectTuples removes every relation tuple whose object is the
// given lakekeeper Project, for the given user(s).
//
// We don't have a reliable cross-user enumeration via the Authorizer
// interface (ListObjects is user→object, not object→user), so callers
// pass the known users explicitly. The admin delete-project flow today
// only knows the creator — that's the user we wrote the project_admin
// tuple for; deleting that one tuple is sufficient. Future Slices that
// add editor/viewer grants must extend this list.
//
// Missing tuples are tolerated (re-running delete on a half-completed
// teardown succeeds).
func DeleteProjectTuples(ctx context.Context, a Authorizer, lakekeeperProjectID string, userSubjects []string) error {
	if lakekeeperProjectID == "" {
		return errors.New("DeleteProjectTuples: lakekeeperProjectID is required")
	}
	if len(userSubjects) == 0 {
		return nil
	}
	obj := ProjectObject(lakekeeperProjectID)
	// Issue per-user deletes so a missing tuple on one user doesn't
	// abort the whole batch (OpenFGA's Write transaction fails atomically
	// on any missing-tuple error).
	for _, sub := range userSubjects {
		// Cover all three relations that admin grant can write: project_admin
		// (project creator), editor, and viewer. Missing tuples are tolerated
		// — the per-relation loop tolerates each miss individually.
		for _, rel := range []string{"project_admin", "editor", "viewer"} {
			t := Tuple{User: UserObject(sub).String(), Relation: rel, Object: obj}
			if err := a.DeleteTuples(ctx, []Tuple{t}); err != nil {
				if isMissingTupleErr(err) {
					continue
				}
				return fmt.Errorf("delete %s tuple for user %q: %w", rel, sub, err)
			}
		}
	}
	return nil
}

// EnsureUserHasProjectAdmin verifies via FGA Check that userSubject has
// `project_admin` on lakekeeperProjectID. Returns (true, nil) if the
// tuple is present, (false, nil) if it's missing, or (_, err) on
// backend errors.
//
// Used by EnsureProjectAuthz to detect the "lakekeeper Project exists
// + projects.lakekeeper_project_id set + FGA tuple lost" recovery case.
func EnsureUserHasProjectAdmin(ctx context.Context, a Authorizer, userSubject, lakekeeperProjectID string) (bool, error) {
	if userSubject == "" || lakekeeperProjectID == "" {
		return false, errors.New("EnsureUserHasProjectAdmin: both userSubject and lakekeeperProjectID required")
	}
	allowed, err := a.Check(ctx, UserObject(userSubject).String(), "project_admin", ProjectObject(lakekeeperProjectID))
	if err != nil {
		return false, fmt.Errorf("check project_admin: %w", err)
	}
	return allowed, nil
}

// isAlreadyExistsErr inspects an OpenFGA write error for the canonical
// "tuple already exists" wording. Mirrors cmd/pipeline-api/admin_authz.go's
// isTupleAlreadyExistsError so we behave consistently across writers.
//
// We accept a wider net than strictly necessary because OpenFGA's error
// envelope wording has shifted across versions (1.5 → 1.15) and we want
// re-running EnsureProjectAuthz to be safe forever.
func isAlreadyExistsErr(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	return strings.Contains(s, "cannot write a tuple") ||
		strings.Contains(s, "already exists")
}

// isMissingTupleErr returns true ONLY for the OpenFGA error shape that
// indicates "this specific tuple doesn't exist." It deliberately does NOT
// catch generic "not found" errors — those are returned for missing stores
// or models, and swallowing them would mask misconfiguration.
func isMissingTupleErr(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	return strings.Contains(s, "cannot delete a tuple which does not exist")
}
