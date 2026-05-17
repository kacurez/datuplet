// Package framework — test users for FGA-grant-aware e2e scenarios.
//
// The e2e harness uses four hard-coded user identities so scenarios
// can be written against a stable matrix instead of fresh-per-run UUIDs.
// FGA tuples are seeded against these identities at fixture init so
// tokens minted later for them resolve against real grants.
//
// The grant matrix (alice/project_admin, bob/editor, charlie/viewer,
// dora/no-grants) is exercised by the FGA-matrix scenarios.
package framework

import "github.com/google/uuid"

// TestUserID names a hard-coded test identity. Stable UUIDs let the
// harness seed FGA tuples once at fixture init and re-use them across
// every scenario, with predictable per-user authorization outcomes.
type TestUserID = uuid.UUID

// Hard-coded test-user UUIDs.
//
// These are intentionally trivial-pattern UUIDs so they're easy to spot
// in OpenFGA tuple dumps, lakekeeper request logs, and DB grants. They
// are NOT secrets — every e2e run writes them to FGA on bootstrap.
var (
	AliceID   = uuid.MustParse("11111111-1111-1111-1111-111111111111")
	BobID     = uuid.MustParse("22222222-2222-2222-2222-222222222222")
	CharlieID = uuid.MustParse("33333333-3333-3333-3333-333333333333")
	DoraID    = uuid.MustParse("44444444-4444-4444-4444-444444444444")
)

// TestUserGrant pairs a test user with the project-level relation it
// gets seeded with. SeedFGAGrants writes one tuple per non-empty
// Relation; users with Relation=="" get nothing (the dora case — used
// to assert 403s in negative-path scenarios).
//
// Note: Relation is the FGA relation NAME on the project type
// ("project_admin", "editor", "viewer"). The values match the model
// extensions in fga_model.fga §"project". The viewer/editor unions
// resolve through lakekeeper's data_admin → modify chain so
// scenario-level "can read" / "can write" reasoning stays correct.
type TestUserGrant struct {
	ID       TestUserID
	Email    string // informational; appears in audit logs only
	Relation string // "" = no grants
}

// TestUsers is the canonical grant matrix the bootstrap layer seeds.
// Order matters: FGA tuple writes are unordered, but iterating in this
// order keeps log output deterministic for debugging.
var TestUsers = []TestUserGrant{
	{ID: AliceID, Email: "e2e-alice@datuplet.test", Relation: "project_admin"},
	{ID: BobID, Email: "e2e-bob@datuplet.test", Relation: "editor"},
	{ID: CharlieID, Email: "e2e-charlie@datuplet.test", Relation: "viewer"},
	{ID: DoraID, Email: "e2e-dora@datuplet.test", Relation: ""},
}
