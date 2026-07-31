package apps_test

import (
	"testing"

	"github.com/datuplet/datuplet/pkg/pipelineapi/apps"
)

// TestAppJWTSubject pins the exact bare form (RFC 028 P0 report §C): the
// JWT `sub` claim carries no "oidc~"/"user:" prefix.
func TestAppJWTSubject(t *testing.T) {
	got := apps.AppJWTSubject("3f9c1b7a-1111-4a2b-9c3d-abcdefabcdef")
	want := "app-3f9c1b7a-1111-4a2b-9c3d-abcdefabcdef"
	if got != want {
		t.Fatalf("AppJWTSubject() = %q, want %q", got, want)
	}
}

// TestAppFGASubject pins the exact FGA user string the `viewer` tuple
// targets: "user:" + "oidc~" + the JWT subject — composed via
// authz.UserObject, one prefix only (no double-prefixing).
func TestAppFGASubject(t *testing.T) {
	got := apps.AppFGASubject("3f9c1b7a-1111-4a2b-9c3d-abcdefabcdef")
	want := "user:oidc~app-3f9c1b7a-1111-4a2b-9c3d-abcdefabcdef"
	if got != want {
		t.Fatalf("AppFGASubject() = %q, want %q", got, want)
	}
}

// TestAppFGASubject_NoDoublePrefix guards against a regression where
// AppFGASubject is accidentally handed an already-"oidc~"-prefixed value
// (e.g. if a future refactor threads AppJWTSubject's output through
// AppFGASubject twice) — authz.UserObject's idempotent prepend means this
// wouldn't double the prefix even if it happened, but the two helpers must
// independently agree on the single-prefix form for a fresh app UUID.
func TestAppFGASubject_NoDoublePrefix(t *testing.T) {
	appUUID := "00000000-0000-0000-0000-000000000001"
	sub := apps.AppJWTSubject(appUUID)
	fga := apps.AppFGASubject(appUUID)
	want := "user:oidc~" + sub
	if fga != want {
		t.Fatalf("AppFGASubject(%q) = %q, want %q (derived from AppJWTSubject %q)", appUUID, fga, want, sub)
	}
}
