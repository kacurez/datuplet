//go:build duckdb_arrow

package queryengine

import (
	"context"
	"strings"
	"testing"
)

// secretJWT is a deliberately recognizable token string. The security
// assertions below check that it NEVER appears in an error returned by
// attachCatalog — the JWT is embedded as a SQL literal in CREATE SECRET, and
// leaking the statement text (or the token) into an error/log is forbidden. It
// MUST be a structurally valid JWT shape (three base64url segments) so it
// survives the shape guard and actually reaches the CREATE SECRET / ATTACH
// path under test; the recognizable middle segment is base64url-safe.
const secretJWT = "eyJhbGciOiJub25lIn0.THIS-IS-A-SECRET-CATALOG-JWT-do-not-leak.c2ln"

// TestAttachCatalogRedactsTokenOnError drives attachCatalog against an ENDPOINT
// nothing is listening on (127.0.0.1:1). The attach must fail (no catalog
// reachable) and the resulting error must NOT contain the JWT — proving the
// error wrapping uses a redacted description, never the SQL statement text.
//
// No network egress: the connection refusal is purely local.
func TestAttachCatalogRedactsTokenOnError(t *testing.T) {
	ctx := context.Background()
	e, err := openEngine(ctx, Request{TempDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	err = attachCatalog(ctx, e, Request{
		LakekeeperURL: "http://127.0.0.1:1/catalog",
		Warehouse:     "00000000-0000-0000-0000-000000000000/wh",
		CatalogJWT:    secretJWT,
	})
	if err == nil {
		t.Fatal("attachCatalog against an unreachable endpoint must error")
	}
	if strings.Contains(err.Error(), secretJWT) {
		t.Fatalf("SECURITY: error leaked the catalog JWT: %v", err)
	}
}

// TestValidateJWTShape covers the shape guard that runs before any SQL is
// built (the load-bearing defence against transitive token leakage). Every
// invalid input must be rejected AND the returned error must never echo the
// input value. The one valid-shape input must pass validation and be driven
// through attachCatalog against an unreachable endpoint — proving the only
// error it can produce comes from a LATER step (the network attach), never the
// shape guard, and still never contains the token.
func TestValidateJWTShape(t *testing.T) {
	const validShape = "eyJhbGciOiJub25lIn0.eyJzdWIiOiJxZSJ9.c2ln"

	invalid := []struct {
		name string
		in   string
	}{
		{"empty", ""},
		{"one segment", "abc"},
		{"two segments", "abc.def"},
		{"four segments", "a.b.c.d"},
		{"empty middle segment", "abc..ghi"},
		{"segment with single quote", "ab'c.def.ghi"},
		{"segment with space", "abc.de f.ghi"},
		{"segment with newline", "abc.def.gh\ni"},
		{"segment with null byte", "abc.def.gh\x00i"},
		{"base64 padding char", "abc.def.gh=i"},
	}
	for _, tc := range invalid {
		t.Run("invalid/"+tc.name, func(t *testing.T) {
			err := validateJWTShape(tc.in)
			if err == nil {
				t.Fatalf("validateJWTShape(%q) = nil, want error", tc.in)
			}
			// Only redact non-empty inputs: "" cannot meaningfully "appear" in
			// the message, and strings.Contains(msg, "") is always true.
			if tc.in != "" && strings.Contains(err.Error(), tc.in) {
				t.Fatalf("SECURITY: error leaked the input value: %v", err)
			}
		})
	}

	t.Run("valid shape proceeds past validation", func(t *testing.T) {
		if err := validateJWTShape(validShape); err != nil {
			t.Fatalf("validateJWTShape(%q) = %v, want nil", validShape, err)
		}

		ctx := context.Background()
		e, err := openEngine(ctx, Request{TempDir: t.TempDir()})
		if err != nil {
			t.Fatal(err)
		}
		defer e.Close()

		// Unreachable endpoint: attachCatalog must get PAST the shape guard and
		// fail later at the network attach.
		err = attachCatalog(ctx, e, Request{
			LakekeeperURL: "http://127.0.0.1:1/catalog",
			Warehouse:     "00000000-0000-0000-0000-000000000000/wh",
			CatalogJWT:    validShape,
		})
		if err == nil {
			t.Fatal("attachCatalog against an unreachable endpoint must error")
		}
		if strings.Contains(err.Error(), "not a valid JWT shape") {
			t.Fatalf("valid shape was wrongly rejected by the shape guard: %v", err)
		}
		if strings.Contains(err.Error(), validShape) {
			t.Fatalf("SECURITY: error leaked the catalog JWT: %v", err)
		}
	})
}

// TestAttachCatalogRejectsLockedEngine asserts attachCatalog refuses to run
// against an already-locked engine. ATTACH needs mutable config and the secret
// manager must initialize before disabled_filesystems is applied, so attaching
// after lock() is a programming error that must be caught immediately — before
// any SQL touches the (locked) connection.
func TestAttachCatalogRejectsLockedEngine(t *testing.T) {
	ctx := context.Background()
	e, err := openEngine(ctx, Request{TempDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if err := e.lock(ctx); err != nil {
		t.Fatal(err)
	}

	err = attachCatalog(ctx, e, Request{
		LakekeeperURL: "http://127.0.0.1:1/catalog",
		Warehouse:     "00000000-0000-0000-0000-000000000000/wh",
		CatalogJWT:    secretJWT,
	})
	if err == nil {
		t.Fatal("attachCatalog must refuse a locked engine")
	}
	if strings.Contains(err.Error(), secretJWT) {
		t.Fatalf("SECURITY: error leaked the catalog JWT: %v", err)
	}
}
