// Package authz provides typed OpenFGA object constructors, an Authorizer
// interface, an OpenFGA SDK wrapper, and a per-request Check cache for
// pipeline-api's fine-grained authorization.
//
// Type names match lakekeeper's shipped collaboration-4.3 FGA model verbatim —
// see fga_model.fga.
package authz

import (
	"fmt"
	"strings"
)

// ObjectType is the FGA type name as it appears in the authorization model
// (e.g. "project", "table", "user"). Use the typed constructors below —
// never construct Object values with raw string concatenation.
type ObjectType string

const (
	TypeProject   ObjectType = "project"
	TypeWarehouse ObjectType = "warehouse"
	TypeNamespace ObjectType = "namespace"
	TypeTable     ObjectType = "table"
	TypeView      ObjectType = "view"
	TypeUser      ObjectType = "user"
)

// Object is an immutable FGA object reference (type:id). Use the typed
// constructors to create instances — they enforce the correct type strings
// and apply any required normalization (e.g. the oidc~ prefix on user ids).
type Object struct {
	kind string // FGA type name, one of the Type* constants above
	id   string // opaque identifier within that type
}

// String returns the canonical FGA wire form: "<type>:<id>".
// Examples:
//   - ProjectObject("abc").String()   == "project:abc"
//   - UserObject("alice").String()    == "user:oidc~alice"
//   - UserObject("oidc~alice").String() == "user:oidc~alice"  (idempotent)
func (o Object) String() string { return string(o.kind) + ":" + o.id }

// Type returns the object's FGA type.
func (o Object) Type() ObjectType { return ObjectType(o.kind) }

// ID returns the object's identifier within its type.
func (o Object) ID() string { return o.id }

// lakekeeperUserPrefix is hard-coded by lakekeeper when normalizing OIDC
// subjects into FGA user identifiers. Datuplet must match this prefix so
// tuples written by pipeline-api interoperate with those written by lakekeeper.
const lakekeeperUserPrefix = "oidc~"

// ProjectObject returns the FGA object for a lakekeeper project.
// The uuid parameter is the lakekeeper project UUID (stored in
// projects.lakekeeper_project_id — distinct from the Datuplet project UUID).
func ProjectObject(uuid string) Object { return Object{kind: string(TypeProject), id: uuid} }

// NamespaceObject returns the FGA object for a lakekeeper namespace.
//
// The name parameter is the dot-separated lakekeeper namespace identifier
// (e.g. "raw" or "joined.staging"). No normalization is applied: lakekeeper
// does not prefix namespace IDs the way it does user subjects (see
// UserObject's "oidc~" prepend), so the wire form is simply
// "namespace:<name>".
//
// Examples:
//   - NamespaceObject("raw").String()             == "namespace:raw"
//   - NamespaceObject("joined.staging").String()  == "namespace:joined.staging"
func NamespaceObject(name string) Object { return Object{kind: string(TypeNamespace), id: name} }

// UserObject returns the FGA object for a user, applying the oidc~ prefix
// that lakekeeper hard-codes when normalizing OIDC subjects.
//
// The prepend is idempotent: if sub already starts with "oidc~", it is not
// doubled. Callers may pass either the raw JWT sub or a pre-prefixed value.
func UserObject(sub string) Object {
	if !strings.HasPrefix(sub, lakekeeperUserPrefix) {
		sub = lakekeeperUserPrefix + sub
	}
	return Object{kind: string(TypeUser), id: sub}
}

// ParseObject is the inverse of Object.String() for the canonical
// "<type>:<id>" wire form. Used by store.run_tuples to round-trip
// tuples through Postgres without re-deriving them at completion time.
//
// The type segment is round-tripped verbatim — no validation against
// the known TypeProject / TypeWarehouse / etc. set, since lakekeeper's
// FGA model may grow new types we want to store and re-emit blindly.
// The id segment may itself contain ':' (e.g. user IDs with the
// "oidc~" prefix don't, but other types may); we split on the first
// ':' only.
func ParseObject(s string) (Object, error) {
	for i := 0; i < len(s); i++ {
		if s[i] == ':' {
			if i == 0 || i == len(s)-1 {
				return Object{}, fmt.Errorf("authz: ParseObject: malformed %q", s)
			}
			return Object{kind: s[:i], id: s[i+1:]}, nil
		}
	}
	return Object{}, fmt.Errorf("authz: ParseObject: missing ':' in %q", s)
}

// FGA relation names used across the workspace and pipeline-api FGA tuple
// write paths. These names match lakekeeper's shipped collaboration-4.3
// FGA model verbatim — changing them requires a coordinated lakekeeper
// model update.
const (
	// RelationEditor grants read + write access to a namespace or project.
	RelationEditor = "editor"
	// RelationViewer grants read-only access to a namespace or project.
	RelationViewer = "viewer"
)

// Tuple is a single FGA relationship tuple (user → relation → object).
// Used in WriteTuples and DeleteTuples calls.
type Tuple struct {
	// User is the FGA user string, e.g. "user:oidc~alice" or "user:run-<uuid>".
	// Use UserObject(...).String() to produce the canonical form.
	User     string
	Relation string
	Object   Object
}

// CheckQuery is a single authorization question for BatchCheck.
type CheckQuery struct {
	User     string
	Relation string
	Object   Object
}
