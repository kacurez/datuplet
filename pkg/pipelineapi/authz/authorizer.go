package authz

import (
	"context"
	"errors"
)

// ErrAuthzUnavailable is returned when the FGA backend is unreachable or
// times out. HTTP handlers should map this to 503 Service Unavailable.
// It is distinct from a "denied" result (which returns false, nil) and
// from an unexpected API error (which returns false, <other error>).
var ErrAuthzUnavailable = errors.New("authz backend unavailable")

// Authorizer is the primary interface for fine-grained authorization checks.
// All methods accept a context — callers should use
// WithRequestCache to wrap calls within a single HTTP request scope.
//
// Implementations must be safe for concurrent use.
type Authorizer interface {
	// Check returns (true, nil) if user has relation on obj, (false, nil) if
	// denied, or (false, ErrAuthzUnavailable) if the backend is unreachable.
	Check(ctx context.Context, user string, relation string, obj Object) (bool, error)

	// BatchCheck evaluates multiple CheckQuery entries in a single round-trip.
	// Results are returned in the same order as queries. An unavailable backend
	// returns ErrAuthzUnavailable; per-query errors propagate individually via
	// the error slice (nil on success). Use BatchCheck in storage-browse paths
	// that need to filter many objects — it is ~14× faster than N sequential
	// Checks (benchmarked against the OpenFGA service).
	BatchCheck(ctx context.Context, queries []CheckQuery) ([]bool, []error)

	// ListObjects returns all objects of type objType on which user has relation.
	// The returned slice contains objects in FGA's arbitrary ordering.
	ListObjects(ctx context.Context, user string, relation string, objType ObjectType) ([]Object, error)

	// WriteTuples creates the given relationship tuples in the FGA store.
	// Duplicate tuples are accepted (idempotent at the caller's responsibility;
	// OpenFGA returns an error on strict duplicates — callers should check).
	WriteTuples(ctx context.Context, tuples []Tuple) error

	// DeleteTuples removes the given relationship tuples from the FGA store.
	// Deleting non-existent tuples is an error in transaction mode — callers
	// should track what was written.
	DeleteTuples(ctx context.Context, tuples []Tuple) error
}
