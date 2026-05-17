// Package authztest provides an in-memory authz.Authorizer for use by
// downstream packages' tests. Mirrors the fakeAuthorizer in
// pkg/pipelineapi/authz/fake_authorizer_test.go (which is _test.go-only
// and therefore unreachable from outside that package).
//
// Cluster-mode handler tests use this fake to write FGA tuples for the
// relations mustHaveRelation checks. Because the fake is exact-match
// only (no chain inheritance), tests seed the canonical relations the
// handlers query directly — `data_admin` for writes, `describe` for
// reads — not the leaf Datuplet roles (`editor` / `viewer`) that would
// chain into them in real OpenFGA. See seedProjectAuthz in
// pkg/pipelineapi/http/run_handlers_test.go for the seeding pattern.
package authztest

import (
	"context"
	"fmt"
	"sync"

	"github.com/datuplet/datuplet/pkg/pipelineapi/authz"
)

// Fake is an in-memory Authorizer for use in tests. Stores tuples and
// evaluates Check with EXACT MATCH ONLY — no relationship inheritance.
// Tests that need inherited grants must write the intermediate tuples
// explicitly.
type Fake struct {
	mu     sync.RWMutex
	tuples map[fakeTupleKey]struct{}
}

type fakeTupleKey struct {
	user     string
	relation string
	object   string
}

// New constructs an empty Fake.
func New() *Fake {
	return &Fake{tuples: make(map[fakeTupleKey]struct{})}
}

// Allow seeds a single tuple — convenience for test setup.
func (f *Fake) Allow(user, relation string, obj authz.Object) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.tuples[fakeTupleKey{user, relation, obj.String()}] = struct{}{}
}

// Check implements authz.Authorizer.
func (f *Fake) Check(_ context.Context, user string, relation string, obj authz.Object) (bool, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	_, ok := f.tuples[fakeTupleKey{user, relation, obj.String()}]
	return ok, nil
}

// BatchCheck implements authz.Authorizer.
func (f *Fake) BatchCheck(ctx context.Context, queries []authz.CheckQuery) ([]bool, []error) {
	results := make([]bool, len(queries))
	errs := make([]error, len(queries))
	for i, q := range queries {
		results[i], errs[i] = f.Check(ctx, q.User, q.Relation, q.Object)
	}
	return results, errs
}

// ListObjects implements authz.Authorizer.
func (f *Fake) ListObjects(_ context.Context, user string, relation string, objType authz.ObjectType) ([]authz.Object, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	var out []authz.Object
	prefix := string(objType) + ":"
	for k := range f.tuples {
		if k.user == user && k.relation == relation {
			if len(k.object) > len(prefix) && k.object[:len(prefix)] == prefix {
				obj, err := authz.ParseObject(k.object)
				if err == nil {
					out = append(out, obj)
				}
			}
		}
	}
	return out, nil
}

// WriteTuples implements authz.Authorizer.
func (f *Fake) WriteTuples(_ context.Context, tuples []authz.Tuple) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, t := range tuples {
		f.tuples[fakeTupleKey{t.User, t.Relation, t.Object.String()}] = struct{}{}
	}
	return nil
}

// DeleteTuples implements authz.Authorizer.
func (f *Fake) DeleteTuples(_ context.Context, tuples []authz.Tuple) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, t := range tuples {
		key := fakeTupleKey{t.User, t.Relation, t.Object.String()}
		if _, ok := f.tuples[key]; !ok {
			return fmt.Errorf("tuple not found: %v %v %v", t.User, t.Relation, t.Object)
		}
		delete(f.tuples, key)
	}
	return nil
}

// Compile-time assertion.
var _ authz.Authorizer = (*Fake)(nil)
