package authz

import (
	"context"
	"fmt"
	"sync"
)

// ServerAdminChecker resolves the FGA server object once (memoized) and
// answers "is this user a platform superadmin?". Backed by
// OpenFGAAuthorizer in production; stubbed in tests. Discovery is lazy +
// memoized: the first IsServerAdmin call runs DiscoverServerObject, caches
// the server:<uuid> string, and every subsequent call is a plain Check.
type ServerAdminChecker interface {
	// ServerObject returns the memoized server:<uuid> wire string,
	// discovering it on first call. Safe for concurrent use.
	ServerObject(ctx context.Context) (string, error)
	// IsServerAdmin returns true iff (user:oidc~<userUUID>, admin, server:<uuid>)
	// holds. userUUID is the DB user UUID (NOT pre-prefixed); the impl
	// applies UserObject normalization. ErrAuthzUnavailable propagates.
	IsServerAdmin(ctx context.Context, userUUID string) (bool, error)
}

// serverAdmin is the production ServerAdminChecker. It memoizes the
// server:<uuid> object discovered from the FGA /changes feed and then
// answers IsServerAdmin with a plain Check. The lakekeeper server object
// is written exactly once (first bootstrap) and never changes, so a
// process-lifetime memo with no TTL is correct.
//
// discover is a seam: NewServerAdmin wires it to DiscoverServerObject over
// the configured FGA endpoint; tests inject a stub to avoid live HTTP and
// to assert the discovery-once memoization guarantee.
type serverAdmin struct {
	authzr   Authorizer
	discover func(ctx context.Context) (string, error)

	mu     sync.Mutex
	server string // memoized "server:<uuid>"; "" until first discovery
}

// NewServerAdmin builds a ServerAdminChecker. fgaURL/apiKey/storeID feed
// the one-time /changes discovery; authzr issues the Check. All are
// available at pipeline-api startup (main.go resolves storeID via
// ResolveStoreAndModel before constructing the OpenFGAAuthorizer).
func NewServerAdmin(authzr Authorizer, fgaURL, apiKey, storeID string) ServerAdminChecker {
	return &serverAdmin{
		authzr: authzr,
		discover: func(ctx context.Context) (string, error) {
			return DiscoverServerObject(ctx, fgaURL, apiKey, storeID)
		},
	}
}

func (s *serverAdmin) ServerObject(ctx context.Context) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.server != "" {
		return s.server, nil
	}
	obj, err := s.discover(ctx)
	if err != nil {
		return "", fmt.Errorf("discover server object: %w", err)
	}
	s.server = obj
	return obj, nil
}

func (s *serverAdmin) IsServerAdmin(ctx context.Context, userUUID string) (bool, error) {
	serverWire, err := s.ServerObject(ctx)
	if err != nil {
		return false, err
	}
	obj, err := ParseObject(serverWire) // "server:<uuid>" → Object{server,<uuid>}
	if err != nil {
		return false, fmt.Errorf("parse server object %q: %w", serverWire, err)
	}
	return s.authzr.Check(ctx, UserObject(userUUID).String(), "admin", obj)
}
