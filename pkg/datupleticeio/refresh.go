package datupleticeio

import (
	"context"
	"sync"
)

// BearerTokenProvider returns the bearer JWT used to authenticate GCS
// credential-refresh calls back to lakekeeper. Called per-refresh (no
// caching at this layer); callers that want to memoise should do it
// inside their closure.
//
// Returning an error aborts the refresh; the previous (potentially
// stale) cached oauth2.Token is NOT returned — refreshingTokenSource
// propagates the error per RFC 019 §4.5.3.
type BearerTokenProvider func(ctx context.Context) (string, error)

// tokenProviderRegistry holds the package-level BearerTokenProvider
// installed by binaries that link this package (gateway, TableCommit).
// Read-mostly: SetTokenProvider is called once at startup; refresh paths
// call getTokenProvider on the hot path.
var tokenProviderRegistry struct {
	mu       sync.RWMutex
	provider BearerTokenProvider
}

// SetTokenProvider installs the package-level bearer-token provider
// consumed by the loadTable-refresh path inside the GCS factory.
// Call once at binary startup with a closure that returns the binary's
// current run-token (typically:
//
//	datupleticeio.SetTokenProvider(func(context.Context) (string, error) {
//	    return runToken, nil
//	})
//
// Subsequent calls overwrite the prior provider; concurrent reads
// remain safe via the registry's RWMutex.
//
// Without a provider installed, the loadTable-refresh path errors at
// invocation time. The factory still works for transactions that
// complete within the initial token TTL (which today is the common
// case), so this is opt-in fail-loud rather than fail-closed.
func SetTokenProvider(p BearerTokenProvider) {
	tokenProviderRegistry.mu.Lock()
	tokenProviderRegistry.provider = p
	tokenProviderRegistry.mu.Unlock()
}

// getTokenProvider returns the currently-installed provider, or nil
// when none has been set.
func getTokenProvider() BearerTokenProvider {
	tokenProviderRegistry.mu.RLock()
	defer tokenProviderRegistry.mu.RUnlock()
	return tokenProviderRegistry.provider
}
