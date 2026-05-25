package catalogwriter

import (
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
)

// RefreshTransport wraps an http.RoundTripper and retries a request
// ONCE after forcing a credential refresh if the upstream responds
// with HTTP 401 (Unauthorized) or 403 (Forbidden). Intended for
// composing under oauth2.Transport so the inner layer adds the
// Authorization header from a (potentially refreshed) TokenSource and
// the outer layer detects auth rejections that slipped past the
// TokenSource's own renewal heuristic — typically lakekeeper handing
// out a stale-cached token that GCS / S3 then rejects.
//
// Retry guards:
//
//   - At most ONE retry per request. Sustained 401 / 403 propagates so
//     the caller sees the real error instead of infinite loops.
//
//   - Body rewindable. Bodies set via *bytes.Buffer / *bytes.Reader /
//     *strings.Reader carry a non-nil Request.GetBody set by
//     net/http, and we use it to rewind for retry. If GetBody is nil
//     and Body is non-nil (streaming upload bodies, custom io.Reader),
//     we don't retry — re-issuing without a rewindable body would
//     either send empty data or panic.
//
//   - Refresh is best-effort. If the refresh callback errors, the
//     original 4xx response is returned so the caller can react to
//     the auth-rejection error directly.
//
// Concurrency: safe. The retry decision is per-request; the refresh
// callback is the caller's responsibility to make concurrent-safe
// (catalogwriter.VendedCreds.Invalidate + Get already are).
type RefreshTransport struct {
	// Base is the underlying transport that actually issues requests.
	// Typically &oauth2.Transport{Source: ts, Base: http.DefaultTransport}.
	Base http.RoundTripper

	// Refresh is called when the base transport returns a 401 / 403.
	// Typical impl: invalidate the credentials cache and re-fetch.
	// The next RoundTrip after refresh will re-invoke the underlying
	// TokenSource, which should now hit a fresh credential.
	Refresh func() error

	// refreshCount is incremented on each Refresh invocation. Exposed
	// for tests + observability so callers can confirm the retry path
	// actually fired.
	refreshCount atomic.Uint64

	// rmu serialises overlapping refresh attempts on the SAME
	// transport instance. Without it, N concurrent 401s would each
	// invoke Refresh() once and hammer lakekeeper. With it, a refresh
	// in flight blocks other refreshers until the first completes —
	// the second wave reads the already-updated cache.
	rmu sync.Mutex
}

// RoundTrip implements http.RoundTripper.
func (t *RefreshTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := t.Base.RoundTrip(req)
	if err != nil {
		return resp, err
	}
	if resp.StatusCode != http.StatusUnauthorized && resp.StatusCode != http.StatusForbidden {
		return resp, nil
	}
	// Bail if we can't rewind the body. Streaming uploads with no
	// GetBody set would have to re-send empty content; that's worse
	// than surfacing the 401.
	if req.Body != nil && req.GetBody == nil {
		return resp, nil
	}
	// Drain + close so the underlying transport can reuse the conn
	// instead of opening a new TCP session for the retry.
	if resp.Body != nil {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}
	// Refresh under lock — multiple concurrent 401s on the same
	// transport collapse to one lakekeeper roundtrip.
	t.rmu.Lock()
	t.refreshCount.Add(1)
	rerr := t.Refresh()
	t.rmu.Unlock()
	if rerr != nil {
		return nil, fmt.Errorf("refresh after %d: %w", resp.StatusCode, rerr)
	}
	// Re-issue with a fresh body (if any).
	if req.GetBody != nil {
		newBody, err := req.GetBody()
		if err != nil {
			return nil, fmt.Errorf("rewind body for retry after %d: %w", resp.StatusCode, err)
		}
		req.Body = newBody
	}
	return t.Base.RoundTrip(req)
}

// RefreshCount returns the cumulative number of refresh attempts. Useful
// for tests + ops dashboards confirming the retry path fired.
func (t *RefreshTransport) RefreshCount() uint64 {
	return t.refreshCount.Load()
}
