package authz

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"time"
)

var serverObjectPattern = regexp.MustCompile(`^server:[0-9a-f-]+$`)

// DiscoverServerObject scans the FGA /changes feed for the single
// server:<uuid> tuple lakekeeper writes on first bootstrap and returns its
// wire form. Ported from cmd/pipeline-api/admin_lakekeeper.go so both the
// bootstrap CLI and serve-time superadmin checks share one implementation.
func DiscoverServerObject(ctx context.Context, fgaURL, apiKey, storeID string) (string, error) {
	c := &http.Client{Timeout: 30 * time.Second}
	token := ""
	for i := 0; i < 100; i++ {
		u := fmt.Sprintf("%s/stores/%s/changes?page_size=100", strings.TrimRight(fgaURL, "/"), storeID)
		if token != "" {
			u += "&continuation_token=" + token
		}
		req, _ := http.NewRequestWithContext(ctx, "GET", u, nil)
		if apiKey != "" {
			req.Header.Set("Authorization", "Bearer "+apiKey)
		}
		resp, err := c.Do(req)
		if err != nil {
			// Unreachable-FGA transport errors (connection refused, DNS,
			// timeout, canceled ctx) map to ErrAuthzUnavailable so the first
			// superadmin check emits 503, not a misclassified 500.
			return "", unavailableIfTransport(ctx, err)
		}
		// A non-2xx response means FGA could not give an authoritative answer
		// (e.g. 5xx while degraded). Fail closed to ErrAuthzUnavailable rather
		// than decode the error body into an empty page and mislabel it as
		// "no server:<uuid> tuple found".
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			resp.Body.Close()
			return "", fmt.Errorf("%w: FGA /changes returned HTTP %d", ErrAuthzUnavailable, resp.StatusCode)
		}
		var page struct {
			Changes []struct {
				TupleKey struct{ Object string } `json:"tuple_key"`
			} `json:"changes"`
			ContinuationToken string `json:"continuation_token"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&page); err != nil {
			resp.Body.Close()
			return "", err
		}
		resp.Body.Close()
		for _, ch := range page.Changes {
			if serverObjectPattern.MatchString(ch.TupleKey.Object) {
				return ch.TupleKey.Object, nil
			}
		}
		if page.ContinuationToken == "" {
			break
		}
		token = page.ContinuationToken
	}
	return "", fmt.Errorf("no server:<uuid> tuple found")
}
