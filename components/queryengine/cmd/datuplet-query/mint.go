package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// mintHTTPClient is dedicated to the per-invocation query-token mint call. A
// 30s timeout guards against an unresponsive pipeline-api hanging the CLI.
// CheckRedirect refuses ALL redirects so the api-token bearer can never be
// forwarded to a redirect target (mirrors pkg/pipelineapi/queryproxy/client.go;
// even same-host 307s would otherwise replay the Authorization header + body).
var mintHTTPClient = &http.Client{
	Timeout: 30 * time.Second,
	CheckRedirect: func(*http.Request, []*http.Request) error {
		return fmt.Errorf("datuplet-query: unexpected redirect from pipeline-api (refused)")
	},
}

// mintTokenResponse is the 200 body of POST /api/v1/query/token. Mirrors the
// FIXED contract Task 3.2 will implement server-side (RFC 022 §5.3).
type mintTokenResponse struct {
	Token     string `json:"token"`
	ExpiresAt string `json:"expires_at"`
}

// mintErrorResponse is the error envelope (mirrors the query-worker / proxy
// {"error","kind"} shape). On policy-off, kind="forbidden".
type mintErrorResponse struct {
	Error string `json:"error"`
	Kind  string `json:"kind"`
}

// mintQueryToken POSTs to {pipelineAPIURL}/api/v1/query/token with the
// api-token as the bearer and returns the freshly-minted short-lived query
// JWT (aud=datuplet-catalog, token_kind=query). On the policy-off 403 it
// surfaces the server's clear refusal message verbatim so `datuplet-query`
// prints a clean error rather than crashing (RFC 022 §4.1 / §5.3).
//
// SECURITY: the api-token is sent only in the Authorization header; neither
// it nor the returned token is logged.
func mintQueryToken(ctx context.Context, pipelineAPIURL, apiToken string) (string, error) {
	url := strings.TrimRight(pipelineAPIURL, "/") + "/api/v1/query/token"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader([]byte(`{}`)))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+apiToken)

	resp, err := mintHTTPClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("mint query token (POST %s): %w", url, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))

	if resp.StatusCode == http.StatusOK {
		var ok mintTokenResponse
		if err := json.Unmarshal(body, &ok); err != nil {
			return "", fmt.Errorf("decode mint response: %w", err)
		}
		if ok.Token == "" {
			return "", fmt.Errorf("mint endpoint returned an empty token")
		}
		return ok.Token, nil
	}

	// Non-200: surface the server's clear error message. The policy-off 403
	// is a clean refusal — print its message, do not crash.
	var er mintErrorResponse
	if jerr := json.Unmarshal(body, &er); jerr == nil && er.Error != "" {
		if resp.StatusCode == http.StatusForbidden {
			return "", fmt.Errorf("%s", er.Error)
		}
		return "", fmt.Errorf("mint query token failed (HTTP %d): %s", resp.StatusCode, er.Error)
	}
	return "", fmt.Errorf("mint query token failed (HTTP %d): %s", resp.StatusCode, strings.TrimSpace(string(body)))
}
