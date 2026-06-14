// Package e2e — RFC 022 Task 3.3: BYO-local (mode c) query e2e.
//
// This validates the CLUSTER-specific half of the BYO-local flow: the
// POST /api/v1/query/token mint endpoint (Task 3.2), against the real
// pipeline-api. It is the server-side contract the laptop `datuplet-query`
// CLI (Task 3.1) depends on.
//
// # What is covered where
//
//   - ON → 200 (HERE): the e2e cluster is deployed with allowClientSideQuery=true
//     (tests/e2e/values-app.yaml), so the real pipeline-api mints a real,
//     FGA-scoped query JWT for the authenticated principal. This proves the
//     chart wiring (policy env → handler), the auth.WithUser gate, and the
//     token's claim shape end to end against a live deploy.
//   - OFF → 403 refusal (UNIT): the policy-off path is covered by
//     pkg/pipelineapi/http TestQueryTokenHandler_PolicyOff_Forbidden. We do NOT
//     toggle the policy here: flipping DATUPLET_ALLOW_CLIENT_SIDE_QUERY at test
//     time forces a pipeline-api rollout, and rollout churn destabilizes the
//     NodePort the framework reaches pipeline-api through, skipping unrelated
//     query scenarios. A fixed deploy-time policy keeps the suite deterministic.
//   - Full local EXECUTION (mint → queryengine.Run → render rows, FGA, exit
//     codes): proven by the duckdb-tagged integration test
//     components/queryengine/cmd/datuplet-query/integration_test.go against a
//     host-reachable docker lakekeeper+MinIO fixture. It cannot run against the
//     cluster warehouse from the host: lakekeeper vends STS creds pointing at
//     the in-cluster object-store endpoint, which OrbStack does not surface to
//     the host (the same limitation that skips TestQuery_NetworkPolicyEgress).
package e2e

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/datuplet/datuplet/tests/e2e/framework"
)

// postQueryToken sends POST /api/v1/query/token authenticated with a session
// cookie (the endpoint is behind auth.WithUser, same as POST /api/v1/query).
// The body is "{}" — the handler ignores it (the subject comes from the auth
// context, never the request). Returns (statusCode, body).
func postQueryToken(ctx context.Context, sessionCookie string) (int, []byte, error) {
	u := framework.PipelineAPIBaseURL() + "/api/v1/query/token"
	r, err := http.NewRequestWithContext(ctx, http.MethodPost, u, strings.NewReader("{}"))
	if err != nil {
		return 0, nil, fmt.Errorf("build request: %w", err)
	}
	r.Header.Set("Content-Type", "application/json")
	if sessionCookie != "" {
		r.AddCookie(&http.Cookie{Name: "pipeline_api_session", Value: sessionCookie})
	}
	cli := &http.Client{Timeout: 30 * time.Second}
	resp, err := cli.Do(r)
	if err != nil {
		return 0, nil, fmt.Errorf("POST /api/v1/query/token: %w", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, body, nil
}

// decodeJWTClaims decodes (without signature verification — the unit test in
// pkg/pipelineapi/http already verifies the signature) the claims segment of a
// compact JWS. e2e only needs to confirm the minted token carries the query
// claim shape the laptop engine relies on.
func decodeJWTClaims(t *testing.T, token string) map[string]any {
	t.Helper()
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		t.Fatalf("minted token is not a compact JWT (got %d segments)", len(parts))
	}
	raw, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		t.Fatalf("decode JWT claims segment: %v", err)
	}
	var claims map[string]any
	if err := json.Unmarshal(raw, &claims); err != nil {
		t.Fatalf("unmarshal JWT claims: %v", err)
	}
	return claims
}

// TestQuery_LocalMint_Enabled proves the POST /api/v1/query/token mint endpoint
// end to end: with the e2e cluster deployed with allowClientSideQuery=true, the
// real pipeline-api mints a fresh query JWT (aud=datuplet-catalog,
// token_kind=query, sub=the authenticated principal) — the credential the
// laptop-side datuplet-query CLI attaches to lakekeeper with.
//
// The policy-OFF 403 refusal is covered by the unit test
// (pkg/pipelineapi/http TestQueryTokenHandler_PolicyOff_Forbidden); see the
// package doc for why we don't toggle the policy at e2e time.
func TestQuery_LocalMint_Enabled(t *testing.T) {
	if framework.SharedHarness() == nil {
		t.Skip("SharedHarness nil")
	}
	if !framework.PipelineAPIReachable() {
		t.Skip("pipeline-api not reachable")
	}
	ctx := context.Background()
	session := getAdminSession(t)

	status, body, err := postQueryToken(ctx, session)
	if err != nil {
		t.Fatalf("POST /api/v1/query/token: %v", err)
	}
	t.Logf("local-mint status=%d", status)
	if status != http.StatusOK {
		t.Fatalf("status=%d body=%s, want 200 (allowClientSideQuery=true in the e2e deploy)",
			status, truncateLog(body, 256))
	}

	var ok struct {
		Token     string `json:"token"`
		ExpiresAt string `json:"expires_at"`
	}
	if err := json.Unmarshal(body, &ok); err != nil {
		t.Fatalf("decode 200 body: %v (raw: %s)", err, string(body))
	}
	if ok.Token == "" {
		t.Fatal("token is empty")
	}
	if ok.ExpiresAt == "" {
		t.Fatal("expires_at is empty")
	}

	// The minted token must carry the query claim shape the laptop engine
	// presents to lakekeeper. (Signature is verified by the pkg/pipelineapi/http
	// unit test; here we assert the claim shape end to end.)
	claims := decodeJWTClaims(t, ok.Token)
	if got := audClaim(claims); got != "datuplet-catalog" {
		t.Errorf("minted token aud = %q, want datuplet-catalog", got)
	}
	if got, _ := claims["token_kind"].(string); got != "query" {
		t.Errorf("minted token token_kind = %q, want query", got)
	}
	if got, _ := claims["sub"].(string); got == "" {
		t.Error("minted token sub is empty — must be the authenticated principal")
	}
	t.Logf("local-mint verified: 200 query token (aud=%v token_kind=%v)",
		audClaim(claims), claims["token_kind"])
}

// audClaim normalizes the JWT `aud` claim, which JSON-decodes to either a
// string (single audience) or a []any (multiple), into a single string.
func audClaim(claims map[string]any) string {
	switch v := claims["aud"].(type) {
	case string:
		return v
	case []any:
		if len(v) == 1 {
			if s, ok := v[0].(string); ok {
				return s
			}
		}
	}
	return ""
}
