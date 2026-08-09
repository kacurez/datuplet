package apps_test

// audit_test.go pins the control-plane audit vocabulary (RFC 028 §9) that
// fires from the AUTHOR routes. The identity-lifecycle events
// (app_identity_created / app_identity_deleted / impersonation_minted) fire
// from the IdentityManager and are covered in identity_manager_test.go.
//
// These names are a contract with operators (log queries / alerts), so they
// are asserted literally rather than "some line was emitted".

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/google/uuid"
)

func decodeJSON(t *testing.T, body []byte, dst any) {
	t.Helper()
	if err := json.Unmarshal(body, dst); err != nil {
		t.Fatalf("decode response: %v (body %s)", err, body)
	}
}

// TestAudit_AppPromoted asserts the app_promoted{from,to} record. `from` is
// the CAS-expected production hash (empty on a first promote), `to` is the
// newly promoted one — an operator must be able to reconstruct the rollout
// from the audit trail alone.
func TestAudit_AppPromoted(t *testing.T) {
	h := newAppsHarness(t)
	appID, hash1 := h.putApp("dash1", []byte("bundle-one"))
	_, hash2 := h.putApp("dash1", []byte("bundle-two"))

	// First promote: from = "" (production unset).
	capture := captureAudit(t)
	w := h.do(http.MethodPost, h.appsPath("/dash1/promote"),
		`{"version":"`+hash1+`","expectedProduction":null}`)
	if w.Code != http.StatusOK {
		t.Fatalf("first promote: status = %d, body = %s", w.Code, w.Body.String())
	}
	rec := capture.findAction(t, "app_promoted")
	if got, _ := rec["app_id"].(string); got != appID {
		t.Errorf("app_id = %q, want %q", got, appID)
	}
	if got, ok := rec["from"]; !ok {
		t.Errorf(`audit record has no "from" field: %v`, rec)
	} else if s, _ := got.(string); s != "" {
		t.Errorf("from = %q, want \"\" on a first promote", s)
	}
	if got, _ := rec["to"].(string); got != hash1 {
		t.Errorf("to = %q, want %q", got, hash1)
	}

	// Second promote: from = the previous production hash.
	capture2 := captureAudit(t)
	w = h.do(http.MethodPost, h.appsPath("/dash1/promote"),
		`{"version":"`+hash2+`","expectedProduction":"`+hash1+`"}`)
	if w.Code != http.StatusOK {
		t.Fatalf("second promote: status = %d, body = %s", w.Code, w.Body.String())
	}
	rec = capture2.findAction(t, "app_promoted")
	if got, _ := rec["from"].(string); got != hash1 {
		t.Errorf("from = %q, want %q", got, hash1)
	}
	if got, _ := rec["to"].(string); got != hash2 {
		t.Errorf("to = %q, want %q", got, hash2)
	}
}

// TestAudit_ViewerTokenMintedAndRevoked asserts the viewer_token_minted /
// viewer_token_revoked records AND that neither carries the plaintext secret —
// the token transits exactly once, in the mint response body (spec §5.3).
func TestAudit_ViewerTokenMintedAndRevoked(t *testing.T) {
	h := newAppsHarness(t)
	appID, _ := h.putApp("dash1", []byte("bundle"))

	capture := captureAudit(t)
	w := h.do(http.MethodPost, h.appsPath("/dash1/tokens"), "")
	if w.Code != http.StatusCreated {
		t.Fatalf("mint token: status = %d, body = %s", w.Code, w.Body.String())
	}
	var minted struct {
		TokenID string `json:"token_id"`
		Token   string `json:"token"`
	}
	decodeJSON(t, w.Body.Bytes(), &minted)

	rec := capture.findAction(t, "viewer_token_minted")
	if got, _ := rec["app_id"].(string); got != appID {
		t.Errorf("app_id = %q, want %q", got, appID)
	}
	if got, _ := rec["token_id"].(string); got != minted.TokenID {
		t.Errorf("token_id = %q, want %q", got, minted.TokenID)
	}
	assertNoSecretInLog(t, capture.raw(), minted.Token, minted.TokenID)

	capture2 := captureAudit(t)
	w = h.do(http.MethodDelete, h.appsPath("/dash1/tokens/"+minted.TokenID), "")
	if w.Code != http.StatusNoContent {
		t.Fatalf("revoke token: status = %d, body = %s", w.Code, w.Body.String())
	}
	rec = capture2.findAction(t, "viewer_token_revoked")
	if got, _ := rec["token_id"].(string); got != minted.TokenID {
		t.Errorf("token_id = %q, want %q", got, minted.TokenID)
	}
	assertNoSecretInLog(t, capture2.raw(), minted.Token, minted.TokenID)
}

// assertNoSecretInLog checks that neither the composed `vw_<id>.<secret>`
// token nor its secret half reached the log. The token_id alone IS allowed
// (it is the audit handle), so the secret is checked separately from the
// composed value.
func assertNoSecretInLog(t *testing.T, logged, token, tokenID string) {
	t.Helper()
	if logged == "" {
		t.Fatal("no audit output captured — this assertion would be vacuous")
	}
	if strings.Contains(logged, token) {
		t.Error("the plaintext viewer token appears in a log line")
	}
	if _, secret, ok := strings.Cut(token, "."); ok {
		if strings.Contains(logged, secret) {
			t.Error("the viewer-token secret appears in a log line")
		}
	} else {
		t.Fatalf("unexpected viewer-token shape %q (want vw_<id>.<secret>)", strings.Repeat("*", len(token)))
	}
	if !strings.Contains(logged, tokenID) {
		t.Errorf("token_id %q is missing from the audit line — the record has no handle", tokenID)
	}
}

// TestAudit_AppLifecycleEventsFireFromHandlers is a smoke test over the two
// remaining author-route records so a rename cannot silently drop them.
func TestAudit_AppLifecycleEventsFireFromHandlers(t *testing.T) {
	h := newAppsHarness(t)

	capture := captureAudit(t)
	appID, hash := h.putApp("dash1", []byte("bundle"))
	rec := capture.findAction(t, "put_version")
	if got, _ := rec["app_id"].(string); got != appID {
		t.Errorf("put_version app_id = %q, want %q", got, appID)
	}
	if got, _ := rec["version_hash"].(string); got != hash {
		t.Errorf("put_version version_hash = %q, want %q", got, hash)
	}

	capture2 := captureAudit(t)
	if w := h.do(http.MethodDelete, h.appsPath("/dash1"), ""); w.Code != http.StatusNoContent {
		t.Fatalf("delete: status = %d, body = %s", w.Code, w.Body.String())
	}
	rec = capture2.findAction(t, "delete_app")
	if got, _ := rec["app_id"].(string); got != appID {
		t.Errorf("delete_app app_id = %q, want %q", got, appID)
	}
	if _, err := uuid.Parse(appID); err != nil {
		t.Fatalf("app id is not a uuid: %v", err)
	}
}
