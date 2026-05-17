// Package runtoken validates run-token JWTs against pipeline-api's JWKS using
// an 8-check validation contract.
//
// Returns ValidatedClaims; callers MUST NOT trust any claim that did not come
// from this package's output (i.e. do not parse the JWT independently after
// this validates it).
package runtoken

import (
	"context"
	"crypto/rsa"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

const (
	// ExpectedIssuer mirrors tokens.tokenIssuer in pkg/pipelineapi/tokens/mint.go.
	ExpectedIssuer = "datuplet-api"

	// ExpectedAudience mirrors tokens.TableTokenAudience.
	ExpectedAudience = "datuplet-catalog"

	// ExpectedTokenKind mirrors tokens.TokenKindRun.
	// The validator rejects service tokens, impersonation tokens, and local-CLI
	// tokens — only "run" kind is acceptable on the DG / TableCommit path.
	ExpectedTokenKind = "run"

	// ClockSkewSeconds is the permitted clock-skew tolerance applied to exp / nbf
	// checks. 60 s is consistent with typical JWT verifier defaults.
	ClockSkewSeconds = 60

	// maxRunTokenFileBytes caps run-token file reads to guard against an
	// outsized projected Secret. K8s caps Secrets at 1 MiB; 2 MiB gives
	// headroom for any framing while still preventing a runaway file from
	// OOM-ing the gateway. Mirrors the constant in the parent package.
	maxRunTokenFileBytes = 2 << 20
)

// ValidatedClaims is the result of a successful LoadAndValidateRunToken call.
// All fields are guaranteed non-empty — the validator rejects tokens that lack
// any of them.
type ValidatedClaims struct {
	RunID     string
	Subject   string
	ProjectID string
	Warehouse string
	Issuer    string
	Audience  string
	Expiry    time.Time
}

// JWKSClient is the interface the validator uses to look up an RSA public key
// by kid. pkg/datagateway/jwks.Client satisfies this interface.
type JWKSClient interface {
	KeyFor(ctx context.Context, kid string) (*rsa.PublicKey, error)
}

// LoadAndValidateRunToken reads a JWT from path, validates it against the
// JWKS returned by jwksClient, and returns the validated claims.
//
// The 8 checks (in order):
//  1. Token parses + signature verifies against the JWKS key matched by kid.
//  2. iss == ExpectedIssuer ("datuplet-api")
//  3. aud == ExpectedAudience ("datuplet-catalog")
//  4. exp not elapsed; nbf not in the future (±ClockSkewSeconds tolerance).
//  5. token_kind == ExpectedTokenKind ("run")
//  6. Required claims present and non-empty: project_id, warehouse, run_id, sub.
//  7. sub == run_id (no synthetic-identity spoofing).
//  8. run_id == expectedRunID (Secret-swap defence — binds the JWT to the
//     operator-supplied execution context env var RUN_ID).
//
// path == "" returns (nil, error) — the validator does NOT soft-degrade.
// Callers that want soft-degrade (e.g. static-backend mode with no mounted
// token) MUST check path emptiness themselves and skip calling this function.
//
// NEVER log token bytes; errors describe which check failed, not the raw JWT.
func LoadAndValidateRunToken(
	ctx context.Context,
	path string,
	jwksClient JWKSClient,
	expectedRunID string,
) (*ValidatedClaims, error) {
	// path == "" is an explicit fail: the validator does not soft-degrade.
	if path == "" {
		return nil, errors.New("run-token validation: path is empty (soft-degrade is caller's responsibility)")
	}

	raw, err := readBoundedFile(path)
	if err != nil {
		return nil, fmt.Errorf("run-token validation: read file %q: %w", path, err)
	}
	tokenStr := strings.TrimSpace(string(raw))

	// Check 1: parse + signature verification.
	// The keyfunc pins the signing method to RS256 *exactly* (rejecting RS384 /
	// RS512 / PSxxx / HMAC / none — they all share the *jwt.SigningMethodRSA
	// embedded type so the earlier type assertion let RS384 / RS512 through).
	// We compare against the package-level singleton; pointer equality is the
	// load-bearing check.
	// jwt.WithValidMethods adds a second layer of defense at the parser level.
	var claims jwt.MapClaims
	_, err = jwt.ParseWithClaims(tokenStr, &claims, func(t *jwt.Token) (interface{}, error) {
		if t.Method != jwt.SigningMethodRS256 {
			return nil, fmt.Errorf("check 1: unexpected signing method %q (want RS256 only)", t.Header["alg"])
		}
		kid, _ := t.Header["kid"].(string)
		if kid == "" {
			return nil, errors.New("check 1: kid header missing from token")
		}
		return jwksClient.KeyFor(ctx, kid)
	}, jwt.WithLeeway(ClockSkewSeconds*time.Second), jwt.WithValidMethods([]string{"RS256"}))
	if err != nil {
		return nil, fmt.Errorf("run-token validation check 1 (parse/signature): %w", err)
	}

	// Check 2: issuer.
	iss, _ := claims["iss"].(string)
	if iss != ExpectedIssuer {
		return nil, fmt.Errorf("run-token validation check 2 (iss): got %q, want %q", iss, ExpectedIssuer)
	}

	// Check 3: audience.
	// jwt-go v5 may deliver aud as string or []string depending on the token.
	aud, err := extractAudience(claims)
	if err != nil {
		return nil, fmt.Errorf("run-token validation check 3 (aud): %w", err)
	}
	if aud != ExpectedAudience {
		return nil, fmt.Errorf("run-token validation check 3 (aud): got %q, want %q", aud, ExpectedAudience)
	}

	// Check 4: exp / nbf are validated by jwt.ParseWithClaims above (with
	// the leeway we set). Nothing further to do here.

	// Check 5: token_kind.
	kind, _ := claims["token_kind"].(string)
	if kind != ExpectedTokenKind {
		return nil, fmt.Errorf("run-token validation check 5 (token_kind): got %q, want %q", kind, ExpectedTokenKind)
	}

	// Check 6: required claims present and non-empty.
	sub, _ := claims["sub"].(string)
	projectID, _ := claims["project_id"].(string)
	warehouse, _ := claims["warehouse"].(string)
	runID, _ := claims["run_id"].(string)

	if sub == "" {
		return nil, errors.New("run-token validation check 6: required claim sub is missing or empty")
	}
	if projectID == "" {
		return nil, errors.New("run-token validation check 6: required claim project_id is missing or empty")
	}
	if warehouse == "" {
		return nil, errors.New("run-token validation check 6: required claim warehouse is missing or empty")
	}
	if runID == "" {
		return nil, errors.New("run-token validation check 6: required claim run_id is missing or empty")
	}

	// Check 7: sub == run_id (no synthetic-identity spoofing).
	if sub != runID {
		// Do NOT include the actual values in the error — they identify the run
		// and would aid an attacker capturing error logs to confirm a swap.
		return nil, errors.New("run-token validation check 7: sub does not match run_id")
	}

	// Check 8: run_id == expectedRunID (Secret-swap defence).
	// Mirrors check 7's discipline: do NOT include the actual values in the
	// returned error. Both run IDs identify the run; logging the mismatch with
	// values would let an attacker with log-read access confirm a successful
	// Secret-swap attempt.
	if runID != expectedRunID {
		return nil, errors.New("run-token validation check 8: run_id does not match operator-supplied RUN_ID env — refusing to proceed")
	}

	// Extract expiry for the claims struct (it was validated by ParseWithClaims).
	var expiry time.Time
	if exp, ok := claims["exp"].(float64); ok {
		expiry = time.Unix(int64(exp), 0)
	}

	return &ValidatedClaims{
		RunID:     runID,
		Subject:   sub,
		ProjectID: projectID,
		Warehouse: warehouse,
		Issuer:    iss,
		Audience:  aud,
		Expiry:    expiry,
	}, nil
}

// extractAudience handles the JWT aud claim, which may be a single string or
// a []interface{} (when json.Unmarshal parses a JSON array) as emitted by
// golang-jwt/jwt/v5 MapClaims.
func extractAudience(claims jwt.MapClaims) (string, error) {
	raw, ok := claims["aud"]
	if !ok {
		return "", errors.New("aud claim missing")
	}
	switch v := raw.(type) {
	case string:
		if v == "" {
			return "", errors.New("aud claim is empty string")
		}
		return v, nil
	case []interface{}:
		if len(v) == 0 {
			return "", errors.New("aud claim is empty array")
		}
		s, ok := v[0].(string)
		if !ok || s == "" {
			return "", errors.New("aud claim first element is not a non-empty string")
		}
		// We expect exactly one audience for run tokens.
		if len(v) != 1 {
			return "", fmt.Errorf("aud claim has %d values, want exactly 1", len(v))
		}
		return s, nil
	default:
		return "", fmt.Errorf("aud claim has unexpected type %T", raw)
	}
}

// readBoundedFile opens path and reads up to maxRunTokenFileBytes bytes.
// If the limit is hit the file content is truncated; downstream JWT parse will
// fail (intentional: better than silently processing a garbled secret).
func readBoundedFile(path string) ([]byte, error) {
	f, err := os.Open(path) // #nosec G304 -- path is operator-controlled
	if err != nil {
		return nil, err
	}
	defer f.Close()
	buf, err := io.ReadAll(io.LimitReader(f, maxRunTokenFileBytes))
	if err != nil {
		return nil, err
	}
	if len(buf) == maxRunTokenFileBytes {
		log.Printf("runtoken: token file %q hit %d-byte read cap; downstream JWT parse will likely fail", path, maxRunTokenFileBytes)
	}
	return buf, nil
}
