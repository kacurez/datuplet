package http

import (
	"errors"
	"log"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/datuplet/datuplet/pkg/pipelineapi/auth"
	"github.com/datuplet/datuplet/pkg/pipelineapi/store"
	"github.com/datuplet/datuplet/pkg/pipelineapi/tokens"
)

type cliTokenRequest struct {
	Email    string `json:"email"`
	Password string `json:"password"`
}

type cliTokenCluster struct {
	LakekeeperURL string `json:"lakekeeper_url"`
	WarehouseName string `json:"warehouse_name"`
}

type cliTokenProject struct {
	ID                  string `json:"id"`
	Name                string `json:"name"`
	LakekeeperProjectID string `json:"lakekeeper_project_id"`
}

type cliTokenResponse struct {
	Token        string            `json:"token"`
	ExpiresAt    string            `json:"expires_at"`
	UserID       string            `json:"user_id"`
	Cluster      cliTokenCluster   `json:"cluster"`
	Projects     []cliTokenProject `json:"projects"`
	// APIToken is an RS256 JWT scoped to pipeline-api itself
	// (aud=datuplet-api, token_kind=cli-api). CLI subcommands that call
	// pipeline-api endpoints (trigger, storage) must use this token via
	// Authorization: Bearer, NOT the catalog token above. Existing CLI
	// clients that don't read this field are unaffected (backward-compat).
	APIToken     string `json:"api_token"`
	APIExpiresAt string `json:"api_expires_at"`
}

// defaultCLITokenLifetime is the default TTL for the local-cli + cli-api
// JWTs minted by `datuplet login --remote`. 24h matches the run-token
// TTL (runbackend.RunTokenLifetime) and is comfortable for an all-day
// dev session without forcing a re-login every hour.
//
// Operators who want shorter TTLs (production environments where leaked
// token blast radius matters more) override via the
// DATUPLET_CLI_TOKEN_LIFETIME env var on pipeline-api — see
// cliTokenLifetime() below.
//
// The same TTL applies to BOTH the local-cli (lakekeeper-catalog scope)
// AND cli-api (pipeline-api scope) tokens that this handler mints in
// one response; we keep them in lockstep so the CLI's session lifetime
// is single-axis from the operator's point of view.
//
// Either way, lakekeeper-side authz is what actually governs what the
// token can do — cancellation goes through FGA tuple deletion, not
// token-level revocation, so a long TTL doesn't extend privilege
// beyond the user's current grants.
const defaultCLITokenLifetime = 24 * time.Hour

// maxCLITokenLifetime is the upper bound on operator overrides — guards
// against accidental "never expires" tokens (e.g. someone setting the
// env to "8760h" — a year — by mistake). 30 days is long enough for
// the longest plausible dev workflow and short enough that a leaked
// token isn't a forever liability.
const maxCLITokenLifetime = 30 * 24 * time.Hour

// cliTokenLifetime resolves the CLI-token TTL from env (with bounds
// + validation), falling back to defaultCLITokenLifetime. Invalid /
// out-of-range values log a warning and use the default — never panic
// or fail-closed, because the login flow is the bootstrap-out-of-bad
// state and shouldn't be undermined by a bad operator env.
func cliTokenLifetime() time.Duration {
	raw := os.Getenv("DATUPLET_CLI_TOKEN_LIFETIME")
	if raw == "" {
		return defaultCLITokenLifetime
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		log.Printf("pipeline-api: DATUPLET_CLI_TOKEN_LIFETIME=%q is not a valid Go duration (%v); using default %s",
			raw, err, defaultCLITokenLifetime)
		return defaultCLITokenLifetime
	}
	if d <= 0 {
		log.Printf("pipeline-api: DATUPLET_CLI_TOKEN_LIFETIME=%q must be positive; using default %s",
			raw, defaultCLITokenLifetime)
		return defaultCLITokenLifetime
	}
	if d > maxCLITokenLifetime {
		log.Printf("pipeline-api: DATUPLET_CLI_TOKEN_LIFETIME=%q exceeds %s cap; clamping to %s",
			raw, maxCLITokenLifetime, maxCLITokenLifetime)
		return maxCLITokenLifetime
	}
	return d
}

// clientIPFromRemoteAddr strips the port from r.RemoteAddr so the rate
// limiter keys on the IP rather than the (IP, ephemeral-port) tuple. NAT
// gateways and reverse-proxy clients reuse the same IP across many
// outbound connections; without this strip, each TCP connection would
// get its own bucket and the limiter would be useless.
//
// `net.SplitHostPort` returns an error on inputs that don't look like
// host:port (e.g. when the test framework sets RemoteAddr=""). In that
// case we fall back to the raw value — better to over-rate-limit a
// malformed input than to crash the request.
func clientIPFromRemoteAddr(remoteAddr string) string {
	host, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		return remoteAddr
	}
	return host
}

// handleCLIToken implements the password-grant endpoint backing the
// `datuplet login --remote` flow. On success it returns
// an RS256 JWT (1h TTL, token_kind=local-cli, aud=datuplet-catalog)
// + the deploy-time cluster metadata the CLI needs to talk to
// lakekeeper directly.
//
// # Security posture (P1 — read before changing)
//
//  1. Per-IP rate limiting fires BEFORE password verification. argon2id
//     is the most expensive thing in this handler; a brute-force attempt
//     must be rejected before it consumes CPU.
//  2. The handler reuses dummyPasswordHash from auth_handlers.go so
//     VerifyPassword runs the full argon2 cost path even when the email
//     is unknown — without this, response-latency timing leaks would
//     enumerate registered emails. NEVER short-circuit the password
//     check based on `user == nil`.
//  3. Disabled users (`DisabledAt != nil`) are rejected. Mirrors
//     handleLogin verbatim.
//  4. Errors return "invalid credentials" without disclosing whether
//     the email or password failed (or whether the account is disabled).
//  5. The handler NEVER logs the password or the minted token. Failures
//     log only the error type, never the input.
//  6. No DB writes. No FGA tuple writes. The user's existing FGA grants
//     drive lakekeeper authz on the run-token side.
//
// # Wire shape
//
// Request:  {"email": "alice@example.com", "password": "hunter2"}
// Response (200): {"token": "...", "expires_at": "...", "user_id": "...",
//
//	"cluster": {"lakekeeper_url": "...", "warehouse_name": "..."}}
//
// Errors:
//
//	400 — body unparseable, or email/password empty
//	401 — invalid credentials (covers unknown email, wrong password,
//	      disabled user — uniform error to prevent enumeration)
//	429 — per-IP rate limit exhausted
//	500 — DB unavailable, signer failure, or other infra error
func (s *Server) handleCLIToken(w http.ResponseWriter, r *http.Request) {
	// Step 1: rate limit BEFORE the DB lookup or argon2 verify so a
	// brute-force attempt can't waste CPU. The limiter is keyed on the
	// IP portion of RemoteAddr (port stripped) so NAT'd clients sharing
	// an outbound IP share a bucket — by design, since they're a single
	// blast-radius unit from our perspective.
	if s.cliTokenLimiter != nil && !s.cliTokenLimiter.Allow(clientIPFromRemoteAddr(r.RemoteAddr)) {
		writeError(w, http.StatusTooManyRequests, "rate limit exceeded")
		return
	}

	// Cap the request body before decoding. 4 KiB is generous for an
	// email+password pair; anything larger is either malformed or a
	// slow-read / resource-exhaustion attempt. Rate-limiting fires first
	// (above), so the attacker's per-IP budget is already bounded — this
	// cap is a cheap second layer (CWE-400). Matches the pattern in
	// pipeline_handlers.go.
	r.Body = http.MaxBytesReader(w, r.Body, 4096)

	var req cliTokenRequest
	if err := readJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if req.Email == "" || req.Password == "" {
		writeError(w, http.StatusBadRequest, "email and password required")
		return
	}

	user, err := store.GetUserByEmail(r.Context(), s.db, req.Email)
	if err != nil && !errors.Is(err, store.ErrUserNotFound) {
		// Real DB/backend failure — surface to the operator. Never log
		// req.Password or req.Email here; the standard library's request
		// logger has already noted the URL + status.
		writeError(w, http.StatusInternalServerError, "authentication temporarily unavailable")
		return
	}

	// Constant-ish-time password verify regardless of whether the email
	// exists. dummyPasswordHash is initialised at package-init from
	// random bytes; VerifyPassword runs the same expensive argon2 path
	// against it as it would against a real user's hash. Without this,
	// a timing side channel leaks "is this email registered?".
	stored := dummyPasswordHash
	if user != nil {
		stored = user.PasswordHash
	}
	ok, verr := auth.VerifyPassword(req.Password, stored)
	invalid := user == nil || user.DisabledAt != nil || verr != nil || !ok
	if invalid {
		writeError(w, http.StatusUnauthorized, "invalid credentials")
		return
	}

	// Bind the verified user to ctx so MintLocalCLIToken can derive the
	// `actor`/`sub` claims from auth.UserFromContext — same audit-forgery
	// guard MintRunToken / MintImpersonation use. We do NOT pass the
	// user-id as an argument: the helper's contract is "actor comes from
	// ctx, never the caller", and we honour it here.
	ctx := auth.WithCtxUser(r.Context(), user)

	ttl := cliTokenLifetime()
	tok, exp, err := tokens.MintLocalCLIToken(ctx, s.signer, ttl)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "could not mint token")
		return
	}

	apiTok, apiExp, err := tokens.MintCLIAPIToken(ctx, s.signer, ttl)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "could not mint api token")
		return
	}

	// Pull the user's projects so the CLI knows which lakekeeper project
	// to forward as `x-project-id` on every catalog/STS call. Without
	// this, the CLI defaults to the all-zeros
	// lakekeeper default project where the user has no FGA grants —
	// requests fail with ProjectActionForbidden. Soft-fail: an empty
	// projects list (e.g. user hasn't been granted on any project yet)
	// still returns 200; the CLI surfaces a clear error at run time
	// rather than blocking login here.
	var projectsOut []cliTokenProject
	if s.projects != nil {
		ps, perr := s.projects.ListForUser(r.Context(), user.ID)
		if perr == nil {
			projectsOut = make([]cliTokenProject, 0, len(ps))
			for _, p := range ps {
				projectsOut = append(projectsOut, cliTokenProject{
					ID:                  p.ID.String(),
					Name:                p.Name,
					LakekeeperProjectID: p.LakekeeperProjectID,
				})
			}
		}
	}

	// Use the exp returned by MintLocalCLIToken verbatim — it is the exact
	// value embedded in the JWT's exp claim. Recomputing from time.Now()
	// here would introduce a sub-ms drift that confuses CLI clients
	// comparing ExpiresAt with the JWT's exp.
	writeJSON(w, http.StatusOK, cliTokenResponse{
		Token:     tok,
		ExpiresAt: exp.Format(time.RFC3339),
		UserID:    user.ID.String(),
		Cluster: cliTokenCluster{
			LakekeeperURL: s.cliLakekeeperURL,
			WarehouseName: s.cliWarehouseName,
		},
		Projects:     projectsOut,
		APIToken:     apiTok,
		APIExpiresAt: apiExp.Format(time.RFC3339),
	})
}
