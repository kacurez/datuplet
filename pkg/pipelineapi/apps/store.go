// Package apps is the user-apps control plane (RFC 028). This file (store.go)
// is the Postgres-backed data-access layer; handlers.go (author routes),
// internal.go (worker-facing routes) and identity.go's concrete FGA/mint
// wiring land in later tasks (P2-P4). See
// docs/superpowers/specs/2026-07-22-rfc-028-user-apps-wasm-workers-design.md
// §4/§5.1/§5.3/§6.6 and
// .superpowers/sdd/2026-07-23-rfc-028-user-apps-implementation/contract-and-constraints.md
// ("Control plane (Part 2)") for the full contract.
package apps

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Limits from spec §4/§7. MaxBundleBytes and MaxProjectBytes are measured
// against the RAW (uncompressed) bundle size, never the gzip-compressed
// on-disk size.
const (
	// MaxBundleBytes is the per-upload raw-bundle cap (spec §4/§7).
	MaxBundleBytes = 5 * 1024 * 1024
	// MaxProjectBytes is the per-project stored-bundle quota (spec §4).
	MaxProjectBytes = 200 * 1024 * 1024
	// MaxUnreferencedVersions is how many versions not pointed at by any
	// channel are retained per app before GC collects the oldest (spec §4).
	MaxUnreferencedVersions = 20
	// MaxRenderLogsPerApp is the render-log ring-buffer size bound (spec §6.6).
	MaxRenderLogsPerApp = 200
	// DefaultRenderLogRetention is the render-log ring-buffer age bound
	// (spec §6.6). Operator-tunable via WithRenderLogRetention.
	DefaultRenderLogRetention = 14 * 24 * time.Hour
)

// Errors returned by Store methods. Handlers (P2/P3) map these to HTTP
// status codes (e.g. ErrCASMismatch -> 409, ErrNotFound -> 404).
var (
	ErrNotFound       = errors.New("apps: not found")
	ErrAlreadyExists  = errors.New("apps: already exists")
	ErrBundleTooLarge = errors.New("apps: bundle exceeds maximum size")
	ErrProjectQuota   = errors.New("apps: project storage quota exceeded")
	ErrCASMismatch    = errors.New("apps: compare-and-swap mismatch")
)

// App is the in-memory view of an apps row. ID and ProjectID are the string
// (uuid.String()) form throughout this package — see identity.go's
// AppJWTSubject/AppFGASubject, which compose directly on the string form.
type App struct {
	ID            string
	ProjectID     string
	Name          string
	FGARegistered bool
	CreatedAt     time.Time
}

// Version is one immutable, content-addressed app bundle version.
// SizeBytes is always the RAW (uncompressed) size.
type Version struct {
	ID        string
	AppID     string
	Hash      string
	SizeBytes int64
	CreatedAt time.Time
}

// Resolved is the (pid, name, channel) -> version resolution result
// returned by Resolve, and mirrored by the internal resolve endpoint (P3).
type Resolved struct {
	AppID       string
	VersionID   string
	VersionHash string
}

// RenderLogRecord is one row of the per-app render ring buffer (spec §6.6).
// Error is "" when the render succeeded (stored as SQL NULL).
type RenderLogRecord struct {
	RequestID     string
	AppID         string
	VersionHash   string
	Channel       string
	PrincipalKind string
	PrincipalID   string
	StartedAt     time.Time
	DurationMS    int64
	Outcome       string
	LogText       string
	Error         string
}

// Store is the Postgres-backed apps data-access layer. Zero value is not
// usable — construct via NewStore.
type Store struct {
	pool      *pgxpool.Pool
	now       func() time.Time
	retention time.Duration
}

// Option configures a Store at construction time.
type Option func(*Store)

// WithClock overrides the Store's notion of "now", used only for render-log
// retention trimming. Tests inject a fixed/advancing clock to make the
// 14-day (default) retention bound deterministic without sleeping.
func WithClock(now func() time.Time) Option {
	return func(s *Store) { s.now = now }
}

// WithRenderLogRetention overrides the render-log ring-buffer age bound
// (default DefaultRenderLogRetention). Mirrors the operator-tunable
// appWorker.render.* knobs described in spec §6.6/§7.
func WithRenderLogRetention(d time.Duration) Option {
	return func(s *Store) { s.retention = d }
}

// NewStore constructs a Store backed by pool.
func NewStore(pool *pgxpool.Pool, opts ...Option) *Store {
	s := &Store{pool: pool, now: time.Now, retention: DefaultRenderLogRetention}
	for _, o := range opts {
		o(s)
	}
	return s
}

// Create inserts a new app row. name must already be validated by the
// caller (DNS-label rules, spec §4.1) — Create only enforces the
// UNIQUE(project_id, name) constraint.
func (s *Store) Create(ctx context.Context, projectID uuid.UUID, name string) (*App, error) {
	a := &App{ProjectID: projectID.String(), Name: name}
	var id uuid.UUID
	err := s.pool.QueryRow(ctx,
		`INSERT INTO apps (project_id, name) VALUES ($1, $2)
		 RETURNING id, fga_registered, created_at`,
		projectID, name,
	).Scan(&id, &a.FGARegistered, &a.CreatedAt)
	if err != nil {
		if isUniqueViolation(err, "apps_project_id_name_key") {
			return nil, ErrAlreadyExists
		}
		return nil, fmt.Errorf("apps: insert app: %w", err)
	}
	a.ID = id.String()
	return a, nil
}

// PutVersion stores a new bundle version (or returns the existing one,
// idempotently, if this app already has a version with the same content
// hash) and repoints the app's draft channel to it. hash is the hex
// SHA-256 of the RAW bundle; the bytea column stores the bundle
// gzip-compressed. Enforces MaxBundleBytes (raw) and, for genuinely new
// content, the project's MaxProjectBytes stored quota (sum of RAW
// size_bytes across every version of every app in the project).
//
// Concurrency: quota accounting is a classic read-then-write race under
// Postgres's default READ COMMITTED isolation — two concurrent uploads for
// the same project can both read the same pre-insert SUM(size_bytes) and
// both pass the quota check. This is closed by taking a `SELECT ... FOR
// UPDATE` on the owning PROJECT row (not just this app's row — the quota
// is project-wide, spanning every app in it) before computing the sum:
// any other PutVersion for any app in this project blocks here until this
// transaction commits or rolls back, so the sum a transaction observes is
// always up to date with every quota-relevant write that preceded it. The
// version insert also switches to `ON CONFLICT (app_id, hash) DO NOTHING`
// + a re-select on conflict, so that even if this code path is ever
// reached without the project lock serializing it, a lost idempotency
// race returns the existing version instead of a unique-constraint error.
//
// After the insert, version GC drops the oldest versions of this app not
// referenced by any channel beyond MaxUnreferencedVersions.
func (s *Store) PutVersion(ctx context.Context, appID string, bundle []byte) (*Version, error) {
	if len(bundle) > MaxBundleBytes {
		return nil, ErrBundleTooLarge
	}
	appUUID, err := uuid.Parse(appID)
	if err != nil {
		return nil, fmt.Errorf("apps: invalid app id %q: %w", appID, err)
	}

	sum := sha256.Sum256(bundle)
	hash := hex.EncodeToString(sum[:])
	rawSize := int64(len(bundle))

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("apps: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var projectID uuid.UUID
	if err := tx.QueryRow(ctx, `SELECT project_id FROM apps WHERE id = $1`, appUUID).Scan(&projectID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("apps: app %s: %w", appID, ErrNotFound)
		}
		return nil, fmt.Errorf("apps: lookup app: %w", err)
	}

	// Serialize this project's whole quota domain (see doc comment above).
	var lockedProjectID uuid.UUID
	if err := tx.QueryRow(ctx,
		`SELECT id FROM projects WHERE id = $1 FOR UPDATE`, projectID,
	).Scan(&lockedProjectID); err != nil {
		return nil, fmt.Errorf("apps: lock project: %w", err)
	}

	v := &Version{AppID: appID, Hash: hash, SizeBytes: rawSize}
	var versionUUID uuid.UUID
	err = tx.QueryRow(ctx,
		`SELECT id, created_at FROM app_versions WHERE app_id = $1 AND hash = $2`,
		appUUID, hash,
	).Scan(&versionUUID, &v.CreatedAt)
	switch {
	case err == nil:
		// Idempotent: identical content already stored for this app. Fall
		// through to repointing draft — no quota/insert work needed.
	case errors.Is(err, pgx.ErrNoRows):
		var used int64
		if err := tx.QueryRow(ctx,
			`SELECT COALESCE(SUM(av.size_bytes), 0)
			   FROM app_versions av
			   JOIN apps a ON a.id = av.app_id
			  WHERE a.project_id = $1`,
			projectID,
		).Scan(&used); err != nil {
			return nil, fmt.Errorf("apps: quota lookup: %w", err)
		}
		if used+rawSize > MaxProjectBytes {
			return nil, ErrProjectQuota
		}

		var compressed bytes.Buffer
		gz := gzip.NewWriter(&compressed)
		if _, err := gz.Write(bundle); err != nil {
			return nil, fmt.Errorf("apps: gzip write: %w", err)
		}
		if err := gz.Close(); err != nil {
			return nil, fmt.Errorf("apps: gzip close: %w", err)
		}

		err = tx.QueryRow(ctx,
			`INSERT INTO app_versions (app_id, hash, bundle, size_bytes)
			 VALUES ($1, $2, $3, $4)
			 ON CONFLICT (app_id, hash) DO NOTHING
			 RETURNING id, created_at`,
			appUUID, hash, compressed.Bytes(), rawSize,
		).Scan(&versionUUID, &v.CreatedAt)
		if errors.Is(err, pgx.ErrNoRows) {
			// Lost the insert race despite the project lock (e.g. a
			// future caller reusing this branch without holding it) —
			// the row now exists, so re-select it instead of erroring.
			if err := tx.QueryRow(ctx,
				`SELECT id, created_at FROM app_versions WHERE app_id = $1 AND hash = $2`,
				appUUID, hash,
			).Scan(&versionUUID, &v.CreatedAt); err != nil {
				return nil, fmt.Errorf("apps: re-select version after insert conflict: %w", err)
			}
		} else if err != nil {
			return nil, fmt.Errorf("apps: insert version: %w", err)
		}
	default:
		return nil, fmt.Errorf("apps: lookup version: %w", err)
	}
	v.ID = versionUUID.String()

	if _, err := tx.Exec(ctx,
		`INSERT INTO app_channels (app_id, channel, version_id, updated_at)
		 VALUES ($1, 'draft', $2, now())
		 ON CONFLICT (app_id, channel)
		 DO UPDATE SET version_id = EXCLUDED.version_id, updated_at = now()`,
		appUUID, versionUUID,
	); err != nil {
		return nil, fmt.Errorf("apps: update draft channel: %w", err)
	}

	if err := gcVersions(ctx, tx, appUUID); err != nil {
		return nil, fmt.Errorf("apps: version gc: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("apps: commit: %w", err)
	}
	return v, nil
}

// gcVersions deletes this app's versions that are not referenced by any
// channel, keeping the newest MaxUnreferencedVersions of them. A version
// referenced by a channel (production or draft) is never collected,
// regardless of age.
func gcVersions(ctx context.Context, tx pgx.Tx, appID uuid.UUID) error {
	_, err := tx.Exec(ctx, `
		DELETE FROM app_versions
		 WHERE app_id = $1
		   AND id NOT IN (SELECT version_id FROM app_channels WHERE app_id = $1)
		   AND id NOT IN (
		       SELECT id FROM app_versions
		        WHERE app_id = $1
		          AND id NOT IN (SELECT version_id FROM app_channels WHERE app_id = $1)
		        ORDER BY created_at DESC
		        LIMIT $2
		   )`,
		appID, MaxUnreferencedVersions,
	)
	return err
}

// Promote atomically repoints the app's production channel to versionHash,
// a compare-and-swap on the CURRENT production version hash:
// expectedProduction must equal it, or "" if production is not yet set.
// Mismatch returns ErrCASMismatch (callers map this to HTTP 409). An
// unknown versionHash returns an error wrapping ErrNotFound.
//
// Concurrency: the swap itself is the conditional statement — there is no
// separate read-then-compare-then-write sequence for two concurrent
// Promotes to race between. When production is already set, this is a
// single UPDATE whose WHERE clause re-checks "production still points at
// the version whose hash is expectedProduction" as part of the same
// statement; when production is not set yet, it's an
// INSERT ... ON CONFLICT DO NOTHING (succeeds only if no row exists yet).
// Either way, RowsAffected()==0 means the precondition didn't hold at the
// moment of the write, not at some earlier read — that's what makes it an
// atomic CAS under Postgres's default READ COMMITTED isolation: if two
// transactions target the same app_channels row, the loser's UPDATE/INSERT
// blocks on the row lock until the winner commits, then re-evaluates its
// condition against the now-current row (Postgres's standard READ
// COMMITTED re-check for UPDATE/INSERT..ON CONFLICT, sometimes called
// EvalPlanQual) and finds it no longer holds, affecting 0 rows.
func (s *Store) Promote(ctx context.Context, appID, versionHash, expectedProduction string) error {
	appUUID, err := uuid.Parse(appID)
	if err != nil {
		return fmt.Errorf("apps: invalid app id %q: %w", appID, err)
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("apps: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var targetVersion uuid.UUID
	if err := tx.QueryRow(ctx,
		`SELECT id FROM app_versions WHERE app_id = $1 AND hash = $2`,
		appUUID, versionHash,
	).Scan(&targetVersion); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("apps: unknown version hash %q: %w", versionHash, ErrNotFound)
		}
		return fmt.Errorf("apps: lookup target version: %w", err)
	}

	var tag pgconn.CommandTag
	if expectedProduction == "" {
		// First promote: the INSERT succeeds only if no production row
		// exists yet. A concurrent first-promote loser hits the conflict
		// and DOES NOTHING, affecting 0 rows.
		tag, err = tx.Exec(ctx,
			`INSERT INTO app_channels (app_id, channel, version_id, updated_at)
			 VALUES ($1, 'production', $2, now())
			 ON CONFLICT (app_id, channel) DO NOTHING`,
			appUUID, targetVersion,
		)
	} else {
		// Conditional swap: the UPDATE only matches (and thus only
		// affects a row) if production is STILL pointing at the version
		// with hash expectedProduction at the moment this statement
		// actually runs.
		tag, err = tx.Exec(ctx,
			`UPDATE app_channels
			    SET version_id = $2, updated_at = now()
			  WHERE app_id = $1 AND channel = 'production'
			    AND version_id = (SELECT id FROM app_versions WHERE app_id = $1 AND hash = $3)`,
			appUUID, targetVersion, expectedProduction,
		)
	}
	if err != nil {
		return fmt.Errorf("apps: promote: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrCASMismatch
	}
	return tx.Commit(ctx)
}

// Resolve looks up (projectID, name, channel) -> the currently pointed-at
// version. Returns an error wrapping ErrNotFound if the app doesn't exist
// or the channel has no version pointer yet (e.g. production before any
// promote).
func (s *Store) Resolve(ctx context.Context, projectID uuid.UUID, name, channel string) (*Resolved, error) {
	r := &Resolved{}
	var appUUID, versionUUID uuid.UUID
	err := s.pool.QueryRow(ctx,
		`SELECT a.id, v.id, v.hash
		   FROM apps a
		   JOIN app_channels c ON c.app_id = a.id AND c.channel = $3
		   JOIN app_versions v ON v.id = c.version_id
		  WHERE a.project_id = $1 AND a.name = $2`,
		projectID, name, channel,
	).Scan(&appUUID, &versionUUID, &r.VersionHash)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("apps: resolve %s/%s@%s: %w", projectID, name, channel, ErrNotFound)
		}
		return nil, fmt.Errorf("apps: resolve: %w", err)
	}
	r.AppID = appUUID.String()
	r.VersionID = versionUUID.String()
	return r, nil
}

// GetBundle returns the RAW (decompressed) bundle bytes for a content hash.
// hash is globally content-addressed (the internal bundle-fetch endpoint,
// spec §5.2, is keyed by hash alone, with no app scope) — LIMIT 1 covers
// the case where two apps happen to have uploaded byte-identical bundles,
// which produces two rows with the same hash and identical content.
func (s *Store) GetBundle(ctx context.Context, hash string) ([]byte, error) {
	var compressed []byte
	err := s.pool.QueryRow(ctx,
		`SELECT bundle FROM app_versions WHERE hash = $1 LIMIT 1`, hash,
	).Scan(&compressed)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("apps: bundle %q: %w", hash, ErrNotFound)
		}
		return nil, fmt.Errorf("apps: get bundle: %w", err)
	}
	gz, err := gzip.NewReader(bytes.NewReader(compressed))
	if err != nil {
		return nil, fmt.Errorf("apps: gunzip open: %w", err)
	}
	defer gz.Close()
	raw, err := io.ReadAll(gz)
	if err != nil {
		return nil, fmt.Errorf("apps: gunzip read: %w", err)
	}
	return raw, nil
}

// MintToken creates a new viewer token for appID and returns its lookup key
// (tokenID) and the plaintext secret. The plaintext is never stored or
// retrievable again — only SHA-256(salt||secret) is persisted, with a
// fresh random salt per token. Callers (P2's tokens handler) compose the
// external `vw_<tokenID>.<secret>` form.
func (s *Store) MintToken(ctx context.Context, appID string) (tokenID, secret string, err error) {
	appUUID, err := uuid.Parse(appID)
	if err != nil {
		return "", "", fmt.Errorf("apps: invalid app id %q: %w", appID, err)
	}

	secretBytes := make([]byte, 32)
	if _, err := rand.Read(secretBytes); err != nil {
		return "", "", fmt.Errorf("apps: generate secret: %w", err)
	}
	secret = base64.RawURLEncoding.EncodeToString(secretBytes)

	salt := make([]byte, 16)
	if _, err := rand.Read(salt); err != nil {
		return "", "", fmt.Errorf("apps: generate salt: %w", err)
	}

	var id uuid.UUID
	err = s.pool.QueryRow(ctx,
		`INSERT INTO app_viewer_tokens (app_id, salt, secret_hash)
		 VALUES ($1, $2, $3)
		 RETURNING token_id`,
		appUUID, salt, hashToken(salt, secret),
	).Scan(&id)
	if err != nil {
		return "", "", fmt.Errorf("apps: insert token: %w", err)
	}
	return id.String(), secret, nil
}

// VerifyToken reports whether secret is the current, non-revoked secret for
// (appID, tokenID). Returns (false, nil) — not an error — for unknown
// tokens, revoked tokens, and wrong secrets; comparison is constant-time.
func (s *Store) VerifyToken(ctx context.Context, appID, tokenID, secret string) (bool, error) {
	appUUID, err := uuid.Parse(appID)
	if err != nil {
		return false, fmt.Errorf("apps: invalid app id %q: %w", appID, err)
	}
	tokenUUID, err := uuid.Parse(tokenID)
	if err != nil {
		return false, fmt.Errorf("apps: invalid token id %q: %w", tokenID, err)
	}

	var salt, wantHash []byte
	var revokedAt *time.Time
	err = s.pool.QueryRow(ctx,
		`SELECT salt, secret_hash, revoked_at
		   FROM app_viewer_tokens
		  WHERE app_id = $1 AND token_id = $2`,
		appUUID, tokenUUID,
	).Scan(&salt, &wantHash, &revokedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, fmt.Errorf("apps: lookup token: %w", err)
	}
	if revokedAt != nil {
		return false, nil
	}
	return subtle.ConstantTimeCompare(hashToken(salt, secret), wantHash) == 1, nil
}

// TokenActive reports whether (appID, tokenID) names a live, non-revoked
// viewer token — WITHOUT checking a secret. Unknown (appID, tokenID),
// another app's token, and a revoked token all answer (false, nil), the
// same "no error, just no" shape VerifyToken uses.
//
// This exists for the cookie-only revocation recheck (spec §5.3): the
// signed session cookie deliberately carries no secret (the plaintext
// transits exactly once, at the 302 exchange — contract-and-constraints.md's
// Cookie spec), so a cookie-authenticated request has nothing to present to
// VerifyToken. TokenActive answers the narrower question a cookie CAN ask:
// "is this token I already exchanged for still live?" — never "is this
// secret correct?", which is VerifyToken's job and stays gated on the
// secret.
func (s *Store) TokenActive(ctx context.Context, appID, tokenID string) (bool, error) {
	appUUID, err := uuid.Parse(appID)
	if err != nil {
		return false, fmt.Errorf("apps: invalid app id %q: %w", appID, err)
	}
	tokenUUID, err := uuid.Parse(tokenID)
	if err != nil {
		return false, fmt.Errorf("apps: invalid token id %q: %w", tokenID, err)
	}

	var revokedAt *time.Time
	err = s.pool.QueryRow(ctx,
		`SELECT revoked_at
		   FROM app_viewer_tokens
		  WHERE app_id = $1 AND token_id = $2`,
		appUUID, tokenUUID,
	).Scan(&revokedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, fmt.Errorf("apps: lookup token: %w", err)
	}
	return revokedAt == nil, nil
}

// RevokeToken marks a viewer token revoked (idempotent-looking to callers:
// re-revoking an already-revoked token is a no-op, not an error — only a
// wholly unknown (appID, tokenID) returns ErrNotFound). Not part of the P1
// contract's named Store API, but required so VerifyToken's
// false-after-revoke behavior is testable here; P2's DELETE
// .../tokens/{token_id} handler is expected to call this directly.
func (s *Store) RevokeToken(ctx context.Context, appID, tokenID string) error {
	appUUID, err := uuid.Parse(appID)
	if err != nil {
		return fmt.Errorf("apps: invalid app id %q: %w", appID, err)
	}
	tokenUUID, err := uuid.Parse(tokenID)
	if err != nil {
		return fmt.Errorf("apps: invalid token id %q: %w", tokenID, err)
	}
	tag, err := s.pool.Exec(ctx,
		`UPDATE app_viewer_tokens SET revoked_at = now()
		  WHERE app_id = $1 AND token_id = $2`,
		appUUID, tokenUUID,
	)
	if err != nil {
		return fmt.Errorf("apps: revoke token: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("apps: token %s: %w", tokenID, ErrNotFound)
	}
	return nil
}

// AppendRenderLog appends one render record to the app's ring buffer, then
// trims it by BOTH bounds: records older than the Store's retention window
// (default DefaultRenderLogRetention, measured against the injectable
// clock — see WithClock) are dropped, and only the newest
// MaxRenderLogsPerApp records are kept.
func (s *Store) AppendRenderLog(ctx context.Context, rec RenderLogRecord) error {
	appUUID, err := uuid.Parse(rec.AppID)
	if err != nil {
		return fmt.Errorf("apps: invalid app id %q: %w", rec.AppID, err)
	}
	requestUUID, err := uuid.Parse(rec.RequestID)
	if err != nil {
		return fmt.Errorf("apps: invalid request id %q: %w", rec.RequestID, err)
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("apps: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var errVal *string
	if rec.Error != "" {
		errVal = &rec.Error
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO app_render_logs
		    (request_id, app_id, version_hash, channel, principal_kind, principal_id,
		     started_at, duration_ms, outcome, log_text, error)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
		requestUUID, appUUID, rec.VersionHash, rec.Channel, rec.PrincipalKind, rec.PrincipalID,
		rec.StartedAt, rec.DurationMS, rec.Outcome, rec.LogText, errVal,
	); err != nil {
		return fmt.Errorf("apps: insert render log: %w", err)
	}

	cutoff := s.now().Add(-s.retention)
	if _, err := tx.Exec(ctx,
		`DELETE FROM app_render_logs WHERE app_id = $1 AND started_at < $2`,
		appUUID, cutoff,
	); err != nil {
		return fmt.Errorf("apps: trim render logs by age: %w", err)
	}

	if _, err := tx.Exec(ctx, `
		DELETE FROM app_render_logs
		 WHERE app_id = $1
		   AND request_id NOT IN (
		       SELECT request_id FROM app_render_logs
		        WHERE app_id = $1
		        ORDER BY started_at DESC
		        LIMIT $2
		   )`,
		appUUID, MaxRenderLogsPerApp,
	); err != nil {
		return fmt.Errorf("apps: trim render logs by count: %w", err)
	}

	return tx.Commit(ctx)
}

// GetRenderLogs returns render-log records for appID. If requestID is
// non-empty, it returns exactly that one record (as a single-element
// slice), or an error wrapping ErrNotFound if no such record exists
// (never existed, or aged/trimmed out of the ring buffer). If requestID is
// empty, it returns up to limit records ordered newest-first (limit <= 0
// defaults to MaxRenderLogsPerApp).
func (s *Store) GetRenderLogs(ctx context.Context, appID, requestID string, limit int) ([]RenderLogRecord, error) {
	appUUID, err := uuid.Parse(appID)
	if err != nil {
		return nil, fmt.Errorf("apps: invalid app id %q: %w", appID, err)
	}

	if requestID != "" {
		requestUUID, err := uuid.Parse(requestID)
		if err != nil {
			return nil, fmt.Errorf("apps: invalid request id %q: %w", requestID, err)
		}
		rec, err := scanRenderLogRow(s.pool.QueryRow(ctx,
			`SELECT request_id, app_id, version_hash, channel, principal_kind, principal_id,
			        started_at, duration_ms, outcome, log_text, error
			   FROM app_render_logs
			  WHERE app_id = $1 AND request_id = $2`,
			appUUID, requestUUID,
		))
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return nil, fmt.Errorf("apps: render log %s: %w", requestID, ErrNotFound)
			}
			return nil, fmt.Errorf("apps: get render log: %w", err)
		}
		return []RenderLogRecord{*rec}, nil
	}

	if limit <= 0 {
		limit = MaxRenderLogsPerApp
	}
	rows, err := s.pool.Query(ctx,
		`SELECT request_id, app_id, version_hash, channel, principal_kind, principal_id,
		        started_at, duration_ms, outcome, log_text, error
		   FROM app_render_logs
		  WHERE app_id = $1
		  ORDER BY started_at DESC
		  LIMIT $2`,
		appUUID, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("apps: list render logs: %w", err)
	}
	defer rows.Close()

	var out []RenderLogRecord
	for rows.Next() {
		rec, err := scanRenderLogRow(rows)
		if err != nil {
			return nil, fmt.Errorf("apps: scan render log: %w", err)
		}
		out = append(out, *rec)
	}
	return out, rows.Err()
}

// rowScanner is satisfied by both pgx.Row (QueryRow) and pgx.Rows (Query),
// letting scanRenderLogRow serve both GetRenderLogs branches.
type rowScanner interface {
	Scan(dest ...any) error
}

func scanRenderLogRow(row rowScanner) (*RenderLogRecord, error) {
	rec := &RenderLogRecord{}
	var requestUUID, appUUID uuid.UUID
	var errVal *string
	if err := row.Scan(
		&requestUUID, &appUUID, &rec.VersionHash, &rec.Channel, &rec.PrincipalKind,
		&rec.PrincipalID, &rec.StartedAt, &rec.DurationMS, &rec.Outcome, &rec.LogText, &errVal,
	); err != nil {
		return nil, err
	}
	rec.RequestID = requestUUID.String()
	rec.AppID = appUUID.String()
	if errVal != nil {
		rec.Error = *errVal
	}
	return rec, nil
}

// ---------------------------------------------------------------------------
// Author-route queries (RFC 028 P2)
//
// The contract's Store API list (contract-and-constraints.md, "Control plane
// (Part 2)") names the write/resolve path only; the author routes
// (GET/DELETE/list, spec §5.1) additionally need plain metadata reads and a
// row-delete. They live here rather than in handlers.go so all SQL stays in
// the one data-access file.
// ---------------------------------------------------------------------------

// ChannelRef is a channel's current pointer: which version hash it resolves
// to and when it was last repointed.
type ChannelRef struct {
	Channel     string
	VersionHash string
	UpdatedAt   time.Time
}

// AppSummary is an app row plus its channel pointers — the shape the author
// list route serializes (spec §5.1/§5.5: "apps + channel/version status").
type AppSummary struct {
	App
	Channels []ChannelRef
}

// Get looks up one app by (projectID, name). Returns an error wrapping
// ErrNotFound when no such app exists in that project.
func (s *Store) Get(ctx context.Context, projectID uuid.UUID, name string) (*App, error) {
	a := &App{ProjectID: projectID.String(), Name: name}
	var id uuid.UUID
	err := s.pool.QueryRow(ctx,
		`SELECT id, fga_registered, created_at FROM apps WHERE project_id = $1 AND name = $2`,
		projectID, name,
	).Scan(&id, &a.FGARegistered, &a.CreatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("apps: app %s/%s: %w", projectID, name, ErrNotFound)
		}
		return nil, fmt.Errorf("apps: get app: %w", err)
	}
	a.ID = id.String()
	return a, nil
}

// GetByID looks up one app by its id alone, with no project scope. The
// internal API (P3) needs this shape: app-worker names an app_id and the
// project must be derived from the row, never from the caller (spec §5.2 —
// the worker can't name an identity or a project).
func (s *Store) GetByID(ctx context.Context, appID string) (*App, error) {
	appUUID, err := uuid.Parse(appID)
	if err != nil {
		return nil, fmt.Errorf("apps: invalid app id %q: %w", appID, err)
	}
	a := &App{ID: appUUID.String()}
	var projectUUID uuid.UUID
	err = s.pool.QueryRow(ctx,
		`SELECT project_id, name, fga_registered, created_at FROM apps WHERE id = $1`,
		appUUID,
	).Scan(&projectUUID, &a.Name, &a.FGARegistered, &a.CreatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("apps: app %s: %w", appID, ErrNotFound)
		}
		return nil, fmt.Errorf("apps: get app by id: %w", err)
	}
	a.ProjectID = projectUUID.String()
	return a, nil
}

// List returns every app in the project, name-ordered, each with its channel
// pointers. One LEFT-JOINed query (not N+1) — an app with no channels yet
// still appears, with an empty Channels slice.
func (s *Store) List(ctx context.Context, projectID uuid.UUID) ([]AppSummary, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT a.id, a.name, a.fga_registered, a.created_at, c.channel, v.hash, c.updated_at
		   FROM apps a
		   LEFT JOIN app_channels c ON c.app_id = a.id
		   LEFT JOIN app_versions v ON v.id = c.version_id
		  WHERE a.project_id = $1
		  ORDER BY a.name, c.channel`,
		projectID,
	)
	if err != nil {
		return nil, fmt.Errorf("apps: list apps: %w", err)
	}
	defer rows.Close()

	out := []AppSummary{}
	for rows.Next() {
		var id uuid.UUID
		var a App
		var channel, hash *string
		var updatedAt *time.Time
		if err := rows.Scan(&id, &a.Name, &a.FGARegistered, &a.CreatedAt, &channel, &hash, &updatedAt); err != nil {
			return nil, fmt.Errorf("apps: scan app: %w", err)
		}
		a.ID = id.String()
		a.ProjectID = projectID.String()
		if n := len(out); n == 0 || out[n-1].ID != a.ID {
			out = append(out, AppSummary{App: a, Channels: []ChannelRef{}})
		}
		if channel != nil && hash != nil && updatedAt != nil {
			cur := &out[len(out)-1]
			cur.Channels = append(cur.Channels, ChannelRef{
				Channel: *channel, VersionHash: *hash, UpdatedAt: *updatedAt,
			})
		}
	}
	return out, rows.Err()
}

// Channels returns the app's channel pointers (never nil).
func (s *Store) Channels(ctx context.Context, appID string) ([]ChannelRef, error) {
	appUUID, err := uuid.Parse(appID)
	if err != nil {
		return nil, fmt.Errorf("apps: invalid app id %q: %w", appID, err)
	}
	rows, err := s.pool.Query(ctx,
		`SELECT c.channel, v.hash, c.updated_at
		   FROM app_channels c
		   JOIN app_versions v ON v.id = c.version_id
		  WHERE c.app_id = $1
		  ORDER BY c.channel`,
		appUUID,
	)
	if err != nil {
		return nil, fmt.Errorf("apps: list channels: %w", err)
	}
	defer rows.Close()

	out := []ChannelRef{}
	for rows.Next() {
		var c ChannelRef
		if err := rows.Scan(&c.Channel, &c.VersionHash, &c.UpdatedAt); err != nil {
			return nil, fmt.Errorf("apps: scan channel: %w", err)
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// ListVersions returns the app's retained versions, newest-first (never nil).
// limit <= 0 means "every retained version" — version GC already bounds the
// row count per app (MaxUnreferencedVersions plus the channel-referenced ones).
func (s *Store) ListVersions(ctx context.Context, appID string, limit int) ([]Version, error) {
	appUUID, err := uuid.Parse(appID)
	if err != nil {
		return nil, fmt.Errorf("apps: invalid app id %q: %w", appID, err)
	}
	if limit <= 0 {
		limit = MaxUnreferencedVersions + 2 // + the two channel-referenced versions
	}
	rows, err := s.pool.Query(ctx,
		`SELECT id, hash, size_bytes, created_at
		   FROM app_versions
		  WHERE app_id = $1
		  ORDER BY created_at DESC
		  LIMIT $2`,
		appUUID, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("apps: list versions: %w", err)
	}
	defer rows.Close()

	out := []Version{}
	for rows.Next() {
		v := Version{AppID: appID}
		var id uuid.UUID
		if err := rows.Scan(&id, &v.Hash, &v.SizeBytes, &v.CreatedAt); err != nil {
			return nil, fmt.Errorf("apps: scan version: %w", err)
		}
		v.ID = id.String()
		out = append(out, v)
	}
	return out, rows.Err()
}

// Delete removes the app row; versions, channels, viewer tokens and render
// logs go with it via ON DELETE CASCADE. Returns an error wrapping
// ErrNotFound when the app is already gone.
//
// The FGA tuple must already have been removed by
// IdentityManager.Unregister before this is called (spec §5.4 — the
// tuple-then-rows ordering mirrors runbackend.K8sBackend.CancelRun).
func (s *Store) Delete(ctx context.Context, appID string) error {
	appUUID, err := uuid.Parse(appID)
	if err != nil {
		return fmt.Errorf("apps: invalid app id %q: %w", appID, err)
	}
	tag, err := s.pool.Exec(ctx, `DELETE FROM apps WHERE id = $1`, appUUID)
	if err != nil {
		return fmt.Errorf("apps: delete app: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("apps: app %s: %w", appID, ErrNotFound)
	}
	return nil
}

// SetFGARegistered records that the app's `viewer` tuple has been written.
// Separate from Create because the tuple write is an HTTP call to OpenFGA,
// not a Postgres participant: the row is inserted first, the tuple written
// second, and this flag flipped third, so a Register that fails half-way
// leaves fga_registered=false and the next upsert retries it.
func (s *Store) SetFGARegistered(ctx context.Context, appID string) error {
	appUUID, err := uuid.Parse(appID)
	if err != nil {
		return fmt.Errorf("apps: invalid app id %q: %w", appID, err)
	}
	if _, err := s.pool.Exec(ctx,
		`UPDATE apps SET fga_registered = true WHERE id = $1`, appUUID,
	); err != nil {
		return fmt.Errorf("apps: mark fga_registered: %w", err)
	}
	return nil
}

func hashToken(salt []byte, secret string) []byte {
	h := sha256.New()
	h.Write(salt)
	h.Write([]byte(secret))
	return h.Sum(nil)
}

func isUniqueViolation(err error, constraint string) bool {
	return strings.Contains(err.Error(), constraint)
}
