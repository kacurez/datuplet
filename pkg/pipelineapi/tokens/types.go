package tokens

// ImpersonationToken is a redacting wrapper around a short-lived JWT
// minted by MintImpersonation. The fmt-package paths (%s, %v, %#v) all
// hit String() / GoString() and emit "[redacted impersonation token]"
// instead of the raw JWT, so an accidental log line like
// `fmt.Errorf("forwarding %v: %w", token, err)` doesn't leak the
// impersonation grant.
//
// Callers that need the raw JWT (e.g. to set an Authorization header)
// must call Reveal() explicitly. The double-redaction surface
// (String + GoString) makes the leak easier to audit in code review:
// any literal JWT material visible in logs is a Reveal() call gone
// wrong, not an accidental %v.
type ImpersonationToken string

// String redacts. fmt's %s/%v/%q paths all reach this.
func (t ImpersonationToken) String() string { return "[redacted impersonation token]" }

// GoString redacts the %#v path. spew, log/slog with default formatters,
// and fmt %#v all hit this.
func (t ImpersonationToken) GoString() string { return "[redacted impersonation token]" }

// Reveal returns the raw JWT. Use ONLY when constructing the actual
// HTTP Bearer header — the call site is the audit point.
func (t ImpersonationToken) Reveal() string { return string(t) }

// MarshalJSON redacts: encoding/json bypasses Stringer on string-typed
// values, so without this a struct embedding the token would serialize
// the raw JWT.
func (t ImpersonationToken) MarshalJSON() ([]byte, error) {
	return []byte(`"[redacted impersonation token]"`), nil
}

// QueryToken is the redacting wrapper for the RFC 022 ad-hoc-query JWTs
// minted by MintQueryToken (aud=datuplet-catalog, token_kind=query) and
// MintInternalQueryToken (aud=datuplet-query-worker,
// token_kind=internal-query). It follows the identical RFC 019 §4.10
// convention as ImpersonationToken: String() / GoString() redact the
// bearer material so a stray %v / %#v in an error chain doesn't leak the
// grant; callers that set an Authorization header call Reveal() — the
// audit point. A separate type (not ImpersonationToken) keeps the
// impersonation-vs-query semantics distinct in signatures and logs.
type QueryToken string

// String redacts. fmt's %s/%v/%q paths all reach this.
func (t QueryToken) String() string { return "[redacted query token]" }

// GoString redacts the %#v path.
func (t QueryToken) GoString() string { return "[redacted query token]" }

// Reveal returns the raw JWT. Use ONLY when constructing the actual
// HTTP Bearer header — the call site is the audit point.
func (t QueryToken) Reveal() string { return string(t) }

// MarshalJSON redacts: encoding/json bypasses Stringer on string-typed
// values, so without this a struct embedding the token would serialize
// the raw JWT.
func (t QueryToken) MarshalJSON() ([]byte, error) {
	return []byte(`"[redacted query token]"`), nil
}
