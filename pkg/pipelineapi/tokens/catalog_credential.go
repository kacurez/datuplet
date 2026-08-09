package tokens

import "context"

// catalogCredentialCtxKey carries a pre-supplied catalog credential (an
// aud=datuplet-catalog bearer token) through ctx so warehouse resolution can
// present it instead of minting a fresh user-impersonation token.
//
// Motivation (RFC 028 P5): the app-render query route authenticates with the
// app's own app-token and deliberately bypasses auth.WithUser (an app has no
// *store.User row), so ctx carries no user subject. The user-impersonation
// warehouse resolver therefore cannot mint (MintImpersonation → subjectFromCtx
// fails) and every render fails with "no warehouse registered for project".
// The app-token is itself a catalog credential (aud=datuplet-catalog) whose FGA
// identity (viewer on the project) transitively grants can_list_warehouses, so
// the app-query path stashes it here and warehouse resolution uses it directly.
type catalogCredentialCtxKey struct{}

// WithCatalogCredential returns ctx carrying tok as the catalog credential that
// warehouse resolution should present, in place of minting one. tok is a raw
// bearer JWT (aud=datuplet-catalog).
func WithCatalogCredential(ctx context.Context, tok string) context.Context {
	return context.WithValue(ctx, catalogCredentialCtxKey{}, tok)
}

// CatalogCredentialFromCtx returns the catalog credential stashed by
// WithCatalogCredential, or "" if none is present (the common case: every
// non-app path leaves it unset and falls back to a fresh mint).
func CatalogCredentialFromCtx(ctx context.Context) string {
	v, _ := ctx.Value(catalogCredentialCtxKey{}).(string)
	return v
}
