// Package catalogwriter is a fake stub used only by the notokenlog
// analyzer's testdata. The real package lives at
// github.com/datuplet/datuplet/pkg/catalogwriter. The analyzer matches
// seed types by fully-qualified name, so the testdata import path must
// match the real one.
package catalogwriter

// CredsType mirrors the real type (string alias).
type CredsType string

// GCSCreds mirrors the shape of the real catalogwriter.GCSCreds. The
// notokenlog analyzer flags any call that passes a value (or pointer)
// of this type to a formatter / logger.
type GCSCreds struct {
	OAuthToken   string
	GCPProjectID string
}

// Type returns the credential family. Safe to format.
func (c GCSCreds) Type() CredsType { return "gcs" }
