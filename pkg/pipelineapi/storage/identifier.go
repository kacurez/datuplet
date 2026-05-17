// Package storage exposes warehouse table metadata + preview endpoints
// for the Datuplet UI, using iceberg-go for all Iceberg semantics.
package storage

import "regexp"

// identifierRe matches a single namespace or table segment.
// Anchored start-to-end; first char must be alphanumeric; 1..128 chars
// total; subsequent chars may be alphanumeric, underscore, dot, hyphen.
//
// Deliberately rejects: empty string, path separators ('/', '\'),
// parent-dir traversal ('..' standalone is rejected because the first
// char can't be '.'), percent-encoded bytes, control chars (NUL etc.),
// leading underscore/dot/hyphen, anything > 128 chars.
var identifierRe = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$`)

// ValidIdentifier reports whether s is a safe namespace or table segment
// for use in URL path values that resolve to warehouse subdirectories.
func ValidIdentifier(s string) bool {
	return identifierRe.MatchString(s)
}
