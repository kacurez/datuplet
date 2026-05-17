package storage

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
)

// ContainedUnder reports whether path (after lexical cleanup) is equal
// to root or a descendant of it. Works for both local paths
// ("/warehouse/...") and URI-shaped paths ("s3://bucket/..."). The
// trailing-separator check prevents prefix-adjacency bypasses such as
// "/warehouse/a" matching root "/warehouse/aX".
//
// URI paths have their post-authority portion collapsed with path.Clean
// so dot-segments can't be used to escape the root lexically (e.g.
// "s3://bucket/p/../q" no longer matches root "s3://bucket/p").
func ContainedUnder(root, path string) bool {
	if root == "" {
		// An empty root would otherwise match any absolute path via the
		// HasPrefix(cleanPath, sep) branch below. Reject explicitly.
		return false
	}
	cleanRoot := normalize(root)
	if cleanRoot == "" {
		// Defensive: normalize can surface empty strings for pathological
		// inputs; treat them the same as the raw empty-root case.
		return false
	}
	cleanPath := normalize(path)
	if cleanPath == "" {
		// Empty input is not a valid absolute path/URI; nothing is contained.
		return false
	}
	if cleanPath == cleanRoot {
		return true
	}
	sep := "/"
	if !hasScheme(cleanRoot) {
		sep = string(filepath.Separator)
	}
	// Root-of-filesystem special case: every absolute path starts with
	// the separator and is therefore contained under it. The generic
	// "cleanRoot + sep" test would otherwise produce a spurious "//" that
	// never matches a normal absolute path.
	if cleanRoot == sep {
		return strings.HasPrefix(cleanPath, sep)
	}
	return strings.HasPrefix(cleanPath, cleanRoot+sep)
}

// normalize collapses dot-segments in both local paths and URIs.
// For local paths it just calls filepath.Clean. For URIs it splits off
// the "scheme://authority" prefix and applies stdlib path.Clean to the
// remainder so filepath.Clean can't corrupt the "://" double-slash.
func normalize(p string) string {
	if p == "" {
		return ""
	}
	if hasScheme(p) {
		return normalizeURI(p)
	}
	return filepath.Clean(p)
}

// normalizeURI cleans the path portion of a "scheme://authority/rest"
// URI using stdlib path.Clean (URIs always use "/"). The scheme and
// authority are preserved verbatim.
//
// If the caller passes a path that escapes the authority (e.g.
// "s3://bucket/../x"), path.Clean surfaces the leading ".." and we
// return the string unchanged — it won't match any legitimate cleaned
// root, so ContainedUnder correctly returns false.
func normalizeURI(uri string) string {
	schemeEnd := strings.Index(uri, "://")
	if schemeEnd < 0 {
		return uri
	}
	prefix := uri[:schemeEnd+3] // "scheme://"
	rest := uri[schemeEnd+3:]
	slash := strings.Index(rest, "/")
	if slash < 0 {
		// No path portion, just "scheme://authority".
		return uri
	}
	authority := rest[:slash]
	raw := rest[slash:] // starts with "/"
	cleaned := path.Clean(raw)
	// path.Clean may produce a leading ".." sequence when raw escapes
	// above the authority root. In that case return the input verbatim
	// so the caller sees a non-canonical string that won't match anything.
	if strings.HasPrefix(cleaned, "..") {
		return uri
	}
	return prefix + authority + cleaned
}

// hasScheme reports whether p begins with "<scheme>://" for a plausibly
// short scheme (up to 7 chars). Deliberately conservative; all our
// real schemes (s3, file, gs, abfs) fit.
func hasScheme(p string) bool {
	i := strings.Index(p, "://")
	return i > 0 && i < 8
}

// RejectSymlinks returns a non-nil error if path or any of its existing
// ancestors contain a symlink that causes the user-visible path segments
// to diverge from the filesystem-canonical ones. A non-existing leaf is
// acceptable — the caller may be probing a path that hasn't been written
// yet; only existing ancestors matter.
//
// Implementation: compare the cleaned input path to filepath.EvalSymlinks
// aligned from the tail. If any tail-aligned segment differs, a symlink
// has rewritten a user-visible segment and we reject. This is resilient
// to OS-level symlinks that only add/remove head segments (e.g. macOS's
// /var -> /private/var) because those cause head-side length mismatch
// only, never tail-aligned segment mismatch.
//
// If the leaf (or deeper ancestors) don't exist yet, we walk up to the
// deepest existing ancestor, resolve it, and tail-align the comparison
// against "resolved-ancestor + missing-suffix". This catches adversarial
// probes where an attacker picks a non-existent leaf whose parent is a
// symlink.
//
// Intended for local warehouses (file://) only — s3:// has no symlinks.
func RejectSymlinks(p string) error {
	if !filepath.IsAbs(p) {
		return fmt.Errorf("RejectSymlinks: %q is not absolute", p)
	}
	cleaned := filepath.Clean(p)
	resolved, err := filepath.EvalSymlinks(cleaned)
	if err == nil {
		return compareTailAligned(cleaned, resolved)
	}
	if !os.IsNotExist(err) {
		return fmt.Errorf("evalsymlinks %q: %w", cleaned, err)
	}
	// Leaf (or some ancestor) doesn't exist. Walk up until we find the
	// deepest existing ancestor, resolve THAT, and rebuild the full
	// canonical path as "resolved-ancestor + remaining-suffix". Then
	// tail-align against the input. This catches symlinks along the
	// existing prefix even when the leaf hasn't been written yet.
	ancestor := filepath.Dir(cleaned)
	for {
		if ancestor == "" || ancestor == string(filepath.Separator) || ancestor == "." {
			// Nothing exists along the way at all — no symlinks possible.
			return nil
		}
		resolvedAncestor, aerr := filepath.EvalSymlinks(ancestor)
		if aerr == nil {
			missingSuffix := strings.TrimPrefix(cleaned, ancestor)
			reconstructed := resolvedAncestor + missingSuffix
			return compareTailAligned(cleaned, reconstructed)
		}
		if !os.IsNotExist(aerr) {
			return fmt.Errorf("evalsymlinks %q: %w", ancestor, aerr)
		}
		parent := filepath.Dir(ancestor)
		if parent == ancestor {
			return nil
		}
		ancestor = parent
	}
}

// compareTailAligned rejects when user-visible tail segments of cleaned
// diverge from the filesystem-canonical tail segments of resolved.
func compareTailAligned(cleaned, resolved string) error {
	cleanedParts := splitPath(cleaned)
	resolvedParts := splitPath(resolved)
	ci, ri := len(cleanedParts)-1, len(resolvedParts)-1
	for ci >= 0 && ri >= 0 {
		if cleanedParts[ci] != resolvedParts[ri] {
			return fmt.Errorf("path traverses symlink: segment %q resolves to %q", cleanedParts[ci], resolvedParts[ri])
		}
		ci--
		ri--
	}
	return nil
}

// splitPath returns the path's non-empty segments, most significant first.
func splitPath(p string) []string {
	var out []string
	for {
		d, b := filepath.Split(p)
		if b != "" {
			out = append([]string{b}, out...)
		}
		cleaned := filepath.Clean(d)
		if cleaned == p || cleaned == "/" || cleaned == "." {
			break
		}
		p = cleaned
	}
	return out
}
