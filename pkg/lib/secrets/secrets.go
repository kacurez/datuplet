// Package secrets resolves $[name] references in decoded YAML/JSON trees
// by looking up values through a pluggable Provider. Used by the pipeline
// parser (syntax validation) and the DataGateway (resolution at boot).
package secrets

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

// ErrNotFound is returned by Provider.Get when a named secret is unavailable.
var ErrNotFound = errors.New("secrets: secret not found")

// Grammar:
//   ref    = ^\$\[[A-Za-z0-9_-]+\]$  (whole scalar only)
//   escape = ^\$\$\[[A-Za-z0-9_-]+\]$ -> literal "$[name]"
var (
	refPattern    = regexp.MustCompile(`^\$\[([A-Za-z0-9_-]+)\]$`)
	escapePattern = regexp.MustCompile(`^\$\$\[([A-Za-z0-9_-]+)\]$`)
	// anyRefLike catches candidate refs so we can reject bad forms (mid-string,
	// multi-ref, illegal chars) explicitly instead of silently passing them through.
	anyRefLike = regexp.MustCompile(`\$\[`)
)

// Provider reads a secret value by name.
type Provider interface {
	Get(name string) (string, error)
}

// FileProvider reads one file per secret from Dir. Each file's contents are
// the secret value. A single trailing "\n" or "\r\n" is stripped (common for
// hand-written Kubernetes Secrets and editor auto-newlines). Empty files are
// accepted and yield an empty string.
type FileProvider struct {
	Dir string
}

// NewFileProvider creates a FileProvider rooted at dir.
func NewFileProvider(dir string) *FileProvider {
	return &FileProvider{Dir: dir}
}

// Get reads the secret named by name.
func (p *FileProvider) Get(name string) (string, error) {
	path := filepath.Join(p.Dir, name)
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return "", ErrNotFound
		}
		return "", err
	}
	s := string(data)
	// Strip exactly one trailing \r\n or \n — no other whitespace is trimmed.
	// A lone \r without a following \n is left intact.
	if strings.HasSuffix(s, "\r\n") {
		s = s[:len(s)-2]
	} else if strings.HasSuffix(s, "\n") {
		s = s[:len(s)-1]
	}
	return s, nil
}

// Validate walks the tree and enforces the $[name] string-leaf syntax:
//   - whole-scalar refs only (no mid-string, no multiple refs per scalar);
//   - names match [A-Za-z0-9_-]+;
//   - $$[name] is the escape form and passes validation without being a ref.
//
// Returns the sorted list of unique referenced names. Does not call any
// Provider. Non-string scalars (numbers, bools, nil) are ignored.
func Validate(tree any) ([]string, error) {
	refs := map[string]struct{}{}
	if err := walkValidate(tree, "", refs); err != nil {
		return nil, err
	}
	out := make([]string, 0, len(refs))
	for k := range refs {
		out = append(out, k)
	}
	sort.Strings(out)
	return out, nil
}

func walkValidate(node any, path string, refs map[string]struct{}) error {
	switch v := node.(type) {
	case map[string]any:
		for k, child := range v {
			if err := walkValidate(child, joinPathKey(path, k), refs); err != nil {
				return err
			}
		}
	case map[any]any:
		// Some YAML libraries decode maps as map[any]any; coerce key to string.
		for k, child := range v {
			if err := walkValidate(child, joinPathKey(path, fmt.Sprintf("%v", k)), refs); err != nil {
				return err
			}
		}
	case []any:
		for i, child := range v {
			if err := walkValidate(child, fmt.Sprintf("%s[%d]", path, i), refs); err != nil {
				return err
			}
		}
	case string:
		return checkLeafSyntax(v, path, refs)
	default:
		// numbers, bools, nil — nothing to check
	}
	return nil
}

func checkLeafSyntax(s, path string, refs map[string]struct{}) error {
	if m := refPattern.FindStringSubmatch(s); m != nil {
		refs[m[1]] = struct{}{}
		return nil
	}
	if escapePattern.MatchString(s) {
		return nil // escape form, valid, not a ref
	}
	// If the string contains $[ anywhere else, it's a malformed ref.
	if anyRefLike.MatchString(s) {
		return fmt.Errorf("invalid secret reference at %s: only whole-scalar $[name] is supported (got %q)", path, s)
	}
	return nil
}

func joinPathKey(path, key string) string {
	if path == "" {
		return key
	}
	return path + "." + key
}

// Resolve walks the tree in a SINGLE pass:
//   - string leaves matching ^\$\[name\]$        -> replaced with p.Get(name);
//   - string leaves matching ^\$\$\[name\]$      -> rewritten to literal "$[name]";
//   - other string leaves pass through unchanged;
//   - non-string scalars (numbers, bools, nil) pass through untouched;
//   - secret values are NOT re-resolved (no recursion on resolver output).
//
// On error the returned tree may be partially mutated; callers should discard it.
// Returns the sorted list of names actually resolved.
func Resolve(tree any, p Provider) (any, []string, error) {
	resolved := map[string]struct{}{}
	out, err := walkResolve(tree, "", p, resolved)
	if err != nil {
		return nil, nil, err
	}
	names := make([]string, 0, len(resolved))
	for k := range resolved {
		names = append(names, k)
	}
	sort.Strings(names)
	return out, names, nil
}

func walkResolve(node any, path string, p Provider, resolved map[string]struct{}) (any, error) {
	switch v := node.(type) {
	case map[string]any:
		out := make(map[string]any, len(v))
		for k, child := range v {
			newChild, err := walkResolve(child, joinPathKey(path, k), p, resolved)
			if err != nil {
				return nil, err
			}
			out[k] = newChild
		}
		return out, nil
	case map[any]any:
		out := make(map[any]any, len(v))
		for k, child := range v {
			newChild, err := walkResolve(child, joinPathKey(path, fmt.Sprintf("%v", k)), p, resolved)
			if err != nil {
				return nil, err
			}
			out[k] = newChild
		}
		return out, nil
	case []any:
		out := make([]any, len(v))
		for i, child := range v {
			newChild, err := walkResolve(child, fmt.Sprintf("%s[%d]", path, i), p, resolved)
			if err != nil {
				return nil, err
			}
			out[i] = newChild
		}
		return out, nil
	case string:
		return resolveLeaf(v, path, p, resolved)
	default:
		return v, nil
	}
}

func resolveLeaf(s, path string, p Provider, resolved map[string]struct{}) (string, error) {
	if m := refPattern.FindStringSubmatch(s); m != nil {
		name := m[1]
		val, err := p.Get(name)
		if err != nil {
			if errors.Is(err, ErrNotFound) {
				return "", fmt.Errorf("secret %q referenced at %s was not found", name, path)
			}
			return "", fmt.Errorf("reading secret %q at %s: %w", name, path, err)
		}
		resolved[name] = struct{}{}
		return val, nil
	}
	if m := escapePattern.FindStringSubmatch(s); m != nil {
		return "$[" + m[1] + "]", nil
	}
	if anyRefLike.MatchString(s) {
		return "", fmt.Errorf("invalid secret reference at %s: only whole-scalar $[name] is supported (got %q)", path, s)
	}
	return s, nil
}
