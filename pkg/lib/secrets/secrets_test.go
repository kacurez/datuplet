package secrets

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestFileProviderGetReadsFile(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "db_password"), []byte("hunter2\n"), 0o400); err != nil {
		t.Fatalf("write: %v", err)
	}

	p := NewFileProvider(dir)
	got, err := p.Get("db_password")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got != "hunter2" {
		t.Errorf("got %q, want %q (trailing LF must be stripped)", got, "hunter2")
	}
}

func TestFileProviderGetStripsCRLF(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "api"), []byte("token\r\n"), 0o400); err != nil {
		t.Fatalf("write: %v", err)
	}
	got, err := NewFileProvider(dir).Get("api")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got != "token" {
		t.Errorf("got %q, want %q", got, "token")
	}
}

func TestFileProviderGetEmptyFileAllowed(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "maybe_empty"), []byte(""), 0o400); err != nil {
		t.Fatalf("write: %v", err)
	}
	got, err := NewFileProvider(dir).Get("maybe_empty")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got != "" {
		t.Errorf("got %q, want empty", got)
	}
}

func TestFileProviderGetMissingReturnsErrNotFound(t *testing.T) {
	p := NewFileProvider(t.TempDir())
	_, err := p.Get("nope")
	if err == nil {
		t.Fatal("expected error")
	}
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("got %v, want ErrNotFound", err)
	}
}

func TestFileProviderGetStripsOnlyOneTrailingNewline(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "double"), []byte("val\n\n"), 0o400); err != nil {
		t.Fatalf("write: %v", err)
	}
	got, err := NewFileProvider(dir).Get("double")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got != "val\n" {
		t.Errorf("got %q, want %q (only one trailing newline stripped)", got, "val\n")
	}
}

func TestFileProviderGetLeavesBareCarriageReturn(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "mac"), []byte("val\r"), 0o400); err != nil {
		t.Fatalf("write: %v", err)
	}
	got, err := NewFileProvider(dir).Get("mac")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got != "val\r" {
		t.Errorf("got %q, want %q (bare CR must not be stripped)", got, "val\r")
	}
}

func TestValidateAcceptsValidRefs(t *testing.T) {
	tree := map[string]any{
		"credentials": map[string]any{
			"user":     "alice",
			"password": "$[db_password]",
		},
		"token": "$[api_token]",
		"count": 42,
		"flag":  true,
		"maybe": nil,
	}
	refs, err := Validate(tree)
	if err != nil {
		t.Fatalf("Validate: %v", err)
	}
	want := map[string]bool{"db_password": true, "api_token": true}
	if len(refs) != len(want) {
		t.Fatalf("got %v, want %v", refs, want)
	}
	for _, r := range refs {
		if !want[r] {
			t.Errorf("unexpected ref %q", r)
		}
	}
}

func TestValidateAcceptsEscape(t *testing.T) {
	tree := map[string]any{"literal": "$$[not_a_secret]"}
	refs, err := Validate(tree)
	if err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if len(refs) != 0 {
		t.Errorf("escape should not count as a ref, got %v", refs)
	}
}

func TestValidateRejectsMidStringRef(t *testing.T) {
	tree := map[string]any{"url": "postgres://user:$[pass]@host"}
	_, err := Validate(tree)
	if err == nil {
		t.Fatal("expected error for mid-string ref")
	}
}

func TestValidateRejectsMultipleRefs(t *testing.T) {
	tree := map[string]any{"both": "$[a] $[b]"}
	_, err := Validate(tree)
	if err == nil {
		t.Fatal("expected error for multi-ref scalar")
	}
}

func TestValidateRejectsIllegalName(t *testing.T) {
	tree := map[string]any{"bad": "$[not valid!]"}
	_, err := Validate(tree)
	if err == nil {
		t.Fatal("expected error for illegal chars in name")
	}
}

func TestValidateRejectsEmptyName(t *testing.T) {
	tree := map[string]any{"empty": "$[]"}
	_, err := Validate(tree)
	if err == nil {
		t.Fatal("expected error for empty ref name")
	}
}

func TestValidateWalksListsAndNestedMaps(t *testing.T) {
	tree := map[string]any{
		"list": []any{
			map[string]any{"k": "$[one]"},
			"$[two]",
			"plain",
		},
	}
	refs, err := Validate(tree)
	if err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if len(refs) != 2 {
		t.Errorf("got %v, want 2 refs", refs)
	}
}

func TestValidateRejectsIllegalEscapeName(t *testing.T) {
	tree := map[string]any{"x": "$$[not valid!]"}
	_, err := Validate(tree)
	if err == nil {
		t.Fatal("expected error — escape form must also enforce name charset")
	}
}

func TestValidateWalksMapAnyAny(t *testing.T) {
	// Some YAML decoders produce map[any]any. Validate must walk it.
	tree := map[any]any{
		"config": map[any]any{
			"password": "$[db_password]",
		},
	}
	refs, err := Validate(tree)
	if err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if len(refs) != 1 || refs[0] != "db_password" {
		t.Errorf("got %v, want [db_password]", refs)
	}
}

func TestValidateIncludesPathInError(t *testing.T) {
	tree := map[string]any{
		"stages": []any{
			map[string]any{"components": []any{
				map[string]any{"config": map[string]any{"password": "bad $[x]"}},
			}},
		},
	}
	_, err := Validate(tree)
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "stages[0].components[0].config.password") {
		t.Errorf("error does not contain path: %v", err)
	}
}

type mapProvider map[string]string

func (m mapProvider) Get(name string) (string, error) {
	v, ok := m[name]
	if !ok {
		return "", ErrNotFound
	}
	return v, nil
}

func TestResolveReplacesRefsInTree(t *testing.T) {
	tree := map[string]any{
		"credentials": map[string]any{
			"user":     "alice",
			"password": "$[db_password]",
		},
		"token":  "$[api_token]",
		"number": 42,
	}
	p := mapProvider{"db_password": "hunter2", "api_token": "abc-123"}

	resolved, refs, err := Resolve(tree, p)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	m := resolved.(map[string]any)
	creds := m["credentials"].(map[string]any)
	if creds["password"] != "hunter2" {
		t.Errorf("password got %v, want hunter2", creds["password"])
	}
	if m["token"] != "abc-123" {
		t.Errorf("token got %v, want abc-123", m["token"])
	}
	if m["number"] != 42 {
		t.Errorf("number got %v, want 42 (untouched)", m["number"])
	}
	if len(refs) != 2 {
		t.Errorf("refs got %v, want 2", refs)
	}
}

func TestResolveCollapsesEscapeToLiteral(t *testing.T) {
	tree := map[string]any{"literal": "$$[keep_me]"}
	resolved, refs, err := Resolve(tree, mapProvider{})
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	m := resolved.(map[string]any)
	if m["literal"] != "$[keep_me]" {
		t.Errorf("escape not collapsed: got %v", m["literal"])
	}
	if len(refs) != 0 {
		t.Errorf("escape should not count as resolved ref, got %v", refs)
	}
}

func TestResolveReturnsErrNotFoundWithPath(t *testing.T) {
	tree := map[string]any{"a": map[string]any{"b": "$[missing]"}}
	_, _, err := Resolve(tree, mapProvider{})
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "missing") {
		t.Errorf("error missing secret name: %v", err)
	}
	if !strings.Contains(err.Error(), "a.b") {
		t.Errorf("error missing path: %v", err)
	}
}

func TestResolveIsNotRecursiveOnOutput(t *testing.T) {
	// A secret value that itself contains $[x] must stay literal.
	tree := map[string]any{"val": "$[recursive]"}
	p := mapProvider{"recursive": "$[another]"}
	resolved, _, err := Resolve(tree, p)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	m := resolved.(map[string]any)
	if m["val"] != "$[another]" {
		t.Errorf("got %v, want literal '$[another]' (no re-resolution)", m["val"])
	}
}

func TestResolveWalksMapAnyAny(t *testing.T) {
	tree := map[any]any{
		"config": map[any]any{
			"password": "$[db_password]",
		},
	}
	p := mapProvider{"db_password": "hunter2"}
	resolved, refs, err := Resolve(tree, p)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	cfg := resolved.(map[any]any)["config"].(map[any]any)
	if cfg["password"] != "hunter2" {
		t.Errorf("got %v, want hunter2", cfg["password"])
	}
	if len(refs) != 1 || refs[0] != "db_password" {
		t.Errorf("refs got %v, want [db_password]", refs)
	}
}

func TestResolveRootListErrorPath(t *testing.T) {
	tree := []any{"plain", "$[missing]"}
	_, _, err := Resolve(tree, mapProvider{})
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "[1]") {
		t.Errorf("error path missing index marker [1]: %v", err)
	}
}

func TestResolveWalksListsAndNonStringsPassThrough(t *testing.T) {
	tree := map[string]any{
		"list": []any{"$[x]", 1, true, nil, "plain"},
	}
	p := mapProvider{"x": "X"}
	resolved, refs, err := Resolve(tree, p)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	list := resolved.(map[string]any)["list"].([]any)
	if list[0] != "X" || list[1] != 1 || list[2] != true || list[3] != nil || list[4] != "plain" {
		t.Errorf("list walk mismatch: %v", list)
	}
	if len(refs) != 1 || refs[0] != "x" {
		t.Errorf("refs got %v, want [x]", refs)
	}
}
