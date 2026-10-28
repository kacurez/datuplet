package storage

import (
	"os"
	"path/filepath"
	"testing"
)

func TestContainedUnder(t *testing.T) {
	cases := []struct {
		root, path string
		want       bool
	}{
		{"/warehouse/proj/a", "/warehouse/proj/a/public/t/metadata", true},
		{"/warehouse/proj/a", "/warehouse/proj/a", true},
		{"/warehouse/proj/a", "/warehouse/proj/b/t", false},
		{"/warehouse/proj/a", "/warehouse/proj/a/../proj/b", false}, // cleaned
		{"/warehouse/proj/a", "/warehouse/proj/aX", false},          // prefix-adjacent
		{"s3://bucket/orgs/o/projects/p", "s3://bucket/orgs/o/projects/p/tables/x", true},
		{"s3://bucket/orgs/o/projects/p", "s3://bucket/orgs/o/projects/q/tables/x", false},

		// URI lexical dot-segment handling (codex P1)
		{"s3://bucket/org/p", "s3://bucket/org/p/../q", false},      // lexical escape via ..
		{"s3://bucket/org/p", "s3://bucket/org/p/./x", true},        // cosmetic ./
		{"s3://bucket/org/p", "s3://bucket/org/p/x/../y", true},     // ../ inside root is fine
		{"s3://bucket/org/p", "s3://bucket/org/p/x/../../q", false}, // two ../ escapes

		// Filesystem root handling (codex P2)
		{"/", "/tmp/x", true},
		{"/", "/", true},
		{"/", "", false}, // empty is not absolute, not contained

		// Empty-root rejection (codex P1 follow-up)
		{"", "/any/absolute/path", false},
		{"", "", false},
		{"", "relative", false},
	}
	for _, tc := range cases {
		if got := ContainedUnder(tc.root, tc.path); got != tc.want {
			t.Errorf("ContainedUnder(%q, %q) = %v, want %v", tc.root, tc.path, got, tc.want)
		}
	}
}

func TestRejectSymlinks(t *testing.T) {
	dir := t.TempDir()
	real := filepath.Join(dir, "real")
	if err := os.Mkdir(real, 0o755); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(dir, "link")
	if err := os.Symlink(real, link); err != nil {
		t.Fatal(err)
	}
	inside := filepath.Join(link, "sub")
	_ = os.Mkdir(inside, 0o755)

	cases := []struct {
		name    string
		path    string
		wantErr bool
	}{
		{"real_dir", real, false},
		{"symlink", link, true},
		// a path whose parent contains a symlink should also be rejected
		{"path_via_symlink", inside, true},
		// Regression: a non-existing leaf whose parent IS a symlink used to
		// pass because EvalSymlinks returned IsNotExist and we
		// short-circuited. Walk up to the deepest existing ancestor and
		// evaluate there instead.
		{"missing_leaf_via_symlink_parent", filepath.Join(link, "unwritten-file"), true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := RejectSymlinks(tc.path)
			if tc.wantErr && err == nil {
				t.Errorf("RejectSymlinks(%s) = nil, want non-nil", tc.name)
			}
			if !tc.wantErr && err != nil {
				t.Errorf("RejectSymlinks(%s) = %v, want nil", tc.name, err)
			}
		})
	}
}
