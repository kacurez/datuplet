package authz

import (
	"strings"
	"testing"
)

// TestNamespaceObject confirms the constructor preserves the namespace
// name verbatim (no normalization — lakekeeper does not prefix namespace
// IDs the way it does OIDC subjects).
func TestNamespaceObject(t *testing.T) {
	t.Run("simple name", func(t *testing.T) {
		o := NamespaceObject("raw")
		if got, want := o.String(), "namespace:raw"; got != want {
			t.Errorf("String() = %q, want %q", got, want)
		}
		if got, want := o.Type(), TypeNamespace; got != want {
			t.Errorf("Type() = %q, want %q", got, want)
		}
		if got, want := o.ID(), "raw"; got != want {
			t.Errorf("ID() = %q, want %q", got, want)
		}
	})

	t.Run("dot-separated workspace name", func(t *testing.T) {
		o := NamespaceObject("__workspace.abc123")
		if got, want := o.String(), "namespace:__workspace.abc123"; got != want {
			t.Errorf("String() = %q, want %q", got, want)
		}
		if got, want := o.Type(), TypeNamespace; got != want {
			t.Errorf("Type() = %q, want %q", got, want)
		}
	})

	t.Run("dot-separated multi-segment name", func(t *testing.T) {
		// Namespaces can be hierarchical via dot-separation; confirm the
		// constructor doesn't truncate or split.
		o := NamespaceObject("joined.staging.v2")
		if got, want := o.String(), "namespace:joined.staging.v2"; got != want {
			t.Errorf("String() = %q, want %q", got, want)
		}
	})
}

// TestUserObject verifies both branches of the idempotent oidc~ prepend.
func TestUserObject_OidcPrefix(t *testing.T) {
	t.Run("raw JWT sub gets prefix", func(t *testing.T) {
		o := UserObject("8a1d-foobar")
		want := "user:oidc~8a1d-foobar"
		if got := o.String(); got != want {
			t.Errorf("String() = %q, want %q", got, want)
		}
	})

	t.Run("pre-prefixed sub is not doubled", func(t *testing.T) {
		o := UserObject("oidc~8a1d-foobar")
		want := "user:oidc~8a1d-foobar"
		if got := o.String(); got != want {
			t.Errorf("String() = %q, want %q (prefix was doubled)", got, want)
		}
	})

	t.Run("result always has exactly one oidc~ occurrence in id", func(t *testing.T) {
		o := UserObject("alice")
		id := o.ID()
		if count := strings.Count(id, "oidc~"); count != 1 {
			t.Errorf("expected exactly one 'oidc~' in id %q, got %d", id, count)
		}
	})
}

// TestObject_TypeNotUserControllable confirms that the kind field of a typed
// constructor cannot be overridden by user-supplied input. The Object struct
// fields are unexported — the only way to set them is via the constructors.
func TestObject_TypeNotUserControllable(t *testing.T) {
	// A caller cannot craft, for example, "project:warehouse:evil" as the id
	// of a ProjectObject — the type is always "project" regardless of the id value.
	o := ProjectObject("warehouse:evil")
	if o.Type() != TypeProject {
		t.Errorf("Type() = %q, want %q", o.Type(), TypeProject)
	}
	if o.String() != "project:warehouse:evil" {
		t.Errorf("String() = %q, want %q", o.String(), "project:warehouse:evil")
	}
}

// TestObjectType_Constants ensures the type-name strings match lakekeeper's
// shipped collaboration-4.3 model verbatim (no lakekeeper_ prefix).
func TestObjectType_Constants(t *testing.T) {
	cases := []struct{ name, got, want string }{
		{"project", string(TypeProject), "project"},
		{"warehouse", string(TypeWarehouse), "warehouse"},
		{"namespace", string(TypeNamespace), "namespace"},
		{"table", string(TypeTable), "table"},
		{"view", string(TypeView), "view"},
		{"user", string(TypeUser), "user"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.got != tc.want {
				t.Errorf("Type%s = %q, want %q", tc.name, tc.got, tc.want)
			}
		})
	}
}
