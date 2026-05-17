package storage

import (
	"context"
	"testing"

	"github.com/datuplet/datuplet/pkg/pipelineapi/tokens"
)

// TestNewForLakekeeper: constructor returns a usable Service when the
// URL is provided; returns (nil, nil) when it's blank (soft-degrade
// signal so the server registers a 503 route instead of failing boot).
// Warehouse name is no longer a constructor parameter — resolved per-
// request via WarehouseResolver.
func TestNewForLakekeeper(t *testing.T) {
	t.Run("URLSet", func(t *testing.T) {
		svc, err := NewForLakekeeper("http://lakekeeper:8181/catalog")
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if svc == nil {
			t.Fatal("expected non-nil Service")
		}
		if svc.LakekeeperURL != "http://lakekeeper:8181/catalog" {
			t.Errorf("LakekeeperURL = %q", svc.LakekeeperURL)
		}
		// No S3 creds held on the service in lakekeeper mode.
		if svc.S3Props != nil {
			t.Errorf("S3Props must be nil in lakekeeper mode, got %v", svc.S3Props)
		}
	})

	t.Run("EmptyURL", func(t *testing.T) {
		svc, err := NewForLakekeeper("")
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if svc != nil {
			t.Fatalf("expected nil service for empty URL, got %+v", svc)
		}
	})
}

// TestWithLakekeeper: WithLakekeeper wires the minter, projectIDFor, and
// warehouseResolver fields without overwriting unrelated Service fields.
func TestWithLakekeeper(t *testing.T) {
	svc, err := NewForLakekeeper("http://lakekeeper:8181/catalog")
	if err != nil || svc == nil {
		t.Fatalf("NewForLakekeeper: %v", err)
	}
	minterCalled := false
	minter := func(_ context.Context) (tokens.ImpersonationToken, error) {
		minterCalled = true
		return "tok", nil
	}
	warehouseResolverCalled := false
	warehouseResolver := func(_ context.Context, _ string) (string, error) {
		warehouseResolverCalled = true
		return "datuplet", nil
	}

	svc.WithLakekeeper("http://lakekeeper:8181/catalog", minter, nil, warehouseResolver)

	if svc.Minter == nil {
		t.Fatal("Minter not wired by WithLakekeeper")
	}
	if _, err := svc.Minter(context.Background()); err != nil {
		t.Errorf("Minter returned error: %v", err)
	}
	if !minterCalled {
		t.Error("Minter was not called")
	}
	if svc.WarehouseResolver == nil {
		t.Fatal("WarehouseResolver not wired by WithLakekeeper")
	}
	if _, err := svc.WarehouseResolver(context.Background(), "any-lk-proj-id"); err != nil {
		t.Errorf("WarehouseResolver returned error: %v", err)
	}
	if !warehouseResolverCalled {
		t.Error("WarehouseResolver was not called")
	}
}

// TestNewFromEnv: filesystem mode still works (used by tests and
// local-mode pipelines). S3 type is explicitly rejected with a clear
// error pointing to NewForLakekeeper.
func TestNewFromEnv(t *testing.T) {
	t.Run("EmptyEnv", func(t *testing.T) {
		t.Setenv("DATUPLET_STORAGE_TYPE", "")
		t.Setenv("DATUPLET_STORAGE_ROOT", "")
		svc, err := NewFromEnv()
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if svc != nil {
			t.Fatalf("expected nil service, got %+v", svc)
		}
	})

	t.Run("Filesystem", func(t *testing.T) {
		t.Setenv("DATUPLET_STORAGE_TYPE", "filesystem")
		t.Setenv("DATUPLET_STORAGE_ROOT", "/tmp/warehouse")
		t.Setenv("DATUPLET_ORG", "")
		svc, err := NewFromEnv()
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if svc == nil {
			t.Fatal("expected non-nil service")
		}
		if got, want := svc.WarehouseURI, "file:///tmp/warehouse"; got != want {
			t.Errorf("WarehouseURI = %q, want %q", got, want)
		}
		if !svc.AllowLocal {
			t.Error("AllowLocal should be true for filesystem")
		}
		if svc.S3Props != nil {
			t.Errorf("S3Props should be nil for filesystem, got %v", svc.S3Props)
		}
		if svc.OrgName != "myorg" {
			t.Errorf("OrgName default = %q, want %q", svc.OrgName, "myorg")
		}
	})

	t.Run("FilesystemRelativeRootRejected", func(t *testing.T) {
		t.Setenv("DATUPLET_STORAGE_TYPE", "filesystem")
		t.Setenv("DATUPLET_STORAGE_ROOT", "relative/path")
		if _, err := NewFromEnv(); err == nil {
			t.Fatal("expected error for relative root, got nil")
		}
	})

	// S3 type via env vars is no longer supported — production uses
	// NewForLakekeeper. Verify a clear error is returned.
	t.Run("S3TypeRejected", func(t *testing.T) {
		t.Setenv("DATUPLET_STORAGE_TYPE", "s3")
		t.Setenv("DATUPLET_STORAGE_ROOT", "datuplet-bucket")
		_, err := NewFromEnv()
		if err == nil {
			t.Fatal("expected error for s3 type via env, got nil")
		}
	})

	t.Run("UnsupportedType", func(t *testing.T) {
		t.Setenv("DATUPLET_STORAGE_TYPE", "gcs")
		t.Setenv("DATUPLET_STORAGE_ROOT", "bucket")
		if _, err := NewFromEnv(); err == nil {
			t.Fatal("expected error for unsupported storage type, got nil")
		}
	})
}
