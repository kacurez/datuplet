package main

import (
	"context"
	"strings"
	"testing"
)

// TestAdminMigrate_DryRun verifies the migrate subcommand exits cleanly when
// pointed at a non-existent DB (DATABASE_URL set but unreachable).
// We expect a connection error, not a flag-parse error.
func TestAdminMigrate_DryRun(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://nobody:nobody@127.0.0.1:1/none?sslmode=disable&connect_timeout=1")
	err := adminMigrate(context.Background(), []string{})
	if err == nil {
		t.Fatalf("expected connection error, got nil")
	}
	if !strings.Contains(err.Error(), "connect") && !strings.Contains(err.Error(), "refused") {
		t.Fatalf("expected connect-related error, got: %v", err)
	}
}
