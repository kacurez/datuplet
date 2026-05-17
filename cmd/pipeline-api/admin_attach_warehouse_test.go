package main

import (
	"context"
	"testing"
)

// TestAttachWarehouse_BadArgs verifies that calling attach-warehouse with no
// arguments returns an error (both required flags missing).
func TestAttachWarehouse_BadArgs(t *testing.T) {
	err := adminAttachWarehouse(context.Background(), nil, []string{})
	if err == nil {
		t.Fatal("expected error for missing args")
	}
}

// TestAttachWarehouse_MissingProject verifies that --warehouse alone errors
// out because --project is required.
func TestAttachWarehouse_MissingProject(t *testing.T) {
	err := adminAttachWarehouse(context.Background(), nil, []string{"--warehouse", "x"})
	if err == nil {
		t.Fatal("expected error for missing --project")
	}
}

// TestAttachWarehouse_MissingWarehouse verifies that --project alone errors
// out because --warehouse is required.
func TestAttachWarehouse_MissingWarehouse(t *testing.T) {
	err := adminAttachWarehouse(context.Background(), nil, []string{"--project", "myproj"})
	if err == nil {
		t.Fatal("expected error for missing --warehouse")
	}
}
