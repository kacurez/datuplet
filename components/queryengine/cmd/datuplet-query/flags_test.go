package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestParseFlags_Defaults(t *testing.T) {
	opts, err := parseFlags([]string{"--sql", "SELECT 1"})
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	if opts.SQL != "SELECT 1" {
		t.Errorf("SQL = %q", opts.SQL)
	}
	if opts.Format != "table" {
		t.Errorf("Format default = %q, want table", opts.Format)
	}
	if opts.MemoryLimit == "" {
		t.Errorf("MemoryLimit should have a conservative local default")
	}
}

func TestParseFlags_InvalidFormat(t *testing.T) {
	_, err := parseFlags([]string{"--sql", "SELECT 1", "--format", "xml"})
	if err == nil {
		t.Fatalf("expected error for invalid --format")
	}
}

func TestResolveSQL_Precedence(t *testing.T) {
	dir := t.TempDir()
	fpath := filepath.Join(dir, "q.sql")
	if err := os.WriteFile(fpath, []byte("SELECT from_file"), 0o600); err != nil {
		t.Fatal(err)
	}

	// --sql wins over -f and stdin.
	got, err := resolveSQL(options{SQL: "SELECT inline", File: fpath}, strings.NewReader("SELECT stdin"))
	if err != nil {
		t.Fatal(err)
	}
	if got != "SELECT inline" {
		t.Errorf("got %q, want inline", got)
	}

	// -f used when --sql empty.
	got, err = resolveSQL(options{File: fpath}, strings.NewReader("SELECT stdin"))
	if err != nil {
		t.Fatal(err)
	}
	if got != "SELECT from_file" {
		t.Errorf("got %q, want from_file", got)
	}

	// stdin used when neither --sql nor -f.
	got, err = resolveSQL(options{}, strings.NewReader("SELECT stdin"))
	if err != nil {
		t.Fatal(err)
	}
	if got != "SELECT stdin" {
		t.Errorf("got %q, want stdin", got)
	}

	// Nothing provided → error.
	if _, err := resolveSQL(options{}, strings.NewReader("   ")); err == nil {
		t.Errorf("expected error when no SQL source provided")
	}
}
