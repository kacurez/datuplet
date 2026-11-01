//go:build duckdb_arrow

package main

import (
	"bytes"
	"strings"
	"testing"
)

func noEnv(string) string { return "" }

// The operator's sidecar args parse, including the accepted-but-ignored
// --minio; flags win over the env fallbacks.
func TestParseFlagsOperatorArgs(t *testing.T) {
	var stderr bytes.Buffer
	opts, err := parseFlags([]string{
		"--minio",
		"--config", "/etc/datuplet/gateway.yaml",
		"--addr", ":6000",
		"--run-token-path", "/var/run/secrets/datuplet-runtoken/token",
		"--pod-annotations-path", "/etc/podinfo/annotations",
	}, func(k string) string { return "/from-env/" + k }, &stderr)
	if err != nil {
		t.Fatalf("parseFlags: %v (stderr %q)", err, stderr.String())
	}
	want := options{
		configPath:         "/etc/datuplet/gateway.yaml",
		addr:               ":6000",
		runTokenPath:       "/var/run/secrets/datuplet-runtoken/token",
		podAnnotationsPath: "/etc/podinfo/annotations",
		mode:               "minio",
	}
	if opts != want {
		t.Errorf("opts = %+v, want %+v", opts, want)
	}
}

func TestParseFlagsDefaultsAndEnv(t *testing.T) {
	env := map[string]string{"RUN_TOKEN_PATH": "/env/token", "POD_ANNOTATIONS_PATH": "/env/annotations"}
	opts, err := parseFlags([]string{"--local", "--config", "c.yaml"}, func(k string) string { return env[k] }, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	if opts.addr != ":50051" || opts.runTokenPath != "/env/token" || opts.podAnnotationsPath != "/env/annotations" || opts.mode != "local" {
		t.Errorf("opts = %+v", opts)
	}
}

func TestMissingConfigExitsWithUsage(t *testing.T) {
	var stderr bytes.Buffer
	code := realMain([]string{"--minio"}, noEnv, &stderr)
	if code == 0 {
		t.Fatal("missing --config: exit code 0, want non-zero")
	}
	if !strings.Contains(stderr.String(), "--config is required") || !strings.Contains(stderr.String(), "Usage") {
		t.Errorf("stderr = %q, want the error and the usage text", stderr.String())
	}
}

func TestUnknownFlagExitsNonZero(t *testing.T) {
	if code := realMain([]string{"--nope"}, noEnv, &bytes.Buffer{}); code == 0 {
		t.Error("unknown flag: exit code 0, want non-zero")
	}
}

// --selfcheck needs no --config: the image build runs it offline.
func TestParseFlagsSelfcheck(t *testing.T) {
	opts, err := parseFlags([]string{"--selfcheck"}, noEnv, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	if !opts.selfcheck {
		t.Errorf("opts.selfcheck = false, want true")
	}
	opts, err = parseFlags([]string{"--config", "c.yaml"}, noEnv, &bytes.Buffer{})
	if err != nil || opts.selfcheck {
		t.Errorf("without the flag: opts = %+v, err = %v", opts, err)
	}
}

// --selfcheck fails (exit 20) when httpfs/iceberg cannot be loaded. The
// success path needs the baked extensions; the image's verify stage covers it.
func TestSelfcheckFailsWithoutExtensions(t *testing.T) {
	var stderr bytes.Buffer
	// Settings come from the process env; an empty extension dir has nothing
	// to LOAD (and nothing is fetched: an explicit LOAD never installs).
	t.Setenv("DATUPLET_GATEWAY_DUCKDB_EXTENSION_DIR", t.TempDir())
	t.Setenv("DATUPLET_GATEWAY_DUCKDB_TEMP_DIR", t.TempDir())
	if code := realMain([]string{"--selfcheck"}, noEnv, &stderr); code != exitApplication {
		t.Errorf("selfcheck with no extensions: exit %d, want %d (stderr %q)", code, exitApplication, stderr.String())
	}
}
