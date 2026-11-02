package main

import (
	"bytes"
	"os"
	"strings"
	"testing"
	"time"
)

func TestGatewaySubcommandFlags(t *testing.T) {
	noEnv := func(string) string { return "" }

	t.Run("operator args parse", func(t *testing.T) {
		var stderr bytes.Buffer
		opts, err := parseGatewayFlags([]string{
			"--minio", "--config", "x", "--addr", "y",
			"--run-token-path", "z", "--pod-annotations-path", "w",
		}, func(k string) string { return "/from-env/" + k }, &stderr)
		if err != nil {
			t.Fatalf("parseGatewayFlags: %v (stderr %q)", err, stderr.String())
		}
		want := gatewayOptions{configPath: "x", addr: "y", runTokenPath: "z", podAnnotationsPath: "w", mode: "minio"}
		if opts != want {
			t.Errorf("opts = %+v, want %+v", opts, want)
		}
	})

	t.Run("defaults and env fallbacks", func(t *testing.T) {
		env := map[string]string{"RUN_TOKEN_PATH": "/env/token", "POD_ANNOTATIONS_PATH": "/env/annotations"}
		opts, err := parseGatewayFlags([]string{"--local", "--data-dir", "/ignored", "--config", "c.yaml"},
			func(k string) string { return env[k] }, &bytes.Buffer{})
		if err != nil {
			t.Fatalf("parseGatewayFlags: %v", err)
		}
		want := gatewayOptions{configPath: "c.yaml", addr: ":50051", runTokenPath: "/env/token", podAnnotationsPath: "/env/annotations", mode: "local"}
		if opts != want {
			t.Errorf("opts = %+v, want %+v", opts, want)
		}
	})

	t.Run("missing config exits 2 with usage", func(t *testing.T) {
		var stderr bytes.Buffer
		if code := gatewayMain([]string{"--minio"}, noEnv, &stderr); code != 2 {
			t.Fatalf("missing --config: exit code %d, want 2", code)
		}
		if !strings.Contains(stderr.String(), "--config is required") || !strings.Contains(stderr.String(), "Usage") {
			t.Errorf("stderr = %q, want the error and the usage text", stderr.String())
		}
	})

	t.Run("unknown flag exits 2", func(t *testing.T) {
		if code := gatewayMain([]string{"--nope"}, noEnv, &bytes.Buffer{}); code != 2 {
			t.Errorf("unknown flag: exit code %d, want 2", code)
		}
	})

	// Boot failures are served until the hold, then exit 20 (the hold is
	// shortened so the tests do not wait 10 minutes).
	runShort := func(args []string) int {
		opts, err := parseGatewayFlags(args, noEnv, &bytes.Buffer{})
		if err != nil {
			t.Fatalf("parseGatewayFlags: %v", err)
		}
		opts.addr = "127.0.0.1:0"
		return runGatewayWith(opts, noEnv, gatewayHooks{bootFailureHold: 50 * time.Millisecond, httpAddr: "127.0.0.1:0"})
	}

	t.Run("config-load failure exits 20", func(t *testing.T) {
		if code := runShort([]string{"--minio", "--config", t.TempDir() + "/missing.yaml"}); code != gatewayExitApplication {
			t.Errorf("exit code = %d, want %d", code, gatewayExitApplication)
		}
	})

	t.Run("NewServerV2 error exits 20", func(t *testing.T) {
		// A run-token path without a JWKS URL is refused by NewServerV2.
		dir := t.TempDir()
		cfgPath := dir + "/gateway.yaml"
		if err := os.WriteFile(cfgPath, []byte("run_id: r1\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		if code := runShort([]string{"--minio", "--config", cfgPath, "--run-token-path", dir + "/token"}); code != gatewayExitApplication {
			t.Errorf("exit code = %d, want %d", code, gatewayExitApplication)
		}
	})
}
