package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	"golang.org/x/term"
)

// loginArgs holds the inputs for `datuplet login --remote`. Stdin, Stdout,
// and Stderr are injectable for test-friendliness — real CLI passes os.*.
type loginArgs struct {
	Remote string
	Stdin  io.Reader
	Stdout io.Writer
	Stderr io.Writer
}

// loginResponse mirrors the JSON shape returned by
// POST /api/v1/auth/token (pkg/pipelineapi/http/cli_token_handler.go).
//
// The server's `cluster` block carries only the deploy-time URLs; the
// user's project list is a top-level field. The CLI merges them into
// the on-disk clusterMeta when writing ~/.datuplet/cluster.json.
type loginResponse struct {
	Token     string                 `json:"token"`
	ExpiresAt string                 `json:"expires_at"`
	UserID    string                 `json:"user_id"`
	Cluster   loginResponseCluster   `json:"cluster"`
	Projects  []clusterMetaProject   `json:"projects"`
}

type loginResponseCluster struct {
	LakekeeperURL string `json:"lakekeeper_url"`
	WarehouseName string `json:"warehouse_name"`
}

// runLogin implements `datuplet login --remote <url>`.
//
// Flow:
//  1. Prompt for email + password (password hidden if stdin is a terminal).
//  2. POST {email, password} to <remote>/api/v1/auth/token.
//  3. On 200: write ~/.datuplet/token (raw JWT, 0600) and
//     ~/.datuplet/cluster.json (metadata, 0600).
//  4. On non-200: return a descriptive error.
//
// Security invariants (must not be broken):
//   - The token file contains ONLY the raw JWT string — never the full JSON
//     response. The gateway sidecar bind-mounts this file as a bare bearer
//     string; JSON would break K8s ↔ local-CLI parity.
//   - The JWT is NEVER written to stdout or stderr (bash-history exfil).
//   - The password is NEVER written to stdout, stderr, or any file.
func runLogin(args loginArgs) error {
	// Prompt for email.
	fmt.Fprint(args.Stdout, "Email: ")
	email, err := readLine(args.Stdin)
	if err != nil {
		return fmt.Errorf("read email: %w", err)
	}
	email = strings.TrimSpace(email)

	// Prompt for password. Use term.ReadPassword when stdin is a real
	// terminal (suppresses echo); fall back to plain line-read for piped
	// input (test path, CI, scripts).
	fmt.Fprint(args.Stdout, "Password: ")
	var password string
	if f, ok := args.Stdin.(*os.File); ok && term.IsTerminal(int(f.Fd())) {
		pw, err := term.ReadPassword(int(f.Fd()))
		if err != nil {
			return fmt.Errorf("read password: %w", err)
		}
		// The terminal won't echo a newline after term.ReadPassword.
		fmt.Fprintln(args.Stdout)
		password = string(pw)
	} else {
		line, err := readLine(args.Stdin)
		if err != nil {
			return fmt.Errorf("read password: %w", err)
		}
		password = strings.TrimSpace(line)
	}

	// POST credentials to the pipeline-api token endpoint.
	reqBody, err := json.Marshal(map[string]string{
		"email":    email,
		"password": password,
	})
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}

	url := strings.TrimRight(args.Remote, "/") + "/api/v1/auth/token"
	resp, err := http.Post(url, "application/json", bytes.NewReader(reqBody)) //nolint:gosec // URL supplied by the user
	if err != nil {
		return fmt.Errorf("POST %s: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("login failed (HTTP %d): %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var loginResp loginResponse
	if err := json.NewDecoder(resp.Body).Decode(&loginResp); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}
	if loginResp.Token == "" {
		return fmt.Errorf("server returned empty token")
	}

	// Persist files.
	dir, err := datupletDir()
	if err != nil {
		return err
	}

	// SECURITY: write the raw JWT only — never the JSON blob.
	if err := writeTokenFile(dir, loginResp.Token); err != nil {
		return fmt.Errorf("write token file: %w", err)
	}

	meta := clusterMeta{
		LakekeeperURL:  loginResp.Cluster.LakekeeperURL,
		WarehouseName:  loginResp.Cluster.WarehouseName,
		ExpiresAt:      loginResp.ExpiresAt,
		UserID:         loginResp.UserID,
		PipelineAPIURL: args.Remote,
		Projects:       loginResp.Projects,
	}
	if err := writeClusterFile(dir, meta); err != nil {
		return fmt.Errorf("write cluster file: %w", err)
	}

	tokenPath := dir + "/token"
	clusterPath := dir + "/cluster.json"
	fmt.Fprintf(args.Stdout, "Logged in as %s\n", email)
	fmt.Fprintf(args.Stdout, "  token:   %s (expires %s)\n", tokenPath, loginResp.ExpiresAt)
	fmt.Fprintf(args.Stdout, "  cluster: %s\n", clusterPath)

	return nil
}

// readLine reads one line from r, stopping at '\n' or EOF.
func readLine(r io.Reader) (string, error) {
	var sb strings.Builder
	buf := make([]byte, 1)
	for {
		n, err := r.Read(buf)
		if n > 0 {
			if buf[0] == '\n' {
				break
			}
			sb.WriteByte(buf[0])
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", err
		}
	}
	return sb.String(), nil
}
