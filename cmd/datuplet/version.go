package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/datuplet/datuplet/pkg/buildinfo"
)

var clientVersion = func() (string, string, string) {
	return buildinfo.Version, buildinfo.Commit, buildinfo.Date
}

type serverVersion struct {
	Version string `json:"version"`
	Commit  string `json:"commit"`
}

type versionReport struct {
	Client        buildinfo.Build `json:"client"`
	Server        *serverVersion  `json:"server,omitempty"`
	MinorMismatch bool            `json:"minor_mismatch"`
}

func runVersion(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("version", flag.ContinueOnError)
	fs.SetOutput(stderr)
	asJSON := fs.Bool("json", false, "Structured JSON output")
	remote := fs.String("remote", "", "pipeline-api URL (default: $DATUPLET_REMOTE, then ~/.datuplet/cluster.json)")
	if err := fs.Parse(args); err != nil {
		return 2
	}

	v, c, d := clientVersion()
	rep := versionReport{Client: buildinfo.Build{Version: v, Commit: c, Date: d}}
	if url := resolveVersionRemote(*remote); url != "" {
		rep.Server = fetchServerVersion(url)
	}
	if rep.Server != nil {
		rep.MinorMismatch = buildinfo.MinorMismatch(v, rep.Server.Version)
	}

	if *asJSON {
		enc := json.NewEncoder(stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(rep)
		return 0
	}
	fmt.Fprintf(stdout, "client: %s (commit %s, built %s)\n", v, c, d)
	if rep.Server != nil {
		fmt.Fprintf(stdout, "server: %s (commit %s)\n", rep.Server.Version, rep.Server.Commit)
	}
	if rep.MinorMismatch {
		fmt.Fprintf(stderr, "warning: minor versions differ (client %s, server %s); upgrade the older side\n", v, rep.Server.Version)
	}
	return 0
}

// resolveVersionRemote reads the remote without requiring a token: the
// version endpoint is unauthenticated.
func resolveVersionRemote(flagVal string) string {
	if flagVal != "" {
		return strings.TrimRight(flagVal, "/")
	}
	if env := os.Getenv(envRemote); env != "" {
		return strings.TrimRight(env, "/")
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	data, err := os.ReadFile(filepath.Join(home, ".datuplet", "cluster.json"))
	if err != nil {
		return ""
	}
	var meta clusterMeta
	if json.Unmarshal(data, &meta) != nil {
		return ""
	}
	return strings.TrimRight(meta.PipelineAPIURL, "/")
}

func fetchServerVersion(base string) *serverVersion {
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get(base + "/api/v1/version")
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil
	}
	var sv serverVersion
	if json.NewDecoder(resp.Body).Decode(&sv) != nil || sv.Version == "" {
		return nil
	}
	return &sv
}
