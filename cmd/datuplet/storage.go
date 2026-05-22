package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

// storageHTTPClient is dedicated to the storage subcommand. 30s per-request
// timeout matches trigger.go's pattern — guards against unresponsive
// pipeline-api endpoints from hanging the CLI indefinitely.
var storageHTTPClient = &http.Client{Timeout: 30 * time.Second}

// parseNsTable splits "<ns>.<table>" into its two parts. Both parts must
// be non-empty and the single "." separator must appear exactly once.
func parseNsTable(s string) (ns, table string, err error) {
	parts := strings.Split(s, ".")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", fmt.Errorf("invalid <namespace>.<table> reference %q", s)
	}
	return parts[0], parts[1], nil
}

// storageGET issues a GET against the storage REST path and writes the
// raw response body to stdout. Storage endpoints already return JSON; we
// pass it through verbatim. Callers may decode + reformat for non-default
// --format flags.
func storageGET(remote, path, token string) ([]byte, error) {
	url := fmt.Sprintf("%s/api/v1/storage%s", strings.TrimRight(remote, "/"), path)
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := storageHTTPClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode/100 != 2 {
		return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
	}
	return body, nil
}

func storageBaseArgs(remoteFlag, tokenFileFlag, projectFlag string) (*remoteArgs, error) {
	return loadRemoteArgs(remoteFlag, tokenFileFlag, projectFlag)
}

func runStorageTables(remote, tokenFile, project string) error {
	args, err := storageBaseArgs(remote, tokenFile, project)
	if err != nil {
		return err
	}
	body, err := storageGET(args.Remote, fmt.Sprintf("/projects/%s/tables", args.ID), args.Token)
	if err != nil {
		return err
	}
	return prettyPrintJSON(body)
}

func runStorageEndpoint(subPath string) func(remote, tokenFile, project, ref string) error {
	return func(remote, tokenFile, project, ref string) error {
		ns, tbl, err := parseNsTable(ref)
		if err != nil {
			return err
		}
		args, err := storageBaseArgs(remote, tokenFile, project)
		if err != nil {
			return err
		}
		path := fmt.Sprintf("/projects/%s/tables/%s/%s/%s", args.ID, ns, tbl, subPath)
		body, err := storageGET(args.Remote, path, args.Token)
		if err != nil {
			return err
		}
		return prettyPrintJSON(body)
	}
}

var (
	runStorageInfo    = runStorageEndpoint("info")
	runStorageSchema  = runStorageEndpoint("schema")
	runStorageHistory = runStorageEndpoint("snapshots")
)

// runStorageSample is a special-case wrapper that passes the optional
// --rows query parameter; the server defaults to a reasonable preview
// row count when omitted (see pipelineapi/storage/handlers.go).
func runStorageSample(remote, tokenFile, project, ref string, rows int) error {
	ns, tbl, err := parseNsTable(ref)
	if err != nil {
		return err
	}
	args, err := storageBaseArgs(remote, tokenFile, project)
	if err != nil {
		return err
	}
	path := fmt.Sprintf("/projects/%s/tables/%s/%s/preview", args.ID, ns, tbl)
	if rows > 0 {
		path = fmt.Sprintf("%s?rows=%d", path, rows)
	}
	body, err := storageGET(args.Remote, path, args.Token)
	if err != nil {
		return err
	}
	return prettyPrintJSON(body)
}

func prettyPrintJSON(raw []byte) error {
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		// Not JSON? Just write the bytes through.
		_, werr := os.Stdout.Write(raw)
		return werr
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}
