// Package buildinfo carries build-time version metadata, set via -ldflags -X.
package buildinfo

import (
	"strconv"
	"strings"
)

var (
	Version = "dev"
	Commit  = "none"
	Date    = "unknown"
)

type Build struct {
	Version string `json:"version"`
	Commit  string `json:"commit"`
	Date    string `json:"date"`
}

func Info() Build {
	return Build{Version: Version, Commit: Commit, Date: Date}
}

// MinorMismatch reports whether client and server differ in major.minor.
// Unparseable versions (including "dev") never mismatch.
func MinorMismatch(client, server string) bool {
	cm, cn, ok := majorMinor(client)
	if !ok {
		return false
	}
	sm, sn, ok := majorMinor(server)
	if !ok {
		return false
	}
	return cm != sm || cn != sn
}

func majorMinor(v string) (int, int, bool) {
	parts := strings.SplitN(strings.TrimPrefix(v, "v"), ".", 3)
	if len(parts) < 2 {
		return 0, 0, false
	}
	major, err := strconv.Atoi(parts[0])
	if err != nil {
		return 0, 0, false
	}
	minor, err := strconv.Atoi(parts[1])
	if err != nil {
		return 0, 0, false
	}
	return major, minor, true
}
