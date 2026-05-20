package datagateway

import (
	"fmt"
	"log"
	"os"
	"runtime/debug"
	"strings"
	"sync/atomic"
)

// GatewayBuildInfo returns a single-line identifier for the gateway binary:
// VCS revision (truncated) + dirty flag + Go toolchain. Logged at boot so
// cluster operators can confirm which gateway build is running. Useful for
// "did the image get rebuilt?" diagnostics — same purpose as sdk.BuildInfo
// but for the server side.
func GatewayBuildInfo() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "build=(unknown)"
	}
	rev := "(devel)"
	dirty := ""
	for _, s := range info.Settings {
		switch s.Key {
		case "vcs.revision":
			if len(s.Value) >= 12 {
				rev = s.Value[:12]
			} else if s.Value != "" {
				rev = s.Value
			}
		case "vcs.modified":
			if s.Value == "true" {
				dirty = "-dirty"
			}
		}
	}
	return fmt.Sprintf("build=%s%s go=%s", rev, dirty, info.GoVersion)
}

// debugEnabled is a fast atomic flag, set once at boot from the
// DUPLET_GATEWAY_DEBUG environment variable. Hot-path log calls
// check it via Debugf; when the flag is false the call is essentially
// free (one atomic load + a branch).
//
// Set DUPLET_GATEWAY_DEBUG=true (or 1) to enable. Output goes to the
// gateway's stdout via the standard `log` package — same format as
// the existing "Data Gateway v2 listening on …" lines, prefixed with
// "DBG ".
var debugEnabled atomic.Bool

func init() {
	v := strings.ToLower(strings.TrimSpace(os.Getenv("DUPLET_GATEWAY_DEBUG")))
	if v == "1" || v == "true" || v == "yes" || v == "on" {
		debugEnabled.Store(true)
	}
}

// DebugEnabled reports whether verbose debug logging is on. Useful
// when constructing the log message itself is non-trivial (e.g., a
// hex dump or schema stringification) — guard with this so the work
// is skipped when debug is off.
func DebugEnabled() bool { return debugEnabled.Load() }

// Debugf logs a formatted message when DUPLET_GATEWAY_DEBUG is set,
// no-op otherwise. Prefixed with "DBG " so the lines are greppable.
func Debugf(format string, args ...any) {
	if !debugEnabled.Load() {
		return
	}
	log.Printf("DBG "+format, args...)
}
