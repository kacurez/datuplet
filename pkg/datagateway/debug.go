package datagateway

import (
	"fmt"
	"log"
	"os"
	"runtime/debug"
	"strings"
	"sync/atomic"
)

// debugEnabled is set once at boot from DATUPLET_GATEWAY_DEBUG. Hot-path
// debug-log calls check it via Debugf; when the flag is false each call
// is essentially a single atomic.Bool load + branch.
//
// Accepted values: "1" / "true" / "yes" / "on" (case-insensitive). Anything
// else (including unset) keeps debug logging off.
var debugEnabled atomic.Bool

func init() {
	v := strings.ToLower(strings.TrimSpace(os.Getenv("DATUPLET_GATEWAY_DEBUG")))
	if v == "1" || v == "true" || v == "yes" || v == "on" {
		debugEnabled.Store(true)
	}
}

// DebugEnabled reports whether DATUPLET_GATEWAY_DEBUG was set at boot.
// Useful when constructing the log message itself is non-trivial — guard
// with this so the work is skipped when debug is off.
func DebugEnabled() bool { return debugEnabled.Load() }

// Debugf logs a formatted message when DATUPLET_GATEWAY_DEBUG is set,
// no-op otherwise. Prefixed with "DBG " for greppability.
func Debugf(format string, args ...any) {
	if !debugEnabled.Load() {
		return
	}
	log.Printf("DBG "+format, args...)
}

// GatewayBuildInfo returns a single-line identifier for the gateway
// binary: VCS revision (truncated) + dirty flag + Go toolchain. Logged at
// boot so cluster operators can confirm which gateway build is running.
// Useful for "did the image actually get rebuilt?" diagnostics.
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
