package sdk

import (
	"fmt"
	"runtime/debug"
)

// Version returns a human-readable build identifier for the SDK. The
// underlying source is Go's BuildInfo (no build-time -ldflags needed):
// the main module's VCS revision + dirty flag when the binary was built
// from a git checkout, or "(devel)" / "(unknown)" otherwise.
//
// Components are encouraged to log this at startup so cluster operators
// can confirm at a glance which SDK build is actually running — useful
// for verifying that image rebuilds picked up new SDK behavior.
func Version() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "(unknown)"
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
	return rev + dirty
}

// Info is a snapshot of SDK build + default behavior. Components log this
// once at startup to make a run's environment debuggable from pod logs.
type Info struct {
	Version          string // VCS revision (or "(devel)" / "(unknown)")
	GoVersion        string // Go toolchain version
	DefaultBatchSize int64  // Write() accumulator threshold (bytes)
}

// BuildInfo returns a snapshot of SDK build + default behavior.
func BuildInfo() Info {
	out := Info{
		Version:          Version(),
		DefaultBatchSize: defaultBatchSize,
	}
	if info, ok := debug.ReadBuildInfo(); ok {
		out.GoVersion = info.GoVersion
	}
	return out
}

// String formats Info as a single human-readable line. Suitable for a
// startup log entry — terse enough to grep, structured enough to read.
func (i Info) String() string {
	return fmt.Sprintf("datuplet-sdk version=%s go=%s default_batch_size=%d",
		i.Version, i.GoVersion, i.DefaultBatchSize)
}
