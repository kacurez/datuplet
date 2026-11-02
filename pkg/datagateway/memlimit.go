package datagateway

import (
	"runtime/debug"
	"strconv"
	"strings"
)

const (
	cgroupV2MemoryMax   = "/sys/fs/cgroup/memory.max"
	cgroupV1MemoryLimit = "/sys/fs/cgroup/memory/memory.limit_in_bytes"
	// cgroupV1Unlimited: v1 reports "no limit" as a huge page-aligned number.
	cgroupV1Unlimited = int64(1) << 62
)

// ApplyMemoryLimit sets the Go soft memory limit to 70% of the container's
// cgroup memory limit. It does nothing when GOMEMLIMIT is set or no limit is
// readable. limit is the cgroup limit found (0 when none, or when GOMEMLIMIT
// is set); applied reports whether debug.SetMemoryLimit was called.
func ApplyMemoryLimit(getenv func(string) string, readFile func(string) ([]byte, error)) (limit int64, applied bool) {
	if getenv("GOMEMLIMIT") != "" {
		return 0, false
	}
	limit = cgroupMemoryLimit(readFile)
	if limit <= 0 {
		return 0, false
	}
	debug.SetMemoryLimit(limit * 70 / 100)
	return limit, true
}

func cgroupMemoryLimit(readFile func(string) ([]byte, error)) int64 {
	if v, ok := readCgroupInt(readFile, cgroupV2MemoryMax); ok {
		return v
	}
	if v, ok := readCgroupInt(readFile, cgroupV1MemoryLimit); ok && v < cgroupV1Unlimited {
		return v
	}
	return 0
}

// readCgroupInt parses a positive integer; "max", unreadable or malformed
// content reports false.
func readCgroupInt(readFile func(string) ([]byte, error), path string) (int64, bool) {
	b, err := readFile(path)
	if err != nil {
		return 0, false
	}
	v, err := strconv.ParseInt(strings.TrimSpace(string(b)), 10, 64)
	if err != nil || v <= 0 {
		return 0, false
	}
	return v, true
}
