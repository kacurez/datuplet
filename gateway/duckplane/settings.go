//go:build duckdb_arrow

package duckplane

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/datuplet/datuplet/gateway/duck"
)

const (
	envThreads      = "DATUPLET_GATEWAY_DUCKDB_THREADS"
	envMemoryLimit  = "DATUPLET_GATEWAY_DUCKDB_MEMORY_LIMIT"
	envTempDir      = "DATUPLET_GATEWAY_DUCKDB_TEMP_DIR"
	envExtensionDir = "DATUPLET_GATEWAY_DUCKDB_EXTENSION_DIR"

	defaultThreads        = 1
	defaultMemoryLimit    = "512MiB"
	defaultTempDir        = "/scratch"
	defaultMaxTempDirSize = "1GiB"
	// defaultExtensionDir is where the gateway image bakes httpfs + iceberg.
	defaultExtensionDir = "/home/datuplet/.duckdb/extensions"
)

// cgroupMemoryMax is the cgroup v2 memory limit file (a var for tests).
var cgroupMemoryMax = "/sys/fs/cgroup/memory.max"

// SettingsFromEnv resolves the engine settings: threads 1, memory_limit 50 %
// of the cgroup memory limit (512MiB when there is none), temp_directory
// /scratch, max_temp_directory_size 1GiB, each overridable through
// DATUPLET_GATEWAY_DUCKDB_{THREADS,MEMORY_LIMIT,TEMP_DIR,EXTENSION_DIR}. The
// secret and home directories live under the temp directory so nothing
// depends on $HOME.
func SettingsFromEnv() (duck.Settings, error) {
	s := duck.Settings{
		Threads:        defaultThreads,
		MemoryLimit:    cgroupMemoryLimit(),
		TempDir:        defaultTempDir,
		MaxTempDirSize: defaultMaxTempDirSize,
		ExtensionDir:   defaultExtensionDir,
	}
	if v := os.Getenv(envThreads); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			return duck.Settings{}, fmt.Errorf("duckplane: %s=%q is not a positive integer", envThreads, v)
		}
		s.Threads = n
	}
	if v := os.Getenv(envMemoryLimit); v != "" {
		s.MemoryLimit = v
	}
	if v := os.Getenv(envTempDir); v != "" {
		s.TempDir = v
	}
	if v := os.Getenv(envExtensionDir); v != "" {
		s.ExtensionDir = v
	}
	if s.TempDir == "" {
		return duck.Settings{}, errors.New("duckplane: empty temp directory")
	}
	base := strings.TrimRight(s.TempDir, "/")
	s.SecretDir = base + "/.duckdb-secrets"
	s.HomeDir = base + "/.duckdb-home"
	return s, nil
}

// cgroupMemoryLimit returns half the cgroup memory limit in KiB, or the
// 512MiB default when the file is missing or says "max".
func cgroupMemoryLimit() string {
	raw, err := os.ReadFile(cgroupMemoryMax)
	if err != nil {
		return defaultMemoryLimit
	}
	n, err := strconv.ParseInt(strings.TrimSpace(string(raw)), 10, 64)
	if err != nil || n <= 0 {
		return defaultMemoryLimit
	}
	return strconv.FormatInt(n/2/1024, 10) + "KiB"
}
