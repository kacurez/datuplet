package datagateway

import (
	"errors"
	"runtime/debug"
	"testing"
)

func TestApplyMemoryLimit(t *testing.T) {
	prev := debug.SetMemoryLimit(-1)
	t.Cleanup(func() { debug.SetMemoryLimit(prev) })

	tests := []struct {
		name        string
		env         map[string]string
		files       map[string]string
		wantLimit   int64
		wantApplied bool
	}{
		{"GOMEMLIMIT set", map[string]string{"GOMEMLIMIT": "1GiB"}, map[string]string{cgroupV2MemoryMax: "1073741824"}, 0, false},
		{"v2 limit", nil, map[string]string{cgroupV2MemoryMax: "1073741824\n"}, 1073741824, true},
		{"v2 max falls back to v1", nil, map[string]string{cgroupV2MemoryMax: "max\n", cgroupV1MemoryLimit: "536870912"}, 536870912, true},
		{"v1 unlimited", nil, map[string]string{cgroupV1MemoryLimit: "9223372036854771712"}, 0, false},
		{"v2 max and v1 unlimited", nil, map[string]string{cgroupV2MemoryMax: "max", cgroupV1MemoryLimit: "9223372036854771712"}, 0, false},
		{"nothing readable", nil, nil, 0, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			debug.SetMemoryLimit(prev)
			getenv := func(k string) string { return tc.env[k] }
			readFile := func(p string) ([]byte, error) {
				if s, ok := tc.files[p]; ok {
					return []byte(s), nil
				}
				return nil, errors.New("not found")
			}
			limit, applied := ApplyMemoryLimit(getenv, readFile)
			if limit != tc.wantLimit || applied != tc.wantApplied {
				t.Fatalf("got (%d, %v), want (%d, %v)", limit, applied, tc.wantLimit, tc.wantApplied)
			}
			if applied {
				if got, want := debug.SetMemoryLimit(-1), tc.wantLimit*70/100; got != want {
					t.Fatalf("memory limit = %d, want %d", got, want)
				}
			}
		})
	}
}
