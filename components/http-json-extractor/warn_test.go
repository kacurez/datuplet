package main

import (
	"strconv"
	"strings"
	"testing"

	dgarrow "github.com/datuplet/datuplet/sdk/go/arrow"
)

// TestUnknownKeysWarning covers unknownKeysWarning in both directions: at the
// sink's tracked-name cap (dgarrow.MaxTrackedUnknownKeys), the message must
// disclose that the list may be incomplete; below the cap, it must not — an
// unconditional caveat would be noise on the common, complete-list case.
// Both cases must also keep reporting the exact affected-record count and
// the batch size the schema was fixed from.
func TestUnknownKeysWarning(t *testing.T) {
	atCapKeys := make([]string, dgarrow.MaxTrackedUnknownKeys)
	for i := range atCapKeys {
		atCapKeys[i] = "unknown_" + strconv.Itoa(i)
	}

	tests := []struct {
		name       string
		keys       []string
		dropped    int64
		wantCapped bool
	}{
		{"at_cap_discloses_truncation", atCapKeys, 12345, true},
		{"under_cap_omits_disclosure", []string{"foo", "bar"}, 3, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := unknownKeysWarning(tt.dropped, tt.keys)

			if strings.Contains(msg, "capped") != tt.wantCapped {
				t.Fatalf("capped disclosure present=%v, want %v; message: %q", strings.Contains(msg, "capped"), tt.wantCapped, msg)
			}
			if tt.wantCapped && !strings.Contains(msg, strconv.Itoa(dgarrow.MaxTrackedUnknownKeys)) {
				t.Fatalf("message must name the cap value %d, got: %q", dgarrow.MaxTrackedUnknownKeys, msg)
			}
			if !strings.Contains(msg, strconv.FormatInt(tt.dropped, 10)) {
				t.Fatalf("message must report the exact dropped-record count %d, got: %q", tt.dropped, msg)
			}
			if !strings.Contains(msg, strconv.Itoa(dgarrow.DefaultBatchRows)) {
				t.Fatalf("message must report the schema-fixing batch size %d, got: %q", dgarrow.DefaultBatchRows, msg)
			}
			// First and last key must still be named, whichever branch fired.
			if len(tt.keys) > 0 {
				first, last := tt.keys[0], tt.keys[len(tt.keys)-1]
				if !strings.Contains(msg, first) || !strings.Contains(msg, last) {
					t.Fatalf("message must list the affected keys (checked %q, %q), got: %q", first, last, msg)
				}
			}
		})
	}
}
