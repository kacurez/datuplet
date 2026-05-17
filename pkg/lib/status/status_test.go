package status

import (
	"strings"
	"testing"
)

func TestClassifyExitCode(t *testing.T) {
	tests := []struct {
		exitCode int
		want     FailureType
	}{
		{0, ""},
		{1, FailureTypeUser},
		{20, FailureTypeApplication},
		{21, FailureTypeApplication},
		{127, FailureTypeApplication},
		{137, FailureTypeApplication},
		{255, FailureTypeApplication},
		{2, FailureTypeApplication},
	}

	for _, tt := range tests {
		got := ClassifyExitCode(tt.exitCode)
		if got != tt.want {
			t.Errorf("ClassifyExitCode(%d) = %q, want %q", tt.exitCode, got, tt.want)
		}
	}
}

func TestExtractStatusMessage(t *testing.T) {
	tests := []struct {
		name     string
		logs     string
		exitCode int
		want     string
	}{
		{
			name:     "status message found",
			logs:     "starting...\nprocessing...\nDUPLET_STATUS_MESSAGE:config error: missing field 'source'\ndone",
			exitCode: 1,
			want:     "config error: missing field 'source'",
		},
		{
			name:     "last status message wins",
			logs:     "DUPLET_STATUS_MESSAGE:first\nDUPLET_STATUS_MESSAGE:second",
			exitCode: 1,
			want:     "second",
		},
		{
			name:     "no status message falls back to last line",
			logs:     "starting...\nprocessing...\nfailed to connect to database",
			exitCode: 20,
			want:     "failed to connect to database",
		},
		{
			name:     "empty logs",
			logs:     "",
			exitCode: 1,
			want:     "component exited with code 1",
		},
		{
			name:     "only whitespace logs",
			logs:     "  \n  \n  ",
			exitCode: 20,
			want:     "component exited with code 20",
		},
		{
			name:     "success with no message",
			logs:     "done",
			exitCode: 0,
			want:     "done",
		},
		{
			name:     "success with empty logs",
			logs:     "",
			exitCode: 0,
			want:     "",
		},
		{
			name:     "status message with prefix in middle of line",
			logs:     "2024-01-01 INFO DUPLET_STATUS_MESSAGE:schema mismatch on column 'id'",
			exitCode: 1,
			want:     "schema mismatch on column 'id'",
		},
		{
			name:     "trailing newlines handled",
			logs:     "error: bad config\n\n\n",
			exitCode: 1,
			want:     "error: bad config",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ExtractStatusMessage(tt.logs, tt.exitCode)
			if got != tt.want {
				t.Errorf("ExtractStatusMessage() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestSanitizeMessage(t *testing.T) {
	tests := []struct {
		name string
		msg  string
		want string
	}{
		{
			name: "clean message",
			msg:  "everything is fine",
			want: "everything is fine",
		},
		{
			name: "strip newlines",
			msg:  "line1\nline2\rline3",
			want: "line1 line2 line3",
		},
		{
			name: "strip control characters",
			msg:  "hello\x00world\x01test",
			want: "helloworldtest",
		},
		{
			name: "truncate long message",
			msg:  strings.Repeat("a", 2000),
			want: strings.Repeat("a", MaxMessageLength),
		},
		{
			name: "trim whitespace",
			msg:  "  hello  ",
			want: "hello",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := SanitizeMessage(tt.msg)
			if got != tt.want {
				t.Errorf("SanitizeMessage() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestSanitizeMessageLength(t *testing.T) {
	// Verify that output never exceeds MaxMessageLength
	longMsg := strings.Repeat("x", MaxMessageLength+500)
	result := SanitizeMessage(longMsg)
	if len(result) > MaxMessageLength {
		t.Errorf("SanitizeMessage() returned %d bytes, want <= %d", len(result), MaxMessageLength)
	}
}
