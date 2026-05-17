// Package status provides shared utilities for exit code classification
// and status message extraction from component logs.
package status

import (
	"strings"
	"unicode"
)

// FailureType classifies the type of component failure.
type FailureType string

const (
	// FailureTypeUser indicates a user error (bad config, schema mismatch, etc).
	// Exit code 1 maps to this type.
	FailureTypeUser FailureType = "FailedUser"

	// FailureTypeApplication indicates an application/infrastructure error
	// (network timeout, OOM, internal bug, etc). Exit codes >= 20 map to this type.
	FailureTypeApplication FailureType = "FailedApplication"

	// StatusMessagePrefix is the protocol prefix for status messages in stdout.
	// Components emit "DUPLET_STATUS_MESSAGE:<message>" for structured status reporting.
	StatusMessagePrefix = "DUPLET_STATUS_MESSAGE:"

	// MaxMessageLength is the maximum length of a status message in bytes.
	MaxMessageLength = 1024
)

// ClassifyExitCode returns the failure type for a non-zero exit code.
// Exit code 1 is FailedUser, everything else non-zero is FailedApplication.
// Returns empty string for exit code 0 (success).
func ClassifyExitCode(exitCode int) FailureType {
	switch exitCode {
	case 0:
		return ""
	case 1:
		return FailureTypeUser
	default:
		return FailureTypeApplication
	}
}

// ExtractStatusMessage extracts a human-readable status message from component logs.
// It scans for the last line with the DUPLET_STATUS_MESSAGE: prefix.
// Falls back to the last non-empty log line, then to a generic message.
func ExtractStatusMessage(logs string, exitCode int) string {
	lines := strings.Split(logs, "\n")

	// Scan for last DUPLET_STATUS_MESSAGE: line
	var lastStatusMsg string
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if _, after, ok := strings.Cut(line, StatusMessagePrefix); ok {
			lastStatusMsg = strings.TrimSpace(after)
		}
	}

	if lastStatusMsg != "" {
		return SanitizeMessage(lastStatusMsg)
	}

	// Fall back to last non-empty log line
	for i := len(lines) - 1; i >= 0; i-- {
		line := strings.TrimSpace(lines[i])
		if line != "" {
			return SanitizeMessage(line)
		}
	}

	// Final fallback
	if exitCode != 0 {
		return SanitizeMessage("component exited with code " + itoa(exitCode))
	}

	return ""
}

// SanitizeMessage strips newlines, control characters, and truncates to MaxMessageLength bytes.
func SanitizeMessage(msg string) string {
	// Strip control characters and newlines
	var b strings.Builder
	for _, r := range msg {
		if r == '\n' || r == '\r' {
			b.WriteRune(' ')
		} else if unicode.IsControl(r) {
			continue
		} else {
			b.WriteRune(r)
		}
	}

	result := strings.TrimSpace(b.String())

	// Truncate to MaxMessageLength bytes
	if len(result) > MaxMessageLength {
		result = result[:MaxMessageLength]
	}

	return result
}

// itoa converts an int to a string without importing strconv.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}

	negative := false
	if n < 0 {
		negative = true
		n = -n
	}

	var digits [20]byte
	i := len(digits)
	for n > 0 {
		i--
		digits[i] = byte('0' + n%10)
		n /= 10
	}

	if negative {
		i--
		digits[i] = '-'
	}

	return string(digits[i:])
}
