package main

import (
	"math/rand/v2"
)

// errInjectionPoint decides at which row (or time-analogous point) the
// user-error should fire. Returns nil when no injection is configured.
//
// Priority: rowsCount > timeoutInSeconds > sizeInBytes.
//   - rowsCount set   → random row index in [0, rowsCount).
//   - timeout set     → random ms offset stored as a "virtual row number"
//     equal to timeout_ms; the caller compares rowsWritten against it but
//     shouldStop fires on elapsed time anyway, so this just ensures the
//     error fires before the timeout.
//   - sizeInBytes set → random byte offset in [0, sizeInBytes] treated as
//     a virtual row; the caller checks bytesWritten against the threshold.
//     The random-mode loop compares rowsWritten, not bytes, so we pick a
//     row number in [0, sizeInBytes/estimatedRowSize] where estimatedRowSize
//     is 64 bytes (pessimistic lower bound).
//
// Callers must check `r.UserErrorMessage != ""` before using this function.
func errInjectionPoint(rng *rand.Rand, r *RandomSpec) *int {
	if r == nil || r.UserErrorMessage == "" || r.Limit == nil {
		return nil
	}

	var at int
	switch {
	case r.Limit.RowsCount > 0:
		// Uniform in [0, rowsCount-1] — strictly less than the stop boundary
		// so the error ALWAYS fires before shouldStop trips.
		// Caller's loop checks errAt BEFORE shouldStop on each iteration for
		// belt-and-braces.
		at = rng.IntN(r.Limit.RowsCount)

	case r.Limit.TimeoutInSeconds > 0:
		// Translate to a "row number" that corresponds to a time fraction.
		// The generator increments rowsWritten once per row; we use a large
		// virtual count (timeoutMs) so the comparison `rowsWritten == errAt`
		// fires at a sub-second granularity.
		timeoutMs := r.Limit.TimeoutInSeconds * 1000
		at = rng.IntN(timeoutMs)

	case r.Limit.SizeInBytes > 0:
		const estRowBytes = 64
		maxRows := r.Limit.SizeInBytes / estRowBytes
		if maxRows < 1 {
			maxRows = 1
		}
		at = rng.IntN(maxRows)

	default:
		return nil
	}

	return &at
}
