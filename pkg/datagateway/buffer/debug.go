package buffer

import (
	"log"
	"os"
	"strings"
	"sync/atomic"
)

// debugEnabled mirrors pkg/datagateway/debug.go — the same env var
// gates both packages so operators flip ONE flag at boot. Kept in a
// separate file (not imported from the parent package) because the
// parent already imports buffer; the reverse would be cyclic.
var debugEnabled atomic.Bool

func init() {
	v := strings.ToLower(strings.TrimSpace(os.Getenv("DUPLET_GATEWAY_DEBUG")))
	if v == "1" || v == "true" || v == "yes" || v == "on" {
		debugEnabled.Store(true)
	}
}

// debugf logs a formatted message when DUPLET_GATEWAY_DEBUG is set,
// prefixed with "DBG buffer: " for greppability.
func debugf(format string, args ...any) {
	if !debugEnabled.Load() {
		return
	}
	log.Printf("DBG buffer: "+format, args...)
}
