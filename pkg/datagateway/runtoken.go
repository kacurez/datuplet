package datagateway

import (
	"io"
	"log"
	"os"
)

// maxRunTokenFileBytes caps run-token file reads to guard against an
// outsized projected Secret. K8s caps Secrets at 1 MiB; 2 MiB gives
// headroom for any framing while still preventing a runaway file
// from OOM-ing the gateway. Mirrored in pkg/icebergjob/runtoken.go.
const maxRunTokenFileBytes = 2 << 20

// readBoundedFile opens path and reads up to maxRunTokenFileBytes
// bytes. If the limit is hit (file is at least that large) we log a
// warning and continue with the truncated content; callers will fail
// downstream when JWT validation rejects the truncated payload.
func readBoundedFile(path string) ([]byte, error) {
	f, err := os.Open(path) // #nosec G304 -- path is operator-controlled
	if err != nil {
		return nil, err
	}
	defer f.Close()
	buf, err := io.ReadAll(io.LimitReader(f, maxRunTokenFileBytes))
	if err != nil {
		return nil, err
	}
	if len(buf) == maxRunTokenFileBytes {
		log.Printf("data gateway: run-token file %q hit %d-byte read cap; downstream parse will likely fail", path, maxRunTokenFileBytes)
	}
	return buf, nil
}
