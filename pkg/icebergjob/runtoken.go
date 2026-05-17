package icebergjob

import (
	"io"
	"log"
	"os"
)

// maxRunTokenFileBytes caps run-token file reads. Mirror of the
// constant in pkg/datagateway/runtoken/validator.go — the two services
// have disjoint import graphs so we duplicate the helper rather than
// share it. K8s caps Secrets at 1 MiB; 2 MiB gives headroom for
// any framing while still preventing a runaway file from OOM-ing
// the commit job.
const maxRunTokenFileBytes = 2 << 20

// readBoundedFile opens path and reads up to maxRunTokenFileBytes bytes.
// Used by Execute to re-read the validated token for outbound Bearer headers.
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
		log.Printf("table commit: run-token file %q hit %d-byte read cap; downstream parse will likely fail", path, maxRunTokenFileBytes)
	}
	return buf, nil
}

// Use runtokenpkg.LoadAndValidateRunToken (pkg/datagateway/runtoken) to
// load and validate the run token — it validates all security checks
// before returning the claims.
