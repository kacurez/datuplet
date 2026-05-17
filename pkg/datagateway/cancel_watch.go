package datagateway

import (
	"bufio"
	"context"
	"errors"
	"os"
	"strings"
	"time"
)

// CancelAnnotationKey is the annotation pipeline-operator sets on
// component pods to ask DG to drain + exit (in-band cancellation).
const CancelAnnotationKey = "datuplet.io/cancel"

// CancelAnnotationValue is the truthy value the annotation must hold to
// trigger cancellation. Anything else (missing, "", "false") leaves DG
// running. Comparing as a string keeps the contract simple — kubelet's
// downward-API projection writes the value verbatim.
const CancelAnnotationValue = "true"

// DefaultCancelPollInterval is how often DG re-reads the downward-API
// annotations file (~5s). The kubelet itself refreshes the projection
// every ~60s, so polling more often than that wastes CPU; 5s is a
// compromise that gives a fast reaction once the kubelet does refresh.
const DefaultCancelPollInterval = 5 * time.Second

// WatchCancelAnnotation watches the file at path for the cancel
// annotation. Returns nil when cancellation is requested (caller
// should exit cleanly), or ctx.Err() if ctx is cancelled first.
//
// path is the projected downward-API file (typically
// /etc/podinfo/annotations on K8s; mounted by applyCancelAnnotationMount).
// Empty path → returns immediately with nil error AFTER ctx is done;
// this lets the gateway start the watcher unconditionally and rely on
// the empty-path branch as a no-op for non-K8s deployments.
//
// File format (kubelet downward-API): one annotation per line as
// `key="quoted value"`. We don't parse it as a full quoted-string
// grammar — a substring match for `datuplet.io/cancel="true"` is
// enough because kubelet escapes embedded quotes (we don't put any
// in the value).
func WatchCancelAnnotation(ctx context.Context, path string, interval time.Duration) error {
	if interval <= 0 {
		interval = DefaultCancelPollInterval
	}
	if path == "" {
		<-ctx.Done()
		return ctx.Err()
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			cancelled, err := readCancelAnnotation(path)
			if err != nil && !errors.Is(err, os.ErrNotExist) {
				// Read errors are tolerated — a transient projection
				// glitch shouldn't kill the watcher. Log nothing to
				// avoid spamming; the next tick will retry.
				continue
			}
			if cancelled {
				return nil
			}
		}
	}
}

// readCancelAnnotation returns true iff the kubelet-projected
// annotations file at path contains the cancel marker. Implemented as
// a streaming line scan rather than a regex to keep the import
// footprint tight.
func readCancelAnnotation(path string) (bool, error) {
	f, err := os.Open(path)
	if err != nil {
		return false, err
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	target := CancelAnnotationKey + "=\"" + CancelAnnotationValue + "\""
	for scanner.Scan() {
		line := scanner.Text()
		if strings.Contains(line, target) {
			return true, nil
		}
	}
	return false, scanner.Err()
}
