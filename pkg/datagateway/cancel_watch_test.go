package datagateway

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestWatchCancelAnnotation_FiresOnTrue exercises the happy path:
// once the file contains the cancel marker, the watcher returns nil.
func TestWatchCancelAnnotation_FiresOnTrue(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	p := filepath.Join(dir, "annotations")

	// Pre-populate with non-cancel content first.
	if err := os.WriteFile(p, []byte("other.io/foo=\"bar\"\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- WatchCancelAnnotation(ctx, p, 50*time.Millisecond)
	}()

	// Flip the file to the cancel shape after a short delay.
	time.Sleep(100 * time.Millisecond)
	body := "other.io/foo=\"bar\"\ndatuplet.io/cancel=\"true\"\n"
	if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("watcher returned error %v, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("watcher did not return after cancel marker was written")
	}
}

// TestWatchCancelAnnotation_HonoursContext makes sure ctx cancel
// propagates so the gateway shutdown path can stop the watcher.
func TestWatchCancelAnnotation_HonoursContext(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	p := filepath.Join(dir, "annotations")
	if err := os.WriteFile(p, []byte(""), 0o644); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- WatchCancelAnnotation(ctx, p, 50*time.Millisecond)
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("watcher returned nil; expected ctx.Err()")
		}
	case <-time.After(time.Second):
		t.Fatal("watcher did not honour ctx cancellation")
	}
}

// TestWatchCancelAnnotation_EmptyPathBlocks confirms the no-K8s case:
// when path is "" the watcher waits indefinitely for ctx and returns
// ctx.Err() rather than firing immediately.
func TestWatchCancelAnnotation_EmptyPathBlocks(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	err := WatchCancelAnnotation(ctx, "", 0)
	if err != context.DeadlineExceeded {
		t.Fatalf("err = %v, want context.DeadlineExceeded", err)
	}
}

// TestReadCancelAnnotation_MultilineFormat verifies we tolerate the
// kubelet's actual projection format: one annotation per line, value
// in double quotes, possibly with other keys interleaved.
func TestReadCancelAnnotation_MultilineFormat(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	p := filepath.Join(dir, "annotations")
	body := "kubectl.kubernetes.io/last-applied-configuration=\"...\"\n" +
		"datuplet.io/run-id=\"abc-123\"\n" +
		"datuplet.io/cancel=\"true\"\n"
	if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	got, err := readCancelAnnotation(p)
	if err != nil {
		t.Fatal(err)
	}
	if !got {
		t.Fatal("readCancelAnnotation should fire on multiline format")
	}

	// "false" should NOT trigger.
	body = "datuplet.io/cancel=\"false\"\n"
	if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	got, err = readCancelAnnotation(p)
	if err != nil {
		t.Fatal(err)
	}
	if got {
		t.Fatal("readCancelAnnotation should ignore false")
	}
}
