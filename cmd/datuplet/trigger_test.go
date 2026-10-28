package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// --- pure helper tests ---

// TestPhaseTerminalAndExitCode pins which phases are terminal and the exit-code
// contract's zero/non-zero split: Succeeded exits 0, every other terminal phase
// (FailedUser, FailedApplication, Cancelled, Expired) exits non-zero.
func TestPhaseTerminalAndExitCode(t *testing.T) {
	const (
		notChecked = iota
		zero
		nonZero
	)
	tests := []struct {
		phase        string
		wantTerminal bool
		wantExit     int // notChecked | zero | nonZero
	}{
		{"Pending", false, notChecked},
		{"Running", false, notChecked},
		{"Succeeded", true, zero},
		{"FailedUser", true, nonZero},
		{"FailedApplication", true, nonZero},
		{"Cancelled", true, nonZero},
		{"Expired", true, nonZero},
		{"", false, notChecked},
		{"Unknown", false, notChecked},
	}
	for _, tc := range tests {
		t.Run(tc.phase, func(t *testing.T) {
			if got := isTerminalPhase(tc.phase); got != tc.wantTerminal {
				t.Errorf("isTerminalPhase(%q) = %v, want %v", tc.phase, got, tc.wantTerminal)
			}
			got := exitCodeForPhase(tc.phase)
			switch tc.wantExit {
			case zero:
				if got != 0 {
					t.Errorf("exitCodeForPhase(%q) = %d, want 0", tc.phase, got)
				}
			case nonZero:
				if got == 0 {
					t.Errorf("exitCodeForPhase(%q) = 0, want non-zero", tc.phase)
				}
			}
		})
	}
}

// --- HTTP-layer tests ---

// fakeServer returns an httptest.Server that responds to the three
// endpoints triggerRun / getRun / cancelRun call. Tests configure the
// behaviour via the passed closures.
type fakeBehaviour struct {
	onTrigger func(w http.ResponseWriter, r *http.Request)
	onGet     func(w http.ResponseWriter, r *http.Request)
	onCancel  func(w http.ResponseWriter, r *http.Request)
}

func newFakeServer(t *testing.T, b fakeBehaviour) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/projects/", func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/runs"):
			if b.onTrigger != nil {
				b.onTrigger(w, r)
			} else {
				http.Error(w, "onTrigger not configured", http.StatusInternalServerError)
			}
		case r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/runs/"):
			if b.onGet != nil {
				b.onGet(w, r)
			} else {
				http.Error(w, "onGet not configured", http.StatusInternalServerError)
			}
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/cancel"):
			if b.onCancel != nil {
				b.onCancel(w, r)
			} else {
				http.Error(w, "onCancel not configured", http.StatusInternalServerError)
			}
		default:
			http.NotFound(w, r)
		}
	})
	return httptest.NewServer(mux)
}

func TestTriggerRunDecodesCreateResponseShape(t *testing.T) {
	srv := newFakeServer(t, fakeBehaviour{
		onTrigger: func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(map[string]string{
				"id":     "00000000-0000-0000-0000-00000000abcd",
				"status": "Pending",
				"k8s_ns": "datuplet",
			})
		},
	})
	defer srv.Close()
	id, err := triggerRun(context.Background(), srv.URL, "proj-1", "gen-big-pipeline", "tok")
	if err != nil {
		t.Fatalf("triggerRun: %v", err)
	}
	if id != "00000000-0000-0000-0000-00000000abcd" {
		t.Errorf("id = %q, want fixture", id)
	}
}

func TestPollUntilTerminalReturnsOnSucceeded(t *testing.T) {
	var hits int32
	srv := newFakeServer(t, fakeBehaviour{
		onGet: func(w http.ResponseWriter, _ *http.Request) {
			n := atomic.AddInt32(&hits, 1)
			phase := "Running"
			if n >= 2 {
				phase = "Succeeded"
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]string{
				"id":         "r1",
				"phase":      phase,
				"created_at": "2026-05-20T22:01:00Z",
			})
		},
	})
	defer srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	run, err := pollUntilTerminal(ctx, srv.URL, "proj-1", "r1", "tok")
	if err != nil {
		t.Fatalf("pollUntilTerminal: %v", err)
	}
	if run.Phase != "Succeeded" {
		t.Errorf("phase = %q, want Succeeded", run.Phase)
	}
}

func TestPollUntilTerminalAbortsOnPermanentClientError(t *testing.T) {
	srv := newFakeServer(t, fakeBehaviour{
		onGet: func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusUnauthorized)
		},
	})
	defer srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := pollUntilTerminal(ctx, srv.URL, "proj-1", "r1", "tok")
	if err == nil {
		t.Fatal("expected error on 401, got nil")
	}
	if !strings.Contains(err.Error(), "401") {
		t.Errorf("expected 401 in error, got %v", err)
	}
}

func TestCancelRunPostsToCancelEndpoint(t *testing.T) {
	var called int32
	srv := newFakeServer(t, fakeBehaviour{
		onCancel: func(w http.ResponseWriter, _ *http.Request) {
			atomic.StoreInt32(&called, 1)
			w.WriteHeader(http.StatusNoContent)
		},
	})
	defer srv.Close()
	if err := cancelRun(context.Background(), srv.URL, "proj-1", "r1", "tok"); err != nil {
		t.Fatalf("cancelRun: %v", err)
	}
	if atomic.LoadInt32(&called) != 1 {
		t.Error("expected cancel endpoint to be hit")
	}
}
