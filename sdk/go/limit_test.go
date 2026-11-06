package sdk

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	pb "github.com/datuplet/datuplet/pkg/datagateway/proto/v2"
)

func TestWriter_LimitReachedFromHTTP(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if calls.Add(1) == 1 {
			_, _ = w.Write([]byte(`{"rows_accepted":3}`))
			return
		}
		_, _ = w.Write([]byte(`{"rows_accepted":2,"limit_reached":true}`))
	}))
	defer srv.Close()

	w := newTestWriter(srv, 0)
	res, err := w.WriteChunk(context.Background(), []byte(`{"a":1}`))
	if err != nil {
		t.Fatal(err)
	}
	if res.LimitReached || w.LimitReached() {
		t.Fatalf("limit reported before the gateway said so")
	}
	res, err = w.WriteChunk(context.Background(), []byte(`{"a":2}`))
	if err != nil {
		t.Fatal(err)
	}
	if !res.LimitReached || !w.LimitReached() {
		t.Fatalf("LimitReached res=%v writer=%v, want true/true", res.LimitReached, w.LimitReached())
	}
}

func TestConfig_PreviewRowLimit(t *testing.T) {
	c := &Client{config: &pb.ComponentConfig{PreviewRowLimit: 7}}
	if got := c.Config().PreviewRowLimit; got != 7 {
		t.Fatalf("PreviewRowLimit = %d, want 7", got)
	}
	if got := (&Client{config: &pb.ComponentConfig{}}).Config().PreviewRowLimit; got != 0 {
		t.Fatalf("PreviewRowLimit outside preview = %d, want 0", got)
	}
}
