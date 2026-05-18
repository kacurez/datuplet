package datupleticeio

import (
	"context"
	"strings"
	"testing"

	iceio "github.com/apache/iceberg-go/io"
)

func TestInitRegistersGS(t *testing.T) {
	// LoadFS with a gs:// URI must call OUR factory, not the upstream one.
	// The factory fails with "missing gcs.oauth2.token" when props is empty —
	// that error message is our fingerprint.
	_, err := iceio.LoadFS(context.Background(), nil, "gs://test-bucket")
	if err == nil {
		t.Fatal("expected missing-token error from empty props")
	}
	if !strings.Contains(err.Error(), "missing gcs.oauth2.token") {
		t.Fatalf("error did not come from datupletGCSFactory: %v", err)
	}
}
