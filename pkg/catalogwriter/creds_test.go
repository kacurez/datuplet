package catalogwriter

import (
	"testing"
	"time"
)

func TestS3CredsImplementsCreds(t *testing.T) {
	var _ Creds = S3Creds{}
}

func TestGCSCredsImplementsCreds(t *testing.T) {
	var _ Creds = GCSCreds{}
}

func TestS3CredsType(t *testing.T) {
	c := S3Creds{Issued: time.Now(), Expires: time.Now().Add(15 * time.Minute)}
	if got := c.Type(); got != CredsTypeS3 {
		t.Fatalf("Type() = %q, want %q", got, CredsTypeS3)
	}
	if c.IssuedAt().IsZero() {
		t.Fatal("IssuedAt() returned zero")
	}
}

func TestGCSCredsType(t *testing.T) {
	c := GCSCreds{Issued: time.Now(), Expires: time.Now().Add(15 * time.Minute)}
	if got := c.Type(); got != CredsTypeGCS {
		t.Fatalf("Type() = %q, want %q", got, CredsTypeGCS)
	}
}
