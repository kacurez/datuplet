package catalogwriter

import (
	"fmt"
	"strings"
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

// Stringer redaction tests — RFC 019 §4.10. The %v / %+v / %s verbs must
// not expand the bearer / secret fields. %#v deliberately bypasses
// Stringer; reviewers catch that one case.

func TestS3CredsStringRedacts(t *testing.T) {
	c := S3Creds{
		AccessKeyID:     "AKIAEXAMPLE",
		SecretAccessKey: "supersecret-must-not-leak",
		SessionToken:    "session-token-must-not-leak",
		Region:          "us-east-1",
		Endpoint:        "https://s3.example.com",
		Expires:         time.Date(2026, 5, 18, 14, 0, 0, 0, time.UTC),
	}
	for _, verb := range []string{"%v", "%+v", "%s"} {
		got := fmt.Sprintf(verb, c)
		if strings.Contains(got, "supersecret-must-not-leak") {
			t.Fatalf("%s leaked SecretAccessKey: %s", verb, got)
		}
		if strings.Contains(got, "session-token-must-not-leak") {
			t.Fatalf("%s leaked SessionToken: %s", verb, got)
		}
		if !strings.Contains(got, "AKIAEXAMPLE") {
			t.Fatalf("%s dropped AccessKeyID (the safe identifier): %s", verb, got)
		}
	}
}

func TestGCSCredsStringRedacts(t *testing.T) {
	c := GCSCreds{
		OAuthToken:      "ya29.bearer-must-not-leak",
		GCPProjectID:    "kacurez-labs",
		RefreshEndpoint: "https://lakekeeper.example/refresh",
		Expires:         time.Date(2026, 5, 18, 14, 0, 0, 0, time.UTC),
	}
	for _, verb := range []string{"%v", "%+v", "%s"} {
		got := fmt.Sprintf(verb, c)
		if strings.Contains(got, "ya29.bearer-must-not-leak") {
			t.Fatalf("%s leaked OAuthToken: %s", verb, got)
		}
		if !strings.Contains(got, "kacurez-labs") {
			t.Fatalf("%s dropped GCPProjectID (the safe identifier): %s", verb, got)
		}
	}
}
