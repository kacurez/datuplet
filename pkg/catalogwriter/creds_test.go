package catalogwriter

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

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
