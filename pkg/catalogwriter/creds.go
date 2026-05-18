package catalogwriter

import (
	"fmt"
	"time"
)

// CredsType discriminates between the credential families this package knows
// about. The constants ARE the wire-shape strings used in lakekeeper response
// detection (see parseCreds in vended_creds.go).
type CredsType string

const (
	CredsTypeS3  CredsType = "s3"
	CredsTypeGCS CredsType = "gcs"
)

// Creds is the sealed-interface root for vended credentials returned by
// VendedCreds.Get. The two concrete implementations (S3Creds, GCSCreds)
// MUST be the only types that satisfy it. The unexported isCreds() marker
// method enforces this — no external package can add a third implementation
// without modifying this file. See RFC 019 §4.1.
type Creds interface {
	Type() CredsType
	IssuedAt() time.Time
	ExpiresAt() time.Time
	isCreds() // sealed marker; do NOT remove
}

// S3Creds carries the credential fields produced by parseS3Creds. Populated
// when the lakekeeper response carries the s3.* key family.
type S3Creds struct {
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string
	Region          string
	Endpoint        string
	Issued          time.Time
	Expires         time.Time
}

func (c S3Creds) Type() CredsType      { return CredsTypeS3 }
func (c S3Creds) IssuedAt() time.Time  { return c.Issued }
func (c S3Creds) ExpiresAt() time.Time { return c.Expires }
func (S3Creds) isCreds()               {}

// String redacts SecretAccessKey + SessionToken. Called by every fmt verb
// that defaults to the Stringer (%v, %+v, %s). %#v still exposes fields —
// reviewers must catch that one. RFC 019 §4.10.
func (c S3Creds) String() string {
	return fmt.Sprintf("S3Creds{access_key_id=%q region=%q endpoint=%q expires=%s}",
		c.AccessKeyID, c.Region, c.Endpoint, c.Expires.Format(time.RFC3339))
}

// TTL is the duration between Issued and Expires. Used by the renewal-trigger
// calculation; not exposed to data-plane consumers.
func (c S3Creds) TTL() time.Duration {
	if c.Issued.IsZero() || c.Expires.IsZero() {
		return 0
	}
	return c.Expires.Sub(c.Issued)
}

// GCSCreds carries the credential fields produced by parseGCSCreds. The
// OAuthToken field is sensitive bearer material; the Stringer below
// redacts it. RFC 019 §4.10.
type GCSCreds struct {
	OAuthToken      string
	GCPProjectID    string // GCP project id, NOT the lakekeeper project id
	RefreshEndpoint string // gcs.oauth2.refresh-credentials-endpoint when present, else ""
	Issued          time.Time
	Expires         time.Time
}

func (c GCSCreds) Type() CredsType      { return CredsTypeGCS }
func (c GCSCreds) IssuedAt() time.Time  { return c.Issued }
func (c GCSCreds) ExpiresAt() time.Time { return c.Expires }
func (GCSCreds) isCreds()               {}

// String redacts OAuthToken. Called by every fmt verb that defaults to the
// Stringer (%v, %+v, %s). %#v still exposes fields — reviewers must catch
// that one. Do NOT add MarshalJSON; structured loggers will pick up the
// Stringer via %v formatting.
func (c GCSCreds) String() string {
	return fmt.Sprintf("GCSCreds{project=%q refresh_endpoint=%q expires=%s}",
		c.GCPProjectID, c.RefreshEndpoint, c.Expires.Format(time.RFC3339))
}

// TTL is the duration between Issued and Expires.
func (c GCSCreds) TTL() time.Duration {
	if c.Issued.IsZero() || c.Expires.IsZero() {
		return 0
	}
	return c.Expires.Sub(c.Issued)
}
