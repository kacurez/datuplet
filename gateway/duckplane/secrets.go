//go:build duckdb_arrow

package duckplane

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/datuplet/datuplet/gateway/duck"
	"github.com/datuplet/datuplet/pkg/catalogwriter"
	"github.com/datuplet/datuplet/pkg/datagateway"
)

// minCredsValidity is the floor of the remaining validity a COPY needs.
const minCredsValidity = 2 * time.Minute

var (
	errCredsExpireTooSoon = errors.New("duckplane: vended credentials expire too soon")
	// errSecret is the only error a failed secret statement produces: neither
	// the statement nor DuckDB's message (which can echo it) is surfaced.
	errSecret = errors.New("duckplane: creating the storage secret failed")
)

// ParseS3Endpoint turns a vended s3.endpoint into DuckDB's ENDPOINT, USE_SSL
// and URL_STYLE (spec, Write path 5; like backend/minio.go splitEndpoint): a
// bare host[:port] means HTTPS, an http:// or https:// scheme decides TLS, any
// other scheme is an error. URL_STYLE is vhost only for amazonaws.com and its
// subdomains, path otherwise, independent of TLS.
func ParseS3Endpoint(raw string) (hostPort string, useSSL bool, urlStyle string, err error) {
	if raw == "" {
		return "", false, "", errors.New("duckplane: empty S3 endpoint")
	}
	useSSL = true
	if strings.Contains(raw, "://") {
		u, perr := url.Parse(raw)
		if perr != nil {
			return "", false, "", fmt.Errorf("duckplane: invalid S3 endpoint %q", raw)
		}
		switch u.Scheme {
		case "https":
		case "http":
			useSSL = false
		default:
			return "", false, "", fmt.Errorf("duckplane: unsupported S3 endpoint scheme %q", u.Scheme)
		}
		if u.Host == "" || u.User != nil || (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.Fragment != "" {
			return "", false, "", fmt.Errorf("duckplane: invalid S3 endpoint %q", raw)
		}
		hostPort = u.Host
	} else {
		hostPort = raw
		if strings.ContainsAny(hostPort, "/?#@") {
			return "", false, "", fmt.Errorf("duckplane: invalid S3 endpoint %q", raw)
		}
	}
	host := hostPort
	if u, perr := url.Parse("//" + hostPort); perr == nil && u.Hostname() != "" {
		host = u.Hostname()
	}
	host = strings.ToLower(strings.TrimSuffix(host, "."))
	urlStyle = "path"
	if host == "amazonaws.com" || strings.HasSuffix(host, ".amazonaws.com") {
		urlStyle = "vhost"
	}
	return hostPort, useSSL, urlStyle, nil
}

// isLocalPrefix reports whether a data prefix is a local path (tests): no
// secret is needed to write there.
func isLocalPrefix(prefix string) bool {
	return !strings.Contains(prefix, "://") || strings.HasPrefix(prefix, "file://")
}

// validFor reports whether c stays valid long enough for one COPY:
// max(2 min, 25 % of its issued TTL). Credentials without an expiry pass.
func validFor(c catalogwriter.Creds, now time.Time) bool {
	exp := c.ExpiresAt()
	if exp.IsZero() {
		return true
	}
	need := minCredsValidity
	if iss := c.IssuedAt(); !iss.IsZero() {
		need = max(need, exp.Sub(iss)/4)
	}
	return exp.Sub(now) >= need
}

// currentCreds returns credentials that outlive the next COPY: one Get, and
// if those expire too soon, Invalidate and one more Get.
func currentCreds(ctx context.Context, src datagateway.CredsSource) (catalogwriter.Creds, error) {
	c, err := src.Get(ctx)
	if err != nil {
		return nil, fmt.Errorf("duckplane: vended credentials: %w", err)
	}
	if c != nil && validFor(c, time.Now()) {
		return c, nil
	}
	src.Invalidate()
	if c, err = src.Get(ctx); err != nil {
		return nil, fmt.Errorf("duckplane: vended credentials: %w", err)
	}
	if c != nil && validFor(c, time.Now()) {
		return c, nil
	}
	return nil, errCredsExpireTooSoon
}

// secretSQL builds the CREATE OR REPLACE SECRET for c, scoped to scope. It
// carries secrets: never log it or put it into an error.
func secretSQL(name, scope string, c catalogwriter.Creds) (string, error) {
	var opts []string
	lit := func(key, v string) error {
		q, err := duck.LiteralChecked(v)
		if err != nil {
			return errSecret
		}
		opts = append(opts, key+" "+q)
		return nil
	}
	switch c := c.(type) {
	case catalogwriter.S3Creds:
		opts = append(opts, "TYPE s3")
		if err := lit("KEY_ID", c.AccessKeyID); err != nil {
			return "", err
		}
		if err := lit("SECRET", c.SecretAccessKey); err != nil {
			return "", err
		}
		if c.SessionToken != "" {
			if err := lit("SESSION_TOKEN", c.SessionToken); err != nil {
				return "", err
			}
		}
		if c.Region != "" {
			if err := lit("REGION", c.Region); err != nil {
				return "", err
			}
		}
		// No vended endpoint: AWS S3, DuckDB's defaults (TLS, vhost).
		if c.Endpoint != "" {
			hostPort, useSSL, style, err := ParseS3Endpoint(c.Endpoint)
			if err != nil {
				return "", err
			}
			if err := lit("ENDPOINT", hostPort); err != nil {
				return "", err
			}
			opts = append(opts, fmt.Sprintf("USE_SSL %t", useSSL))
			if err := lit("URL_STYLE", style); err != nil {
				return "", err
			}
		}
	case catalogwriter.GCSCreds:
		opts = append(opts, "TYPE gcs")
		if err := lit("BEARER_TOKEN", c.OAuthToken); err != nil {
			return "", err
		}
	default:
		return "", fmt.Errorf("duckplane: unsupported credentials type %T", c)
	}
	if err := lit("SCOPE", scope); err != nil {
		return "", err
	}
	return "CREATE OR REPLACE SECRET " + duck.Ident(name) + " (" + strings.Join(opts, ", ") + ")", nil
}
