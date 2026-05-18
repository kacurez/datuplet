// Package catalogwriter metrics registers the two Prometheus counter-vecs
// that track vended-credentials refresh attempts. Both are package-init
// registered via promauto (no explicit Register call required from binaries
// — the default prometheus.DefaultRegisterer is used).
package catalogwriter

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// credsRefreshTotal counts every real lakekeeper round-trip (i.e. every
// VendedCreds.fetch call) that completed successfully. Label:
//
//	type — "s3" or "gcs" (matches CredsType string values).
var credsRefreshTotal = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "datuplet_creds_refresh_total",
		Help: "Number of vended-creds refresh attempts that completed successfully, by backend type.",
	},
	[]string{"type"},
)

// credsRefreshFailuresTotal counts every VendedCreds.fetch call that
// returned an error. Labels:
//
//	type   — "s3" or "gcs" (ExpectedCredsType, always set at construction).
//	reason — bucketed error category:
//	           "token_provider" — TokenProvider callback failed.
//	           "http"           — network error or non-2xx HTTP status.
//	           "parse"          — JSON unmarshal or parseCreds rejected body.
//	           "other"          — anything that doesn't fit the above.
var credsRefreshFailuresTotal = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "datuplet_creds_refresh_failures_total",
		Help: "Number of vended-creds refresh failures, by backend type and reason.",
	},
	[]string{"type", "reason"},
)
