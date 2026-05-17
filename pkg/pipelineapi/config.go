// Package pipelineapi is the central API for Datuplet.
package pipelineapi

import (
	"os"
	"time"

	"github.com/datuplet/datuplet/pkg/pipelineapi/tokens"
)

// Config holds all pipeline-api runtime configuration.
type Config struct {
	// Addr is the HTTP listen address (e.g., ":8081").
	Addr string
	// DatabaseURL is the Postgres connection string. Empty in Task 1;
	// required by Task 2+.
	DatabaseURL string
	// CookieSecure controls whether the session cookie requires HTTPS.
	// Defaults to false for local dev; operators must set it true in production.
	CookieSecure bool
	// SigningKeyFile is the path to the RS256 PEM private key. When set,
	// the server loads it as a Signer and enables /api/v1/auth/jwks.json
	// and token minting.
	SigningKeyFile string
	// KeyID is the JWK `kid` label advertised in JWKS (default "key-1").
	KeyID string
	// KubeconfigPath is the filesystem path to a kubeconfig. Dev-only;
	// empty when running in-cluster.
	KubeconfigPath string
	// InCluster, when true, makes pipeline-api use rest.InClusterConfig()
	// to build its K8s client. Set via PIPELINE_API_IN_CLUSTER=true.
	// Auto-detection of KUBERNETES_SERVICE_HOST was rejected: a Pod without
	// a mounted ServiceAccount token would then crash at startup instead of
	// running K8s-disabled. Opt-in keeps the failure mode explicit.
	InCluster bool
	// Audience is the JWT `aud` claim minted on run-tokens. Defaults to
	// tokens.TableTokenAudience ("datuplet-catalog") — the audience
	// lakekeeper validates. AUDIENCE env overrides for tests that pin a
	// different audience.
	Audience string
	// ReaperMaxAge is the oldest a PipelineRun is allowed to live before the
	// reaper deletes it. Default 24h. Used as the default for
	// `pipeline-api reap-once --max-age` (the CronJob sets it explicitly).
	ReaperMaxAge time.Duration
	// UIDir is the filesystem path to the ui/product directory. When set,
	// pipeline-api serves it at /ui/*; when empty, /ui/ returns 404. The
	// Dockerfile.pipeline-api COPYs ui/product to /app/ui/product and the
	// K8s Deployment sets PIPELINE_API_UI_DIR accordingly.
	UIDir string
	// PublicURL is the externally-reachable base URL for pipeline-api,
	// advertised in the OIDC discovery document at
	// /.well-known/openid-configuration. Lakekeeper fetches that doc to
	// discover jwks_uri + issuer. Set via
	// PIPELINE_API_PUBLIC_URL. Empty disables /.well-known.
	//
	// Example (cluster): "http://pipeline-api.datuplet.svc.cluster.local:8081"
	PublicURL string
	// LakekeeperPublicURL is the externally-reachable URL of the lakekeeper
	// REST API as a developer's laptop sees it. Returned to CLI clients in
	// the `cluster.lakekeeper_url` field of POST /api/v1/auth/token
	// so the CLI can talk to lakekeeper directly.
	//
	// Distinct from PublicURL: PublicURL identifies pipeline-api itself
	// (used by lakekeeper's OIDC discovery in the OTHER direction), this
	// field identifies lakekeeper for the CLI. Set via
	// PIPELINE_API_LAKEKEEPER_PUBLIC_URL. Empty omits the field from the
	// response — CLI clients then fall back to their own configured value.
	//
	// Examples:
	//   OrbStack dev: "http://host.docker.internal:8181/catalog"
	//   Production:   "https://lakekeeper.example.com/catalog"
	LakekeeperPublicURL string
}

// LoadConfig reads config from env with sensible defaults.
// Every field can also be overridden by CLI flags in main.go.
func LoadConfig() Config {
	return Config{
		Addr:              envOr("PIPELINE_API_ADDR", ":8081"),
		DatabaseURL:       os.Getenv("DATABASE_URL"),
		CookieSecure:      os.Getenv("PIPELINE_API_COOKIE_SECURE") == "true",
		SigningKeyFile:    os.Getenv("SIGNING_KEY_FILE"),
		KeyID:             envOr("SIGNING_KEY_ID", "key-1"),
		KubeconfigPath:    os.Getenv("KUBECONFIG"),
		InCluster:         os.Getenv("PIPELINE_API_IN_CLUSTER") == "true",
		Audience:          envOr("AUDIENCE", tokens.TableTokenAudience),
		ReaperMaxAge:      envDurationOr("REAPER_MAX_AGE", 24*time.Hour),
		UIDir:               os.Getenv("PIPELINE_API_UI_DIR"),
		PublicURL:           os.Getenv("PIPELINE_API_PUBLIC_URL"),
		LakekeeperPublicURL: os.Getenv("PIPELINE_API_LAKEKEEPER_PUBLIC_URL"),
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envDurationOr(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}
