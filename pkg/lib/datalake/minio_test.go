package datalake

import "testing"

// TestNormalizeMinIOEndpoint pins the contract that lets table-commit
// (and any other caller) pass either a bare host:port or a scheme-ful
// URL to NewMinIODataLake. The MinIO Go SDK's `New()` rejects
// fully-qualified URLs with "Endpoint url cannot have fully qualified
// paths"; we strip the scheme upstream so callers don't have to know.
func TestNormalizeMinIOEndpoint(t *testing.T) {
	cases := []struct {
		name        string
		endpoint    string
		cfgUseSSL   bool
		wantHost    string
		wantUseSSL  bool
	}{
		{"bare host:port preserves cfg flag false", "minio:9000", false, "minio:9000", false},
		{"bare host:port preserves cfg flag true", "minio:9000", true, "minio:9000", true},
		{"http:// strips scheme + flips SSL off", "http://host.docker.internal:30900", true, "host.docker.internal:30900", false},
		{"https:// strips scheme + flips SSL on", "https://s3.us-east-1.amazonaws.com", false, "s3.us-east-1.amazonaws.com", true},
		{"http:// with UseSSL=false stays http", "http://localhost:9000", false, "localhost:9000", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gotHost, gotSSL := normalizeMinIOEndpoint(tc.endpoint, tc.cfgUseSSL)
			if gotHost != tc.wantHost {
				t.Errorf("host = %q, want %q", gotHost, tc.wantHost)
			}
			if gotSSL != tc.wantUseSSL {
				t.Errorf("useSSL = %v, want %v", gotSSL, tc.wantUseSSL)
			}
		})
	}
}
