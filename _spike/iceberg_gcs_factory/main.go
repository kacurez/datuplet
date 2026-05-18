// Spike harness for RFC 019 Slice A0 acceptance criteria.
// Throwaway — deleted in Slice I. Findings recorded in
// docs/tmp/spikes/2026-05-18-iceberg-gcs-factory.md.
//
// Usage:
//
//	go run ./_spike/iceberg_gcs_factory \
//	  -criterion=1|2|3|4|5|all \
//	  -gcs-bucket=<bucket> -gcs-key-file=<path-to-sa-key.json> \
//	  -lakekeeper-url=<url> -warehouse=<name>
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/url"
	"os"

	iceio "github.com/apache/iceberg-go/io"
	_ "github.com/apache/iceberg-go/io/gocloud" // registers the default gs:// factory

	"cloud.google.com/go/storage"
	"golang.org/x/oauth2/google"
)

func main() {
	criterion := flag.String("criterion", "all", "which criterion to verify (1|2|3|4|5|all)")
	bucket := flag.String("gcs-bucket", "", "GCS bucket (must already exist)")
	keyFile := flag.String("gcs-key-file", "", "Path to a GCP SA key JSON (used to mint OAuth bearer)")
	lakekeeperURL := flag.String("lakekeeper-url", "", "Lakekeeper REST URL")
	warehouse := flag.String("warehouse", "", "Warehouse name in Lakekeeper")
	flag.Parse()
	if *bucket == "" {
		log.Fatalf("--gcs-bucket required")
	}
	ctx := context.Background()

	results := map[string]string{}
	run := func(name string, fn func(context.Context) error) {
		if *criterion != "all" && *criterion != name {
			return
		}
		if err := fn(ctx); err != nil {
			results[name] = fmt.Sprintf("FAIL: %v", err)
		} else {
			results[name] = "PASS"
		}
	}

	run("1", func(c context.Context) error { return probeRegistrationOverride(c, *bucket, *keyFile) })
	run("2", func(c context.Context) error { return probeRefreshingTokenSource(c, *bucket, *keyFile) })
	run("3", func(c context.Context) error { return probeFakeGCSServerBearer(c) })
	run("4", func(c context.Context) error { return probeLakekeeperRefreshEndpoint(c, *lakekeeperURL, *warehouse) })
	run("5", func(c context.Context) error { return probeAuditAttribution(c, *bucket, *keyFile) })

	for k, v := range results {
		fmt.Printf("criterion %s: %s\n", k, v)
	}
	for _, v := range results {
		if v != "PASS" && v != "" {
			os.Exit(1)
		}
	}
}

// nopIO is the minimal iceio.IO implementation needed by the probe.
// The real IO interface uses no context on Open/Remove.
type nopIO struct{}

func (nopIO) Open(_ string) (iceio.File, error) { return nil, nil }
func (nopIO) Remove(_ string) error             { return nil }

func probeRegistrationOverride(ctx context.Context, bucket, keyFile string) error {
	// RFC §4.5.4 criterion 1: iceio.Unregister("gs") + iceio.Register("gs", ...)
	// must succeed without panic and the new factory must be the one consulted
	// by subsequent iceio.LoadFS calls.

	// Step 1a: load a static OAuth token so we can pass it as a prop and
	// verify the factory receives it correctly.
	tok, err := loadStaticOAuthToken(ctx, keyFile)
	if err != nil {
		return fmt.Errorf("load static oauth from key: %w", err)
	}

	uri := fmt.Sprintf("gs://%s", bucket)
	props := map[string]string{"gcs.oauth2.token": tok}

	// Step 1b: try to register "gs" again — it was already registered by the
	// blank import above. Expect a panic.
	panicked := func() (p bool) {
		defer func() { p = recover() != nil }()
		iceio.Register("gs", func(_ context.Context, _ *url.URL, _ map[string]string) (iceio.IO, error) {
			return nil, nil
		})
		return
	}()
	if !panicked {
		return fmt.Errorf("Register did NOT panic on duplicate — RFC §4.5.1 premise is wrong; review upstream change-log")
	}

	// Step 1c: Unregister, then Register the probe's own factory. This is
	// exactly what Slice D's production override will do.
	iceio.Unregister("gs")
	called := false
	iceio.Register("gs", func(_ context.Context, parsed *url.URL, gotProps map[string]string) (iceio.IO, error) {
		called = true
		if gotProps["gcs.oauth2.token"] != tok {
			return nil, fmt.Errorf("factory received props missing gcs.oauth2.token")
		}
		if parsed.Host != bucket {
			return nil, fmt.Errorf("factory received wrong bucket in URL: host=%q want %q", parsed.Host, bucket)
		}
		return &nopIO{}, nil
	})

	if _, err := iceio.LoadFS(ctx, props, uri); err != nil {
		return fmt.Errorf("LoadFS after override returned error: %w", err)
	}
	if !called {
		return fmt.Errorf("LoadFS did NOT invoke the new factory — registry override failed silently")
	}
	return nil
}

// loadStaticOAuthToken mints a short-lived access token from a GCP SA key file.
func loadStaticOAuthToken(ctx context.Context, keyFile string) (string, error) {
	if keyFile == "" {
		return "", fmt.Errorf("--gcs-key-file required for criteria 1/2/5")
	}
	keyBytes, err := os.ReadFile(keyFile)
	if err != nil {
		return "", err
	}
	cfg, err := google.JWTConfigFromJSON(keyBytes, storage.ScopeReadWrite)
	if err != nil {
		return "", err
	}
	src := cfg.TokenSource(ctx)
	t, err := src.Token()
	if err != nil {
		return "", err
	}
	return t.AccessToken, nil
}

func probeRefreshingTokenSource(ctx context.Context, bucket, keyFile string) error {
	// RFC §4.5.4 criterion 2: a TokenSource that re-fetches every 30s must be
	// honored by iceberg-go's gocloud-backed storage client (i.e., the client
	// must not cache the first token).
	return fmt.Errorf("TODO: implement in Task A0.3")
}

func probeFakeGCSServerBearer(ctx context.Context) error {
	// RFC §4.5.4 criterion 3: does fake-gcs-server accept (or fake-validate)
	// Authorization: Bearer <tok> headers?
	return fmt.Errorf("TODO: implement in Task A0.4")
}

func probeLakekeeperRefreshEndpoint(ctx context.Context, lkURL, warehouse string) error {
	// RFC §4.5.4 criterion 4: capture a Lakekeeper loadTable response against
	// the SA-key-bootstrapped warehouse; check whether it emits
	// gcs.oauth2.refresh-credentials-endpoint and (if so) whether that endpoint
	// responds to a POST with the current bearer.
	return fmt.Errorf("TODO: implement in Task A0.5")
}

func probeAuditAttribution(ctx context.Context, bucket, keyFile string) error {
	// RFC §4.5.4 criterion 5: PUT one object with x-goog-custom-audit-* headers
	// AND with Object.Metadata["datuplet-run-id"]=spike-<n>. Wait 2 min. Read
	// the GCS audit log for the bucket. Report which signal(s) surfaced.
	return fmt.Errorf("TODO: implement in Task A0.6")
}
