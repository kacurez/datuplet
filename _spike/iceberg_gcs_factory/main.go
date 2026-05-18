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
	"os"
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

func probeRegistrationOverride(ctx context.Context, bucket, keyFile string) error {
	// RFC §4.5.4 criterion 1: iceio.Unregister("gs") + iceio.Register("gs", ...)
	// must succeed without panic and the new factory must be the one consulted
	// by subsequent iceio.LoadFS calls.
	return fmt.Errorf("TODO: implement in Task A0.2")
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
