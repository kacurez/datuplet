package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/datuplet/datuplet/pkg/pipelineapi"
	"github.com/datuplet/datuplet/pkg/pipelineapi/authz"
	pipelineapidb "github.com/datuplet/datuplet/pkg/pipelineapi/db"
	pkg8s "github.com/datuplet/datuplet/pkg/pipelineapi/k8s"
)

// schemaMismatchExitCode is returned when the reap-once binary's
// embedded schema version doesn't match the DB's schema_migrations
// latest. Distinct from the generic `exit 1` so CronJob history shows
// the mid-deploy skip clearly.
const schemaMismatchExitCode = 2

func runReapOnce(args []string) error {
	fs := flag.NewFlagSet("reap-once", flag.ExitOnError)
	maxAge := fs.Duration("max-age", 0, "delete PipelineRuns older than this (default REAPER_MAX_AGE or 24h)")
	timeout := fs.Duration("timeout", 10*time.Minute, "overall sweep timeout")
	_ = fs.Parse(args)

	cfg := pipelineapi.LoadConfig()
	if *maxAge == 0 {
		*maxAge = cfg.ReaperMaxAge
	}
	if cfg.DatabaseURL == "" {
		return fmt.Errorf("DATABASE_URL is required for reap-once")
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	pool, err := pipelineapidb.Open(ctx, cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("open db: %w", err)
	}
	defer pool.Close()

	// Schema-version gate. We do NOT run Migrate here: only
	// pipeline-api's serve path owns migrations, under the
	// pg_advisory_lock. A mid-deploy tick that fires against a
	// not-yet-migrated DB exits cleanly rather than corrupting state.
	if err := pipelineapidb.RequireSchemaVersion(ctx, pool); err != nil {
		fmt.Fprintf(os.Stderr, "reap-once: %v\n", err)
		os.Exit(schemaMismatchExitCode)
	}

	c, err := pkg8s.NewClient(pkg8s.ClientOpts{
		InCluster:      cfg.InCluster,
		KubeconfigPath: cfg.KubeconfigPath,
	})
	if err != nil {
		return fmt.Errorf("k8s client: %w", err)
	}

	// Build an OpenFGA authorizer for the run_tuples sweep. Soft-degrade
	// when OPENFGA_* are unset: still run the K8s + run_tuples-row GC, just
	// skip the DeleteTuples call. Matches the cluster-mode `serve` posture.
	authzr := buildReaperAuthorizer(ctx)
	updater := pkg8s.NewDBRunUpdater(pool)
	return pkg8s.ReapOnceWith(ctx, c, *maxAge, updater, pkg8s.ReapOnceOpts{
		Pool:       pool,
		Authorizer: authzr,
	})
}

// buildReaperAuthorizer mirrors the cluster-mode `serve` construction
// path. Returns nil on any error so the caller falls into soft-degrade
// (the new run_tuples sweep skips the FGA DeleteTuples step but still
// GCs the breadcrumb rows).
//
// Reads OPENFGA_STORE_NAME + OPENFGA_MODEL_VERSION and calls
// authz.ResolveStoreAndModel (pin-tuple lookup) to find store_id + model_id.
// Matches the pipeline-api serve path.
func buildReaperAuthorizer(ctx context.Context) authz.Authorizer {
	openfgaURL := os.Getenv("OPENFGA_URL")
	if openfgaURL == "" {
		openfgaURL = "http://openfga.openfga.svc.cluster.local:8080"
	}
	openfgaAPIKey := os.Getenv("OPENFGA_API_KEY")
	storeName := os.Getenv("OPENFGA_STORE_NAME")
	if storeName == "" {
		storeName = "datuplet"
	}
	modelVersion := os.Getenv("OPENFGA_MODEL_VERSION")
	if modelVersion == "" {
		log.Printf("reap-once: OPENFGA_MODEL_VERSION unset (FGA delete step skipped — tuples may linger)")
		return nil
	}
	storeID, modelID, err := authz.ResolveStoreAndModel(ctx, openfgaURL, openfgaAPIKey, storeName, modelVersion)
	if err != nil {
		log.Printf("reap-once: OpenFGA resolve failed: %v (FGA delete step skipped — tuples may linger)", err)
		return nil
	}
	a, err := authz.NewOpenFGAAuthorizer(openfgaURL, storeID, modelID, openfgaAPIKey, 5*time.Second)
	if err != nil {
		log.Printf("reap-once: OpenFGA authorizer: %v (FGA delete step skipped)", err)
		return nil
	}
	return a
}
