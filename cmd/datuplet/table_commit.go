package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/datuplet/datuplet/pkg/icebergjob"
	"github.com/datuplet/datuplet/pkg/lib/status"
)

// tableCommitArgs aggregates the CLI/env input for `datuplet table-commit`.
// The metadata commit goes through lakekeeper using lakekeeper-vended creds
// obtained via the run-token JWT. No long-lived S3 credentials are accepted
// or propagated. Warehouse and project_id come from the validated JWT claims.
type tableCommitArgs struct {
	RunID     string
	Namespace string
	Table     string
	WriteMode icebergjob.WriteMode

	LakekeeperURL string

	RunTokenPath       string
	PipelineAPIJWKSURL string
}

func runTableCommit(args tableCommitArgs) (retErr error) {
	ctx := context.Background()

	// Emit a status message on every error path so K8s
	// extractStatusMessage finds it in pod logs and surfaces it on the
	// TableCommit's Status.Message — otherwise the controller falls back
	// to "TableCommit job failed with exit code 20" which gives the
	// operator nothing to act on. The deferred branch fires only on a
	// non-nil error, so the success path's own status message (emitted
	// further down) is still the canonical happy-path one.
	defer func() {
		if retErr != nil {
			fmt.Printf("%s%s\n", status.StatusMessagePrefix, retErr.Error())
		}
	}()

	fmt.Printf("TableCommit starting...\n")
	fmt.Printf("  Run ID: %s\n", args.RunID)
	switch {
	case args.Namespace != "" && args.Table != "":
		fmt.Printf("  Target: %s.%s\n", args.Namespace, args.Table)
	case args.Namespace != "":
		fmt.Printf("  Target namespace (auto-discover): %s\n", args.Namespace)
	default:
		fmt.Printf("  Target: every table in lakekeeper catalog\n")
	}
	fmt.Printf("  Write Mode: %s\n", args.WriteMode)
	fmt.Printf("  Lakekeeper: %s\n", args.LakekeeperURL)

	// No long-lived S3 credentials. The commit binary uses the run-token JWT;
	// lakekeeper vends per-table STS credentials for all data-plane reads/writes.
	// Warehouse + project_id come from validated JWT claims.
	cfg := &icebergjob.Config{
		RunID:              args.RunID,
		LakekeeperURL:      args.LakekeeperURL,
		Namespace:          args.Namespace,
		WriteMode:          args.WriteMode,
		RunTokenPath:       args.RunTokenPath,
		PipelineAPIJWKSURL: args.PipelineAPIJWKSURL,
	}
	if args.Namespace != "" && args.Table != "" {
		// Explicit single-table commit; auto-discovery is bypassed.
		cfg.Tables = []icebergjob.TableConfig{{
			Namespace: args.Namespace,
			Table:     args.Table,
			WriteMode: args.WriteMode,
		}}
	}
	committer, err := icebergjob.New(cfg)
	if err != nil {
		return fmt.Errorf("failed to create table committer: %w", err)
	}

	result, err := committer.Execute(ctx)
	if err != nil {
		return fmt.Errorf("table commit failed: %w", err)
	}

	if !result.Success {
		if result.Error != "" {
			return fmt.Errorf("table commit failed: %s", result.Error)
		}
		var failed []string
		for _, t := range result.Tables {
			if !t.Success {
				failed = append(failed, fmt.Sprintf("%s.%s: %s", t.Namespace, t.Table, t.Error))
			}
		}
		if len(failed) > 0 {
			return fmt.Errorf("table commit failed for %d table(s): %s", len(failed), strings.Join(failed, "; "))
		}
		return fmt.Errorf("table commit failed with no error details")
	}

	fmt.Printf("\nTableCommit succeeded!\n")
	fmt.Printf("  Tables committed: %d\n", len(result.Tables))
	totalFiles := 0
	for _, t := range result.Tables {
		if t.Success {
			fmt.Printf("  - %s.%s: files=%d\n", t.Namespace, t.Table, t.FilesAdded)
			totalFiles += t.FilesAdded
		} else {
			fmt.Printf("  - %s.%s: FAILED - %s\n", t.Namespace, t.Table, t.Error)
		}
	}

	// Emit structured status message for controller log extraction.
	fmt.Printf("%scommitted %d tables, %d files\n", status.StatusMessagePrefix, len(result.Tables), totalFiles)

	return nil
}

