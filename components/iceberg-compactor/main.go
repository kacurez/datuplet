// Package main is the iceberg-compactor component: it asks the Data Gateway
// to bin-pack the small data files of each declared output table.
package main

import (
	"context"
	"fmt"
	"os"

	sdk "github.com/datuplet/datuplet/sdk/go"
)

func main() {
	ctx := context.Background()

	client, err := sdk.New(ctx)
	if err != nil {
		sdk.ExitAppError(fmt.Sprintf("failed to connect to gateway: %v", err))
	}
	defer client.Close()

	var cfg Config
	gwCfg := client.Config()
	if len(gwCfg.Raw) > 0 {
		if err := client.ParseConfig(&cfg); err != nil {
			sdk.ExitUserError(fmt.Sprintf("failed to parse config: %v", err))
		}
	}
	code, msg := run(ctx, client, cfg, gwCfg.DefaultBucket, gwCfg.OutputTables)
	sdk.StatusMessage(msg)
	if code != 0 {
		client.Close()
		os.Exit(code)
	}
}
