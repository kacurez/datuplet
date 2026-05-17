// Package main is the Stdout Writer component: reads a table from the data
// lake via the DataGateway and emits its rows to stdout (primarily for
// debugging and local inspection).
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"

	pb "github.com/datuplet/datuplet/pkg/datagateway/proto/v2"
	sdk "github.com/datuplet/datuplet/sdk/go"
)

func main() {
	ctx := context.Background()

	// Connect to gateway
	client, err := sdk.New(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to connect to gateway: %v\n", err)
		os.Exit(1)
	}
	defer client.Close()

	// Get config
	cfg := client.Config()
	client.Log(ctx, "INFO", fmt.Sprintf("Stdout Writer started: execution=%s", cfg.ExecutionID))

	// Parse config for format
	var compCfg struct {
		Format string `json:"format"`
	}
	if err := client.ParseConfig(&compCfg); err != nil {
		compCfg.Format = "csv" // Default
	}
	if compCfg.Format == "" {
		compCfg.Format = "csv"
	}
	compCfg.Format = strings.ToLower(compCfg.Format)

	client.Log(ctx, "INFO", fmt.Sprintf("Output format: %s", compCfg.Format))

	if len(cfg.InputTables) == 0 {
		fmt.Fprintf(os.Stderr, "Error: no input tables configured\n")
		os.Exit(1)
	}

	// Map format string to proto enum - request data in desired format from gateway
	var outputFormat pb.DataFormat
	switch compCfg.Format {
	case "json":
		outputFormat = pb.DataFormat_FORMAT_JSON
	case "csv":
		outputFormat = pb.DataFormat_FORMAT_CSV
	default:
		outputFormat = pb.DataFormat_FORMAT_CSV
	}

	// Process each input table
	for _, inputTable := range cfg.InputTables {
		tableName := fmt.Sprintf("%s.%s", inputTable.Bucket, inputTable.Table)
		client.Log(ctx, "INFO", fmt.Sprintf("Reading input: %s", tableName))

		// Request data in desired format from gateway - no client-side conversion needed
		reader, err := client.OpenReader(ctx, inputTable.Bucket, inputTable.Table, sdk.WithOutputFormat(outputFormat))
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to open reader for %s: %v\n", tableName, err)
			os.Exit(1)
		}

		switch compCfg.Format {
		case "json":
			if err := outputJSON(ctx, tableName, reader); err != nil {
				reader.Close(ctx)
				fmt.Fprintf(os.Stderr, "Failed to output JSON: %v\n", err)
				os.Exit(1)
			}
		case "csv":
			if err := outputCSV(ctx, tableName, reader); err != nil {
				reader.Close(ctx)
				fmt.Fprintf(os.Stderr, "Failed to output CSV: %v\n", err)
				os.Exit(1)
			}
		default:
			if err := outputRaw(ctx, tableName, reader); err != nil {
				reader.Close(ctx)
				fmt.Fprintf(os.Stderr, "Failed to output raw: %v\n", err)
				os.Exit(1)
			}
		}

		reader.Close(ctx)
	}

	client.Log(ctx, "INFO", "Stdout writer completed successfully")
}

// outputCSV outputs data as CSV to stdout.
func outputCSV(ctx context.Context, name string, reader *sdk.Reader) error {
	fmt.Printf("=== %s (CSV) ===\n", name)

	for {
		chunk, err := reader.NextChunk()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to read chunk: %w", err)
		}

		fmt.Print(string(chunk.Data))

		if chunk.IsLast {
			break
		}
	}
	fmt.Println()
	return nil
}

// outputRaw outputs raw data to stdout.
func outputRaw(ctx context.Context, name string, reader *sdk.Reader) error {
	fmt.Printf("=== %s ===\n", name)

	for {
		chunk, err := reader.NextChunk()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to read chunk: %w", err)
		}

		fmt.Print(string(chunk.Data))

		if chunk.IsLast {
			break
		}
	}
	fmt.Println()
	return nil
}

// outputJSON outputs data as JSON to stdout.
// The gateway converts Parquet to JSON, so we just output the received data directly.
func outputJSON(ctx context.Context, name string, reader *sdk.Reader) error {
	fmt.Printf("=== %s (JSON) ===\n", name)

	// Collect all JSON arrays and merge them
	var allData []json.RawMessage

	for {
		chunk, err := reader.NextChunk()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to read chunk: %w", err)
		}

		// The gateway sends JSON array format: [{"col1":val1,...},...]
		// Parse and append to our collection
		var chunkData []json.RawMessage
		if err := json.Unmarshal(chunk.Data, &chunkData); err != nil {
			// If parsing as array fails, try outputting raw
			fmt.Print(string(chunk.Data))
		} else {
			allData = append(allData, chunkData...)
		}

		if chunk.IsLast {
			break
		}
	}

	// Output merged JSON array with pretty printing
	if len(allData) > 0 {
		output, err := json.MarshalIndent(allData, "", "  ")
		if err != nil {
			return fmt.Errorf("failed to marshal JSON: %w", err)
		}
		fmt.Println(string(output))
	} else {
		fmt.Println("[]")
	}

	return nil
}
