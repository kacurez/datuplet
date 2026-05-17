// Example component using the thin Datuplet SDK.
// Filters products with price > threshold from config.
package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"strconv"

	"github.com/datuplet/datuplet/sdk/go"
)

type ComponentConfig struct {
	Operations []Operation `json:"operations"`
}

type Operation struct {
	Type   string  `json:"type"`
	Column string  `json:"column"`
	Op     string  `json:"op"`
	Value  float64 `json:"value"`
}

func main() {
	ctx := context.Background()

	// Connect to gateway
	client, err := sdk.New(ctx)
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer client.Close()

	// Get config
	cfg := client.Config()
	fmt.Printf("Execution: %s\n", cfg.ExecutionID)
	fmt.Printf("Inputs: %v\n", cfg.Inputs)
	fmt.Printf("Outputs: %v\n", cfg.Outputs)

	// Parse component-specific config
	var compCfg ComponentConfig
	if err := client.ParseConfig(&compCfg); err != nil {
		log.Fatalf("Failed to parse config: %v", err)
	}
	fmt.Printf("Operations: %+v\n", compCfg.Operations)

	// Open reader
	reader, err := client.OpenReader(ctx, "products")
	if err != nil {
		log.Fatalf("Failed to open reader: %v", err)
	}

	// Open writer
	writer, err := client.OpenWriter(ctx, "filtered")
	if err != nil {
		log.Fatalf("Failed to open writer: %v", err)
	}

	// Process data
	var totalRows, filteredRows int64
	var header []string

	for {
		chunk, err := reader.NextChunk()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("Read error: %v", err)
		}

		// Parse CSV
		csvReader := csv.NewReader(bytes.NewReader(chunk.Data))
		records, err := csvReader.ReadAll()
		if err != nil {
			log.Fatalf("CSV parse error: %v", err)
		}

		// Get header from first chunk
		if header == nil && len(records) > 0 {
			header = records[0]
			records = records[1:]
		}

		// Find price column index
		priceIdx := -1
		for i, col := range header {
			if col == "price" {
				priceIdx = i
				break
			}
		}

		// Filter rows
		var filtered [][]string
		for _, row := range records {
			totalRows++

			// Apply filter operations
			keep := true
			for _, op := range compCfg.Operations {
				if op.Type == "filter" && op.Column == "price" && priceIdx >= 0 {
					price, _ := strconv.ParseFloat(row[priceIdx], 64)
					switch op.Op {
					case ">":
						keep = keep && price > op.Value
					case ">=":
						keep = keep && price >= op.Value
					case "<":
						keep = keep && price < op.Value
					case "<=":
						keep = keep && price <= op.Value
					}
				}
			}

			if keep {
				filtered = append(filtered, row)
				filteredRows++
			}
		}

		// Write filtered data
		if len(filtered) > 0 {
			var buf bytes.Buffer
			csvWriter := csv.NewWriter(&buf)

			// Include header on first write
			if totalRows == int64(len(records)) {
				csvWriter.Write(header)
			}

			for _, row := range filtered {
				csvWriter.Write(row)
			}
			csvWriter.Flush()

			if err := writer.WriteChunk(ctx, buf.Bytes(), int64(len(filtered))); err != nil {
				log.Fatalf("Write error: %v", err)
			}
		}
	}

	reader.Close(ctx)
	writer.Close(ctx)

	// Commit
	result, err := client.Commit(ctx)
	if err != nil {
		log.Fatalf("Commit failed: %v", err)
	}

	fmt.Printf("\nProcessed %d rows, filtered to %d rows\n", totalRows, filteredRows)
	fmt.Printf("Commit success: %v\n", result.Success)
	for _, t := range result.Tables {
		fmt.Printf("  %s: success=%v, files=%d, rows=%d\n",
			t.Name, t.Success, t.FilesAdded, t.RowsAdded)
	}
}
