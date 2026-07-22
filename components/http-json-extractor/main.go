// Package main is the HTTP JSON Extractor component: fetches JSON arrays
// from HTTP endpoints (with optional pagination) and writes them to the
// data lake via the DataGateway.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"

	pb "github.com/datuplet/datuplet/pkg/datagateway/proto/v2"
	sdk "github.com/datuplet/datuplet/sdk/go"
)

// PaginationConfig defines how to paginate through API results.
type PaginationConfig struct {
	Type          string `json:"type"`            // "page" or "offset"
	Param         string `json:"param"`           // query parameter name (e.g., "page", "offset")
	Start         int    `json:"start"`           // starting value (default: 1 for page, 0 for offset)
	Increment     int    `json:"increment"`       // increment per page (default: 1 for page, page_size for offset)
	PageSize      int    `json:"page_size"`       // results per page
	SizeParam     string `json:"size_param"`      // query parameter for page size (optional)
	MaxPages      int    `json:"max_pages"`       // max pages to fetch (0 = unlimited)
	MaxRecords    int    `json:"max_records"`     // max total records to fetch (0 = unlimited)
	StopWhenEmpty bool   `json:"stop_when_empty"` // stop when empty page received (default: true)
}

// FieldMapping selects a source value (by dot-path) and renames it to an
// output column. Used by the optional `fields` projection.
type FieldMapping struct {
	Path string `json:"path"`
	Name string `json:"name"`
}

// Config is the http-json-extractor component config.
type Config struct {
	URL        string            `json:"url"`
	ArrayPath  string            `json:"array_path"`
	TableName  string            `json:"table_name"`
	Headers    map[string]string `json:"headers"`
	Pagination *PaginationConfig `json:"pagination"`
	Fields     []FieldMapping    `json:"fields"`
}

// columnNameRe validates author-controlled projected output-column names.
var columnNameRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,127}$`)

// ParseAndValidate checks the config before any network or writer work.
func ParseAndValidate(cfg *Config) error {
	if cfg.URL == "" {
		return fmt.Errorf("config.url is required")
	}
	seen := make(map[string]bool, len(cfg.Fields))
	for i, f := range cfg.Fields {
		if f.Path == "" {
			return fmt.Errorf("fields[%d].path is required", i)
		}
		if !columnNameRe.MatchString(f.Name) {
			return fmt.Errorf("fields[%d].name %q must match ^[A-Za-z_][A-Za-z0-9_]{0,127}$", i, f.Name)
		}
		if seen[f.Name] {
			return fmt.Errorf("fields[%d].name %q is duplicated", i, f.Name)
		}
		seen[f.Name] = true
	}
	return nil
}

func main() {
	// Check for sample mode
	if os.Getenv("DATUPLET_MODE") == "sample" {
		if err := runSampleMode(); err != nil {
			fmt.Fprintf(os.Stderr, "Sample mode error: %v\n", err)
			os.Exit(1)
		}
		return
	}

	ctx := context.Background()

	// Connect to gateway
	client, err := sdk.New(ctx)
	if err != nil {
		sdk.ExitAppError(fmt.Sprintf("failed to connect to gateway: %v", err))
	}
	defer client.Close()

	// Get config
	cfg := client.Config()
	client.Log(ctx, "INFO", fmt.Sprintf("HTTP JSON Extractor started: execution=%s", cfg.ExecutionID))

	// Parse component config
	var compCfg struct {
		URL        string            `json:"url"`
		ArrayPath  string            `json:"array_path"`
		TableName  string            `json:"table_name"`
		Headers    map[string]string `json:"headers"`
		Pagination *PaginationConfig `json:"pagination"`
	}
	if err := client.ParseConfig(&compCfg); err != nil {
		sdk.ExitUserError(fmt.Sprintf("failed to parse config: %v", err))
	}

	if compCfg.URL == "" {
		sdk.ExitUserError("config.url is required")
	}

	// Check if pagination is enabled
	if compCfg.Pagination != nil && compCfg.Pagination.Type != "" {
		// Paginated mode - stream data incrementally. runPaginatedExtraction
		// opens its own writer and commits inline, so we must return here
		// rather than fall through to the single-request commit block below
		// (a second Commit returns zero buckets and the status line panics).
		if err := runPaginatedExtraction(ctx, client, compCfg.URL, compCfg.ArrayPath, compCfg.TableName, compCfg.Headers, compCfg.Pagination); err != nil {
			sdk.ExitUserError(fmt.Sprintf("paginated extraction failed: %v", err))
		}
		return
	} else {
		// Single request mode - original behavior
		client.Log(ctx, "INFO", fmt.Sprintf("Fetching JSON from: %s", compCfg.URL))

		records, err := fetchJSON(ctx, compCfg.URL, compCfg.ArrayPath, compCfg.Headers)
		if err != nil {
			sdk.ExitUserError(fmt.Sprintf("failed to fetch JSON: %v", err))
		}

		client.Log(ctx, "INFO", fmt.Sprintf("Fetched %d records", len(records)))

		if len(records) == 0 {
			client.Log(ctx, "WARN", "No records found")
			if _, err := client.Commit(ctx); err != nil {
				sdk.ExitAppError(fmt.Sprintf("commit failed: %v", err))
			}
			sdk.StatusMessage("extracted 0 records (empty response)")
			return
		}

		// Determine output table name: explicit config > array_path > "data"
		outputTable := "data"
		if compCfg.TableName != "" {
			outputTable = compCfg.TableName
		} else if compCfg.ArrayPath != "" {
			outputTable = compCfg.ArrayPath
		}

		client.Log(ctx, "INFO", fmt.Sprintf("Writing to output table: %s (bucket: %s)", outputTable, cfg.DefaultBucket))

		writer, err := client.OpenWriter(ctx, outputTable, sdk.WithFormat(pb.DataFormat_FORMAT_JSON))
		if err != nil {
			sdk.ExitAppError(fmt.Sprintf("failed to open writer: %v", err))
		}

		jsonData, err := json.Marshal(records)
		if err != nil {
			sdk.ExitAppError(fmt.Sprintf("failed to marshal JSON: %v", err))
		}

		if err := writer.Write(ctx, jsonData); err != nil {
			sdk.ExitAppError(fmt.Sprintf("failed to write JSON: %v", err))
		}

		closeResult, err := writer.Close(ctx)
		if err != nil {
			sdk.ExitAppError(fmt.Sprintf("failed to close writer: %v", err))
		}

		client.Log(ctx, "INFO", fmt.Sprintf("Completed output %s.%s: %d rows", writer.Bucket(), writer.Table(), closeResult.TotalRows))
	}

	// Commit all outputs
	result, err := client.Commit(ctx)
	if err != nil {
		sdk.ExitAppError(fmt.Sprintf("commit failed: %v", err))
	}

	if !result.Success {
		sdk.ExitAppError("commit returned failure")
	}

	for _, b := range result.Buckets {
		for _, t := range b.Tables {
			client.Log(ctx, "INFO", fmt.Sprintf("Committed %s.%s: files=%d, rows=%d", t.Bucket, t.Table, t.FilesAdded, t.RowsAdded))
		}
	}

	client.Log(ctx, "INFO", "HTTP JSON extraction completed successfully")
	var rowsAdded int64
	if len(result.Buckets) > 0 && len(result.Buckets[0].Tables) > 0 {
		rowsAdded = result.Buckets[0].Tables[0].RowsAdded
	}
	sdk.StatusMessage(fmt.Sprintf("extracted %d records from %s", rowsAdded, compCfg.URL))
}

// runPaginatedExtraction handles paginated API extraction with streaming writes.
func runPaginatedExtraction(ctx context.Context, client *sdk.Client, baseURL, arrayPath, tableName string, headers map[string]string, pagination *PaginationConfig) error {
	// Set defaults
	if pagination.StopWhenEmpty == false && pagination.MaxPages == 0 && pagination.MaxRecords == 0 {
		// Default to stop on empty if no other limits set
		pagination.StopWhenEmpty = true
	}

	// Determine start value
	currentValue := pagination.Start
	if pagination.Type == "page" && currentValue == 0 {
		currentValue = 1 // Pages typically start at 1
	}

	// Determine increment
	increment := pagination.Increment
	if increment == 0 {
		if pagination.Type == "offset" {
			increment = pagination.PageSize
		} else {
			increment = 1
		}
	}

	// Determine output table name: explicit config > array_path > "data"
	outputTable := "data"
	if tableName != "" {
		outputTable = tableName
	} else if arrayPath != "" {
		outputTable = arrayPath
	}

	// Open writer for output (uses defaultBucket from config)
	writer, err := client.OpenWriter(ctx, outputTable, sdk.WithFormat(pb.DataFormat_FORMAT_JSON))
	if err != nil {
		return fmt.Errorf("failed to open writer: %w", err)
	}

	client.Log(ctx, "INFO", fmt.Sprintf("Starting paginated extraction from: %s (type=%s, param=%s, page_size=%d)",
		baseURL, pagination.Type, pagination.Param, pagination.PageSize))

	totalRecords := 0
	pageCount := 0

	for {
		// Check max pages limit
		if pagination.MaxPages > 0 && pageCount >= pagination.MaxPages {
			client.Log(ctx, "INFO", fmt.Sprintf("Reached max pages limit: %d", pagination.MaxPages))
			break
		}

		// Build paginated URL
		pageURL, err := buildPaginatedURL(baseURL, pagination, currentValue)
		if err != nil {
			return fmt.Errorf("failed to build paginated URL: %w", err)
		}

		client.Log(ctx, "INFO", fmt.Sprintf("Fetching page %d: %s", pageCount+1, pageURL))

		// Fetch page
		records, err := fetchJSON(ctx, pageURL, arrayPath, headers)
		if err != nil {
			return fmt.Errorf("failed to fetch page %d: %w", pageCount+1, err)
		}

		// Check if we should stop
		if len(records) == 0 {
			if pagination.StopWhenEmpty {
				client.Log(ctx, "INFO", "Received empty page, stopping pagination")
				break
			}
		}

		// Check max records limit
		recordsToWrite := records
		if pagination.MaxRecords > 0 && totalRecords+len(records) > pagination.MaxRecords {
			remaining := pagination.MaxRecords - totalRecords
			recordsToWrite = records[:remaining]
			client.Log(ctx, "INFO", fmt.Sprintf("Truncating to max records limit: %d", pagination.MaxRecords))
		}

		// Write records to output
		if len(recordsToWrite) > 0 {
			jsonData, err := json.Marshal(recordsToWrite)
			if err != nil {
				return fmt.Errorf("failed to marshal JSON: %w", err)
			}

			if err := writer.Write(ctx, jsonData); err != nil {
				return fmt.Errorf("failed to write: %w", err)
			}
		}

		totalRecords += len(recordsToWrite)
		pageCount++

		client.Log(ctx, "INFO", fmt.Sprintf("Page %d: fetched %d records (total: %d)", pageCount, len(records), totalRecords))

		// Check if we've hit max records
		if pagination.MaxRecords > 0 && totalRecords >= pagination.MaxRecords {
			client.Log(ctx, "INFO", fmt.Sprintf("Reached max records limit: %d", pagination.MaxRecords))
			break
		}

		// Stop if this page had fewer records than page_size (likely last page)
		if pagination.PageSize > 0 && len(records) < pagination.PageSize {
			client.Log(ctx, "INFO", "Received partial page, stopping pagination")
			break
		}

		// Move to next page
		currentValue += increment
	}

	// Close writer
	closeResult, err := writer.Close(ctx)
	if err != nil {
		return fmt.Errorf("failed to close writer: %w", err)
	}
	client.Log(ctx, "INFO", fmt.Sprintf("Completed output %s.%s: %d rows", writer.Bucket(), writer.Table(), closeResult.TotalRows))

	// Commit
	result, err := client.Commit(ctx)
	if err != nil {
		return fmt.Errorf("commit failed: %w", err)
	}

	if !result.Success {
		return fmt.Errorf("commit returned failure")
	}

	for _, b := range result.Buckets {
		for _, t := range b.Tables {
			client.Log(ctx, "INFO", fmt.Sprintf("Committed %s.%s: files=%d, rows=%d", t.Bucket, t.Table, t.FilesAdded, t.RowsAdded))
		}
	}

	client.Log(ctx, "INFO", fmt.Sprintf("Paginated extraction completed: %d pages, %d total records", pageCount, totalRecords))
	return nil
}

// buildPaginatedURL constructs the URL for a specific page/offset value.
func buildPaginatedURL(baseURL string, pagination *PaginationConfig, value int) (string, error) {
	u, err := url.Parse(baseURL)
	if err != nil {
		return "", fmt.Errorf("invalid base URL: %w", err)
	}

	// Start with existing query parameters
	q := u.Query()

	// Add pagination parameter
	q.Set(pagination.Param, strconv.Itoa(value))

	// Add page size parameter if configured
	if pagination.SizeParam != "" && pagination.PageSize > 0 {
		q.Set(pagination.SizeParam, strconv.Itoa(pagination.PageSize))
	}

	// Encode and then unescape brackets (some APIs like Treasury expect raw brackets)
	encoded := q.Encode()
	encoded = strings.ReplaceAll(encoded, "%5B", "[")
	encoded = strings.ReplaceAll(encoded, "%5D", "]")
	u.RawQuery = encoded

	return u.String(), nil
}

// fetchJSON fetches and parses JSON from the given URL.
func fetchJSON(ctx context.Context, url, arrayPath string, headers map[string]string) ([]map[string]any, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	// Add headers
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("HTTP request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP error: %s", resp.Status)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	return parseJSON(body, arrayPath)
}

// parseJSON parses JSON body into records, handling both array and wrapped formats.
func parseJSON(body []byte, arrayPath string) ([]map[string]any, error) {
	// Bare array of record objects: [ {...}, {...} ]
	var records []map[string]any
	if err := json.Unmarshal(body, &records); err == nil {
		return records, nil
	}

	// Positional array whose records live in a nested array element, e.g. the
	// World Bank API's [ {..pagination metadata..}, [ {..records..} ] ]. A bare
	// record array never has a top-level element that is itself an array, so
	// this branch only changes behaviour for the positional shape.
	var positional []any
	if err := json.Unmarshal(body, &positional); err == nil {
		// Prefer a nested array of records over the top-level elements.
		for _, el := range positional {
			if inner, ok := el.([]any); ok {
				return recordsFromSlice(inner), nil
			}
		}
		// No nested array: fall back to the top-level object elements.
		return recordsFromSlice(positional), nil
	}

	// Object wrapping an array field: { "results": [ ... ] }.
	var wrapper map[string]any
	if err := json.Unmarshal(body, &wrapper); err != nil {
		return nil, fmt.Errorf("failed to parse JSON: %w", err)
	}

	// Find array in wrapper
	arrayKey := arrayPath
	if arrayKey == "" {
		// Auto-detect: look for first array field
		for k, v := range wrapper {
			if _, ok := v.([]any); ok {
				arrayKey = k
				break
			}
		}
	}

	if arrayKey == "" {
		return nil, fmt.Errorf("no array found in JSON response, specify array_path in config")
	}

	arrayData, ok := wrapper[arrayKey].([]any)
	if !ok {
		return nil, fmt.Errorf("field '%s' is not an array", arrayKey)
	}

	return recordsFromSlice(arrayData), nil
}

// recordsFromSlice converts a slice of decoded JSON values into records,
// keeping only the object-valued elements.
func recordsFromSlice(items []any) []map[string]any {
	records := make([]map[string]any, 0, len(items))
	for _, item := range items {
		if record, ok := item.(map[string]any); ok {
			records = append(records, record)
		}
	}
	return records
}

// resolveOutputTable applies the table_name > array_path > "data" precedence.
func resolveOutputTable(tableName, arrayPath string) string {
	switch {
	case tableName != "":
		return tableName
	case arrayPath != "":
		return arrayPath
	default:
		return "data"
	}
}

// encodeJSONL serializes records as newline-delimited JSON (one object per
// line). Concatenation-safe: appending two encodeJSONL outputs is still valid
// JSONL, so coalesced gateway batches parse correctly. HTML escaping is off so
// values like URLs with & are preserved verbatim.
func encodeJSONL(records []map[string]any) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	for _, rec := range records {
		if err := enc.Encode(rec); err != nil { // Encode appends a trailing '\n'
			return nil, fmt.Errorf("marshal record: %w", err)
		}
	}
	return buf.Bytes(), nil
}

// writeJSONL writes records to the gateway as one JSONL blob. Each Write()
// payload contains only whole, newline-terminated records (the SDK never
// splits a payload mid-record), keeping per-POST JSONL parsing safe.
func writeJSONL(ctx context.Context, w *sdk.Writer, records []map[string]any) error {
	if len(records) == 0 {
		return nil
	}
	data, err := encodeJSONL(records)
	if err != nil {
		return err
	}
	return w.Write(ctx, data)
}

// getColumns extracts sorted column names from records, flattening nested objects.
func getColumns(records []map[string]any) []string {
	columnSet := make(map[string]bool)

	for _, record := range records {
		collectColumns("", record, columnSet)
	}

	columns := make([]string, 0, len(columnSet))
	for col := range columnSet {
		columns = append(columns, col)
	}
	sort.Strings(columns)
	return columns
}

// collectColumns recursively collects column names, flattening nested objects.
func collectColumns(prefix string, data map[string]any, columns map[string]bool) {
	for k, v := range data {
		colName := k
		if prefix != "" {
			colName = prefix + "." + k
		}

		switch val := v.(type) {
		case map[string]any:
			// Flatten nested object
			collectColumns(colName, val, columns)
		case []any:
			// Skip arrays (or could serialize as JSON string)
			columns[colName] = true
		default:
			columns[colName] = true
		}
	}
}

// recordToCSV converts a record to a CSV line.
func recordToCSV(record map[string]any, columns []string) string {
	values := make([]string, len(columns))
	for i, col := range columns {
		val := getValue(record, col)
		values[i] = escapeCSV(val)
	}
	return strings.Join(values, ",") + "\n"
}

// getValue gets a value from a record, supporting dot notation for nested fields.
func getValue(record map[string]any, path string) string {
	parts := strings.Split(path, ".")
	var current any = record

	for _, part := range parts {
		if m, ok := current.(map[string]any); ok {
			current = m[part]
		} else {
			return ""
		}
	}

	return formatValue(current)
}

// formatValue converts a value to string for CSV output.
func formatValue(v any) string {
	if v == nil {
		return ""
	}
	switch val := v.(type) {
	case string:
		return val
	case float64:
		if val == float64(int64(val)) {
			return fmt.Sprintf("%d", int64(val))
		}
		return fmt.Sprintf("%g", val)
	case bool:
		return fmt.Sprintf("%t", val)
	case []any:
		// Serialize array as JSON
		b, _ := json.Marshal(val)
		return string(b)
	default:
		return fmt.Sprintf("%v", val)
	}
}

// escapeCSV escapes a value for CSV output.
func escapeCSV(s string) string {
	if strings.ContainsAny(s, ",\"\n\r") {
		return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
	}
	return s
}

// SampleOutput is the JSON output structure for sample mode.
type SampleOutput struct {
	Schema   []ColumnSchema   `json:"schema"`
	Sample   []map[string]any `json:"sample"`
	RowsRead int              `json:"rows_read"`
	Source   string           `json:"source"`
}

// ColumnSchema describes a column in the data.
type ColumnSchema struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// runSampleMode fetches sample data from URL and outputs JSON to stdout.
func runSampleMode() error {
	// Parse limit from env
	limit := 10
	if limitStr := os.Getenv("DATUPLET_SAMPLE_LIMIT"); limitStr != "" {
		if l, err := strconv.Atoi(limitStr); err == nil && l > 0 {
			limit = l
		}
	}

	// Get config from DATUPLET_CONFIG env var
	configJSON := os.Getenv("DATUPLET_CONFIG")
	if configJSON == "" {
		return fmt.Errorf("DATUPLET_CONFIG environment variable is required")
	}

	var compCfg struct {
		URL       string            `json:"url"`
		ArrayPath string            `json:"array_path"`
		Headers   map[string]string `json:"headers"`
	}
	if err := json.Unmarshal([]byte(configJSON), &compCfg); err != nil {
		return fmt.Errorf("failed to parse config: %w", err)
	}

	if compCfg.URL == "" {
		return fmt.Errorf("config.url is required")
	}

	// Fetch JSON data
	ctx := context.Background()
	records, err := fetchJSON(ctx, compCfg.URL, compCfg.ArrayPath, compCfg.Headers)
	if err != nil {
		return fmt.Errorf("failed to fetch JSON: %w", err)
	}

	// Limit records
	if len(records) > limit {
		records = records[:limit]
	}

	// Infer schema from sample data
	schema := inferJSONSchema(records)

	// Build output
	output := SampleOutput{
		Schema:   schema,
		Sample:   records,
		RowsRead: len(records),
		Source:   compCfg.URL,
	}

	// Output JSON to stdout
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(output)
}

// inferJSONSchema infers column types from JSON records.
func inferJSONSchema(records []map[string]any) []ColumnSchema {
	if len(records) == 0 {
		return nil
	}

	// Get column names
	columns := getColumns(records)

	// Infer types for each column
	schema := make([]ColumnSchema, len(columns))
	for i, col := range columns {
		schema[i] = ColumnSchema{
			Name: col,
			Type: inferColumnTypeFromJSON(records, col),
		}
	}

	return schema
}

// inferColumnTypeFromJSON infers the type of a column from JSON values.
func inferColumnTypeFromJSON(records []map[string]any, column string) string {
	hasInt := false
	hasFloat := false
	hasBool := false
	hasArray := false
	hasObject := false

	for _, record := range records {
		val := getValueRaw(record, column)
		if val == nil {
			continue
		}

		switch v := val.(type) {
		case bool:
			hasBool = true
		case float64:
			if v == float64(int64(v)) {
				hasInt = true
			} else {
				hasFloat = true
			}
		case []any:
			hasArray = true
		case map[string]any:
			hasObject = true
		default:
			// String or other
			return "string"
		}
	}

	if hasArray {
		return "array"
	}
	if hasObject {
		return "object"
	}
	if hasBool && !hasInt && !hasFloat {
		return "boolean"
	}
	if hasFloat {
		return "float"
	}
	if hasInt {
		return "integer"
	}
	return "string"
}

// getValueRaw gets a raw value from a record, supporting dot notation.
func getValueRaw(record map[string]any, path string) any {
	parts := strings.Split(path, ".")
	var current any = record

	for _, part := range parts {
		if m, ok := current.(map[string]any); ok {
			current = m[part]
		} else {
			return nil
		}
	}

	return current
}
