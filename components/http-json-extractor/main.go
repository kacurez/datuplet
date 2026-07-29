// Package main is the HTTP JSON Extractor component: fetches JSON arrays
// from HTTP endpoints (with optional pagination) and writes them to the
// data lake via the DataGateway.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"

	sdk "github.com/datuplet/datuplet/sdk/go"
	dgarrow "github.com/datuplet/datuplet/sdk/go/arrow"
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

// commitAndStatus commits all outputs, logs per-table results, and emits the
// status message. Iterates result.Buckets (no [0] indexing).
func commitAndStatus(ctx context.Context, client *sdk.Client, sourceURL string) error {
	result, err := client.Commit(ctx)
	if err != nil {
		return fmt.Errorf("commit failed: %w", err)
	}
	if !result.Success {
		return fmt.Errorf("commit returned failure")
	}
	var rows int64
	for _, b := range result.Buckets {
		for _, t := range b.Tables {
			client.Log(ctx, "INFO", fmt.Sprintf("Committed %s.%s: files=%d, rows=%d", t.Bucket, t.Table, t.FilesAdded, t.RowsAdded))
			rows += t.RowsAdded
		}
	}
	sdk.StatusMessage(fmt.Sprintf("extracted %d records from %s", rows, sourceURL))
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

	// Log SDK build info first (rebuild diagnostics), then the started line.
	client.Log(ctx, "INFO", sdk.BuildInfo().String())

	cfg := client.Config()
	client.Log(ctx, "INFO", fmt.Sprintf("HTTP JSON Extractor started: execution=%s", cfg.ExecutionID))

	// Parse + validate component config before any network or writer work.
	var compCfg Config
	if err := client.ParseConfig(&compCfg); err != nil {
		sdk.ExitUserError(fmt.Sprintf("failed to parse config: %v", err))
	}
	if err := ParseAndValidate(&compCfg); err != nil {
		sdk.ExitUserError(err.Error())
	}

	outputTable := resolveOutputTable(compCfg.TableName, compCfg.ArrayPath)

	// Paginated mode - stream data incrementally, page by page.
	if compCfg.Pagination != nil && compCfg.Pagination.Type != "" {
		if err := runPaginatedExtraction(ctx, client, &compCfg, outputTable); err != nil {
			var we *dgarrow.WriteError
			if errors.As(err, &we) {
				sdk.ExitAppError(fmt.Sprintf("paginated extraction failed: %v", err))
			}
			sdk.ExitUserError(fmt.Sprintf("paginated extraction failed: %v", err))
		}
		return
	}

	// Single-request mode.
	client.Log(ctx, "INFO", fmt.Sprintf("Fetching JSON from: %s", compCfg.URL))

	sink := newExtractorSink(ctx, client, outputTable, compCfg.Fields)

	body, err := fetchStream(ctx, compCfg.URL, compCfg.Headers)
	if err != nil {
		finishQuietly(sink) // sink may already own an open writer; os.Exit skips defers
		sdk.ExitUserError(fmt.Sprintf("failed to fetch JSON: %v", err))
	}
	n, err := decodeRecords(body, compCfg.ArrayPath, sink.Add)
	body.Close()
	if err != nil {
		finishQuietly(sink)
		var we *dgarrow.WriteError
		if errors.As(err, &we) {
			sdk.ExitAppError(err.Error())
		}
		sdk.ExitUserError(fmt.Sprintf("failed to fetch JSON: %v", err))
	}
	client.Log(ctx, "INFO", fmt.Sprintf("Fetched %d records", n))

	rows, closeResult, err := sink.Finish()
	if err != nil {
		sdk.ExitAppError(err.Error())
	}
	if rows == 0 {
		client.Log(ctx, "WARN", "No records found")
		if _, err := client.Commit(ctx); err != nil {
			sdk.ExitAppError(fmt.Sprintf("commit failed: %v", err))
		}
		sdk.StatusMessage("extracted 0 records (empty response)")
		return
	}
	client.Log(ctx, "INFO", fmt.Sprintf("Completed output %s.%s: %d rows", sink.Writer().Bucket(), sink.Writer().Table(), closeResult.TotalRows))
	if keys, dropped := sink.UnknownKeys(); dropped > 0 {
		client.Log(ctx, "WARN", fmt.Sprintf("output schema was fixed from the first %d records; %d later record(s) carried field(s) missing from that schema and those values were NOT written: %v", dgarrow.DefaultBatchRows, dropped, keys))
	}

	if err := commitAndStatus(ctx, client, compCfg.URL); err != nil {
		sdk.ExitAppError(err.Error())
	}
	client.Log(ctx, "INFO", "HTTP JSON extraction completed successfully")
}

// runPaginatedExtraction handles paginated API extraction with streaming writes.
func runPaginatedExtraction(ctx context.Context, client *sdk.Client, cfg *Config, outputTable string) error {
	pagination := cfg.Pagination
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

	// One sink across all pages: the schema is fixed after the first batch
	// and every batch ships as its own IPC stream/POST.
	sink := newExtractorSink(ctx, client, outputTable, cfg.Fields)
	// Unlike main (which os.Exit's and thus skips defers), this function
	// returns errors, so this defer genuinely runs on every early return —
	// closing any writer an earlier page already opened before a later
	// page's failure aborts the run. Finish is idempotent, so this is a
	// no-op once the normal tail below has already called it.
	defer finishQuietly(sink)

	client.Log(ctx, "INFO", fmt.Sprintf("Starting paginated extraction from: %s (type=%s, param=%s, page_size=%d)",
		cfg.URL, pagination.Type, pagination.Param, pagination.PageSize))

	totalRecords := 0
	pageCount := 0

	for {
		// Check max pages limit
		if pagination.MaxPages > 0 && pageCount >= pagination.MaxPages {
			client.Log(ctx, "INFO", fmt.Sprintf("Reached max pages limit: %d", pagination.MaxPages))
			break
		}

		// Build paginated URL
		pageURL, err := buildPaginatedURL(cfg.URL, pagination, currentValue)
		if err != nil {
			return fmt.Errorf("failed to build paginated URL: %w", err)
		}

		client.Log(ctx, "INFO", fmt.Sprintf("Fetching page %d: %s", pageCount+1, pageURL))

		// Fetch + stream-decode the page, feeding the sink. Every decoded
		// object counts toward pageObjects (for empty/partial-page detection)
		// but only records under the max_records cap are written.
		body, err := fetchStream(ctx, pageURL, cfg.Headers)
		if err != nil {
			return fmt.Errorf("failed to fetch page %d: %w", pageCount+1, err)
		}
		truncated := false
		pageObjects, err := decodeRecords(body, cfg.ArrayPath, func(rec map[string]any) error {
			if pagination.MaxRecords > 0 && totalRecords >= pagination.MaxRecords {
				truncated = true
				return nil // keep counting the page; stop writing
			}
			if err := sink.Add(rec); err != nil {
				return err
			}
			totalRecords++
			return nil
		})
		body.Close()
		if err != nil {
			return fmt.Errorf("failed to fetch page %d: %w", pageCount+1, err)
		}
		if truncated {
			client.Log(ctx, "INFO", fmt.Sprintf("Truncating to max records limit: %d", pagination.MaxRecords))
		}

		// Check if we should stop
		if pageObjects == 0 {
			if pagination.StopWhenEmpty {
				client.Log(ctx, "INFO", "Received empty page, stopping pagination")
				break
			}
		}

		pageCount++
		client.Log(ctx, "INFO", fmt.Sprintf("Page %d: fetched %d records (total: %d)", pageCount, pageObjects, totalRecords))

		// Check if we've hit max records
		if pagination.MaxRecords > 0 && totalRecords >= pagination.MaxRecords {
			client.Log(ctx, "INFO", fmt.Sprintf("Reached max records limit: %d", pagination.MaxRecords))
			break
		}

		// Stop if this page had fewer records than page_size (likely last page)
		if pagination.PageSize > 0 && pageObjects < pagination.PageSize {
			client.Log(ctx, "INFO", "Received partial page, stopping pagination")
			break
		}

		// Move to next page
		currentValue += increment
	}

	// Flush + close (no-op writer when zero records; commit still runs).
	rows, closeResult, err := sink.Finish()
	if err != nil {
		return err // already a *dgarrow.WriteError → main exits FailedApplication
	}
	if rows > 0 {
		client.Log(ctx, "INFO", fmt.Sprintf("Completed output %s.%s: %d rows", sink.Writer().Bucket(), sink.Writer().Table(), closeResult.TotalRows))
	} else {
		// Lazy open means zero rows never opened a writer, so there is no
		// bucket.table to name; log completion against the configured
		// output table instead of fabricating a bucket.
		client.Log(ctx, "INFO", fmt.Sprintf("Completed output %s: 0 rows", outputTable))
	}
	if keys, dropped := sink.UnknownKeys(); dropped > 0 {
		client.Log(ctx, "WARN", fmt.Sprintf("output schema was fixed from the first %d records; %d later record(s) carried field(s) missing from that schema and those values were NOT written: %v", dgarrow.DefaultBatchRows, dropped, keys))
	}

	if err := commitAndStatus(ctx, client, cfg.URL); err != nil {
		return &dgarrow.WriteError{Err: err}
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
