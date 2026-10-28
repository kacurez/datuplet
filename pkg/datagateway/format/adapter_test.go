package format

import (
	"testing"
)

func TestDataFormatProperties(t *testing.T) {
	tests := []struct {
		format    DataFormat
		str       string
		mimeType  string
		extension string
	}{
		{FormatUnknown, "unknown", "application/octet-stream", ""},
		{FormatCSV, "csv", "text/csv", ".csv"},
		{FormatJSON, "json", "application/json", ".json"},
		{FormatJSONL, "jsonl", "application/x-ndjson", ".jsonl"},
		{FormatParquet, "parquet", "application/vnd.apache.parquet", ".parquet"},
		{FormatArrowIPC, "arrow", "application/vnd.apache.arrow.stream", ".arrow"},
	}

	for _, tt := range tests {
		t.Run(tt.str, func(t *testing.T) {
			if got := tt.format.String(); got != tt.str {
				t.Errorf("DataFormat.String() = %v, want %v", got, tt.str)
			}
			if got := tt.format.MimeType(); got != tt.mimeType {
				t.Errorf("DataFormat.MimeType() = %v, want %v", got, tt.mimeType)
			}
			if got := tt.format.Extension(); got != tt.extension {
				t.Errorf("DataFormat.Extension() = %v, want %v", got, tt.extension)
			}
		})
	}
}

func TestParseDataFormat(t *testing.T) {
	tests := []struct {
		input    string
		expected DataFormat
	}{
		{"csv", FormatCSV},
		{"CSV", FormatCSV},
		{"json", FormatJSON},
		{"JSON", FormatJSON},
		{"jsonl", FormatJSONL},
		{"JSONL", FormatJSONL},
		{"jsonlines", FormatJSONL},
		{"ndjson", FormatJSONL},
		{"NDJSON", FormatJSONL},
		{"parquet", FormatParquet},
		{"PARQUET", FormatParquet},
		{"arrow", FormatArrowIPC},
		{"ARROW", FormatArrowIPC},
		{"ipc", FormatArrowIPC},
		{"IPC", FormatArrowIPC},
		{"unknown", FormatUnknown},
		{"invalid", FormatUnknown},
		{"", FormatUnknown},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			if got := ParseDataFormat(tt.input); got != tt.expected {
				t.Errorf("ParseDataFormat(%q) = %v, want %v", tt.input, got, tt.expected)
			}
		})
	}
}

func TestRegistry(t *testing.T) {
	r := NewRegistry()

	// Register adapters
	csvAdapter := NewCSVAdapter(nil, nil)
	jsonAdapter := NewJSONAdapter(nil, nil)

	r.Register(csvAdapter)
	r.Register(jsonAdapter)

	// Test Get
	t.Run("GetCSV", func(t *testing.T) {
		adapter, err := r.Get(FormatCSV)
		if err != nil {
			t.Errorf("Get(FormatCSV) error: %v", err)
		}
		if adapter.Format() != FormatCSV {
			t.Errorf("Got adapter format %v, want FormatCSV", adapter.Format())
		}
	})

	t.Run("GetJSON", func(t *testing.T) {
		adapter, err := r.Get(FormatJSON)
		if err != nil {
			t.Errorf("Get(FormatJSON) error: %v", err)
		}
		if adapter.Format() != FormatJSON {
			t.Errorf("Got adapter format %v, want FormatJSON", adapter.Format())
		}
	})

	t.Run("GetUnregistered", func(t *testing.T) {
		_, err := r.Get(FormatParquet)
		if err == nil {
			t.Error("Get(FormatParquet) should return error for unregistered format")
		}
	})

	// Test Formats
	t.Run("Formats", func(t *testing.T) {
		formats := r.Formats()
		if len(formats) != 2 {
			t.Errorf("Formats() returned %d formats, want 2", len(formats))
		}
	})
}

func TestDefaultRegistry(t *testing.T) {
	r := DefaultRegistry()

	// Should have CSV, JSON, JSONL, Arrow IPC and Parquet adapters
	formats := r.Formats()
	if len(formats) < 5 {
		t.Errorf("DefaultRegistry should have at least 5 formats, got %d", len(formats))
	}

	// Verify each is accessible
	for _, format := range []DataFormat{FormatCSV, FormatJSON, FormatJSONL, FormatArrowIPC, FormatParquet} {
		adapter, err := r.Get(format)
		if err != nil {
			t.Errorf("DefaultRegistry missing %s adapter: %v", format, err)
		}
		if adapter.Format() != format {
			t.Errorf("Adapter format mismatch: got %v, want %v", adapter.Format(), format)
		}
	}
}
