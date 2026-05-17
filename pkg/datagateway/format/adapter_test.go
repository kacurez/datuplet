package format

import (
	"testing"
)

func TestDataFormatString(t *testing.T) {
	tests := []struct {
		format   DataFormat
		expected string
	}{
		{FormatUnknown, "unknown"},
		{FormatCSV, "csv"},
		{FormatJSON, "json"},
		{FormatJSONL, "jsonl"},
		{FormatParquet, "parquet"},
		{FormatArrowIPC, "arrow"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			if got := tt.format.String(); got != tt.expected {
				t.Errorf("DataFormat.String() = %v, want %v", got, tt.expected)
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

func TestDataFormatMimeType(t *testing.T) {
	tests := []struct {
		format   DataFormat
		expected string
	}{
		{FormatCSV, "text/csv"},
		{FormatJSON, "application/json"},
		{FormatJSONL, "application/x-ndjson"},
		{FormatParquet, "application/vnd.apache.parquet"},
		{FormatArrowIPC, "application/vnd.apache.arrow.stream"},
		{FormatUnknown, "application/octet-stream"},
	}

	for _, tt := range tests {
		t.Run(tt.format.String(), func(t *testing.T) {
			if got := tt.format.MimeType(); got != tt.expected {
				t.Errorf("DataFormat.MimeType() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestDataFormatExtension(t *testing.T) {
	tests := []struct {
		format   DataFormat
		expected string
	}{
		{FormatCSV, ".csv"},
		{FormatJSON, ".json"},
		{FormatJSONL, ".jsonl"},
		{FormatParquet, ".parquet"},
		{FormatArrowIPC, ".arrow"},
		{FormatUnknown, ""},
	}

	for _, tt := range tests {
		t.Run(tt.format.String(), func(t *testing.T) {
			if got := tt.format.Extension(); got != tt.expected {
				t.Errorf("DataFormat.Extension() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestDataFormatRoundTrip(t *testing.T) {
	formats := []DataFormat{
		FormatCSV,
		FormatJSON,
		FormatJSONL,
		FormatParquet,
		FormatArrowIPC,
	}

	for _, format := range formats {
		t.Run(format.String(), func(t *testing.T) {
			s := format.String()
			parsed := ParseDataFormat(s)
			if parsed != format {
				t.Errorf("Round trip failed: %v -> %q -> %v", format, s, parsed)
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

	// Should have CSV, JSON, JSONL adapters
	formats := r.Formats()
	if len(formats) < 3 {
		t.Errorf("DefaultRegistry should have at least 3 formats, got %d", len(formats))
	}

	// Verify each is accessible
	for _, format := range []DataFormat{FormatCSV, FormatJSON, FormatJSONL} {
		adapter, err := r.Get(format)
		if err != nil {
			t.Errorf("DefaultRegistry missing %s adapter: %v", format, err)
		}
		if adapter.Format() != format {
			t.Errorf("Adapter format mismatch: got %v, want %v", adapter.Format(), format)
		}
	}
}

func TestDefaultParseOptions(t *testing.T) {
	opts := DefaultParseOptions()

	if !opts.HasHeader {
		t.Error("Default HasHeader should be true")
	}
	if opts.Delimiter != ',' {
		t.Errorf("Default Delimiter should be ',', got %q", opts.Delimiter)
	}
}

func TestDefaultSerializeOptions(t *testing.T) {
	opts := DefaultSerializeOptions()

	if !opts.IncludeHeader {
		t.Error("Default IncludeHeader should be true")
	}
	if opts.Delimiter != ',' {
		t.Errorf("Default Delimiter should be ',', got %q", opts.Delimiter)
	}
	if opts.Pretty {
		t.Error("Default Pretty should be false")
	}
	if opts.NullString != "" {
		t.Errorf("Default NullString should be empty, got %q", opts.NullString)
	}
}
