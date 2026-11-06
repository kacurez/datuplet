package main

import "testing"

func TestEffectiveRowsCount(t *testing.T) {
	tests := []struct {
		name    string
		limit   *Limit
		preview int64
		want    int
	}{
		{"no cap", &Limit{RowsCount: 5000}, 0, 5000},
		{"preview only", &Limit{TimeoutInSeconds: 10}, 1000, 1000},
		{"rows below preview", &Limit{RowsCount: 500}, 1000, 500},
		{"rows above preview", &Limit{RowsCount: 5000}, 1000, 1000},
		{"size-only limit capped", &Limit{SizeInBytes: 1 << 30}, 1000, 1000},
		{"nil limit, no preview", nil, 0, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := effectiveRowsCount(tt.limit, tt.preview); got != tt.want {
				t.Errorf("effectiveRowsCount = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestPreviewBatchRows(t *testing.T) {
	if got := previewBatchRows(0); got != 0 {
		t.Errorf("previewBatchRows(0) = %d, want 0", got)
	}
	if got := previewBatchRows(1000); got != 1000 {
		t.Errorf("previewBatchRows(1000) = %d, want 1000", got)
	}
	if got := previewBatchRows(1 << 20); got != 0 {
		t.Errorf("previewBatchRows(1<<20) = %d, want 0", got)
	}
}
