package main

import "testing"

func TestEffectiveRecordCap(t *testing.T) {
	tests := []struct {
		maxRecords int
		preview    int64
		want       int
	}{
		{0, 0, 0},
		{0, 1000, 1000},
		{500, 1000, 500},
		{5000, 1000, 1000},
		{5000, 0, 5000},
	}
	for _, tt := range tests {
		if got := effectiveRecordCap(tt.maxRecords, tt.preview); got != tt.want {
			t.Errorf("effectiveRecordCap(%d, %d) = %d, want %d", tt.maxRecords, tt.preview, got, tt.want)
		}
	}
}

func TestSinkBatchRows(t *testing.T) {
	if got := sinkBatchRows(0); got != 0 {
		t.Errorf("sinkBatchRows(0) = %d, want 0 (use default)", got)
	}
	if got := sinkBatchRows(1000); got != 1000 {
		t.Errorf("sinkBatchRows(1000) = %d, want 1000", got)
	}
	if got := sinkBatchRows(1 << 20); got != 0 {
		t.Errorf("sinkBatchRows(1<<20) = %d, want 0 (default is smaller)", got)
	}
}
