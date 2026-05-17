package testdata_test

import (
	"strings"
	"testing"

	"github.com/datuplet/datuplet/pkg/pipelineapi/storage/testdata"
)

func TestGenerateRejectsRelativePath(t *testing.T) {
	err := testdata.GenerateAllErr("relative/dir")
	if err == nil {
		t.Fatal("want error for relative path, got nil")
	}
	if !strings.Contains(err.Error(), "absolute") {
		t.Errorf("want 'absolute' in error, got %v", err)
	}
}
