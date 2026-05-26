package datagateway

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/apache/iceberg-go/catalog/rest"
)

func TestClassifyCommitError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{"nil", nil, ""},
		{"context.Canceled", context.Canceled, "cancelled"},
		{"wrapped context.Canceled", fmt.Errorf("wrap: %w", context.Canceled), "cancelled"},
		{"context.DeadlineExceeded", context.DeadlineExceeded, "timeout"},
		{"wrapped context.DeadlineExceeded", fmt.Errorf("wrap: %w", context.DeadlineExceeded), "timeout"},
		{"commit conflict (rest.ErrCommitFailed)", rest.ErrCommitFailed, "conflict"},
		{"wrapped commit conflict", fmt.Errorf("commit table foo.bar: %w", rest.ErrCommitFailed), "conflict"},
		{"arbitrary error", errors.New("something unexpected"), "other"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := classifyCommitError(tc.err)
			if got != tc.want {
				t.Errorf("classifyCommitError(%v) = %q, want %q", tc.err, got, tc.want)
			}
		})
	}
}
