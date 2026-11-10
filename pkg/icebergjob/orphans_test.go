package icebergjob

import (
	"errors"
	"strings"
	"testing"
)

func TestObjectKey(t *testing.T) {
	for in, want := range map[string]string{
		"s3://b/k":                        "k",
		"gs://b/k":                        "k",
		"s3a://b/k":                       "k",
		"k":                               "k",
		"/k":                              "k",
		"s3://bucket/wh/t/data/a.parquet": "wh/t/data/a.parquet",
		"wh/t/data/a.parquet":             "wh/t/data/a.parquet",
	} {
		if got := objectKey(in); got != want {
			t.Errorf("objectKey(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestLiveFileGuard_BareKeyListing(t *testing.T) {
	live := map[string]struct{}{objectKey("s3://bucket/wh/t/data/a.parquet"): {}}
	err := liveFileGuard([]string{"wh/t/data/orphan.parquet", "wh/t/data/a.parquet"}, live)
	if !errors.Is(err, ErrRefused) || !strings.Contains(err.Error(), "wh/t/data/a.parquet") {
		t.Fatalf("err = %v, want ErrRefused naming the live file", err)
	}
}

func TestLiveFileGuard_SchemeMismatchListing(t *testing.T) {
	live := map[string]struct{}{objectKey("s3://bucket/wh/t/metadata/00001.metadata.json"): {}}
	err := liveFileGuard([]string{"s3a://bucket/wh/t/metadata/00001.metadata.json"}, live)
	if !errors.Is(err, ErrRefused) {
		t.Fatalf("err = %v, want ErrRefused", err)
	}
}

func TestLiveFileGuard_CleanPlan(t *testing.T) {
	live := map[string]struct{}{objectKey("s3://bucket/wh/t/data/a.parquet"): {}}
	if err := liveFileGuard([]string{"s3://bucket/wh/t/data/orphan.parquet"}, live); err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
}
