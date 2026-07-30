package sdk

import "testing"

// The gateway populates ONLY the per-table Error for a per-table commit
// failure (server_v2_commit.go builds TableCommitResult.Error and leaves the
// response- and bucket-level Error fields empty). Reading CommitResult.Error
// alone therefore yields "" and loses the real reason — that is the exact bug
// FailureDetail exists to prevent, so the table-only case is the load-bearing
// one here.
func TestCommitResult_FailureDetail(t *testing.T) {
	tests := []struct {
		name string
		in   *CommitResult
		want string
	}{
		{
			name: "nil receiver",
			in:   nil,
			want: "",
		},
		{
			name: "no errors anywhere",
			in: &CommitResult{Success: true, Buckets: []BucketResult{
				{Bucket: "raw", Tables: []TableResult{{Bucket: "raw", Table: "t", Success: true}}},
			}},
			want: "",
		},
		{
			name: "table-level only (what the gateway actually sends)",
			in: &CommitResult{Buckets: []BucketResult{{
				Bucket: "raw",
				Tables: []TableResult{{Bucket: "raw", Table: "gbif", Error: "iceberg: schema mismatch"}},
			}}},
			want: "raw.gbif: iceberg: schema mismatch",
		},
		{
			name: "top-level only (sweep error)",
			in:   &CommitResult{Error: "sweep close raw.t: boom"},
			want: "sweep close raw.t: boom",
		},
		{
			name: "bucket-level only",
			in: &CommitResult{Buckets: []BucketResult{
				{Bucket: "raw", Error: "bucket exploded"},
			}},
			want: "bucket raw: bucket exploded",
		},
		{
			name: "all three levels are joined, outermost first",
			in: &CommitResult{
				Error: "top",
				Buckets: []BucketResult{{
					Bucket: "raw",
					Error:  "mid",
					Tables: []TableResult{{Bucket: "raw", Table: "t", Error: "leaf"}},
				}},
			},
			want: "top; bucket raw: mid; raw.t: leaf",
		},
		{
			name: "multiple failing tables across buckets",
			in: &CommitResult{Buckets: []BucketResult{
				{Bucket: "raw", Tables: []TableResult{
					{Bucket: "raw", Table: "a", Error: "e1"},
					{Bucket: "raw", Table: "ok", Success: true},
					{Bucket: "raw", Table: "b", Error: "e2"},
				}},
			}},
			want: "raw.a: e1; raw.b: e2",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.in.FailureDetail(); got != tc.want {
				t.Errorf("FailureDetail() = %q, want %q", got, tc.want)
			}
		})
	}
}
