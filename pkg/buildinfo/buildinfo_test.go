package buildinfo

import "testing"

func TestMinorMismatch(t *testing.T) {
	cases := []struct {
		client, server string
		want           bool
	}{
		{"0.14.1", "0.14.3", false},
		{"0.14.0", "0.15.0", true},
		{"dev", "0.15.0", false},
		{"v0.14.0", "0.14.9", false},
		{"0.14", "0.15.2", true},
		{"garbage", "0.15.0", false},
		{"0.15.0", "", false},
	}
	for _, tc := range cases {
		if got := MinorMismatch(tc.client, tc.server); got != tc.want {
			t.Errorf("MinorMismatch(%q,%q) = %v, want %v", tc.client, tc.server, got, tc.want)
		}
	}
}

func TestInfoDefaults(t *testing.T) {
	b := Info()
	if b.Version != Version || b.Commit != Commit || b.Date != Date {
		t.Fatalf("Info() = %+v", b)
	}
}
