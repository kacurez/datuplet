package controllers

import "testing"

func TestIterTagFromImage(t *testing.T) {
	cases := []struct {
		name, image, want string
	}{
		{"ttl.sh iter form", "ttl.sh/datuplet-gateway-iter-abc1234:24h", "abc1234"},
		{"ttl.sh iter form with dirty suffix", "ttl.sh/datuplet-gateway-iter-abc1234-dirty:24h", "abc1234-dirty"},
		{"ghcr release form", "ghcr.io/kacurez/datuplet-gateway:v0.2.8", ""},
		{"bare image:latest", "datuplet/gateway:latest", ""},
		{"image without tag", "datuplet/gateway", ""},
		{"empty string", "", ""},
		{"iter suffix in middle (not at end)", "ttl.sh/something-iter-x-final:24h", "x-final"},
		{"registry with port and tag", "localhost:5000/datuplet-gateway-iter-abc1234:24h", "abc1234"},
		{"registry with port no tag", "localhost:5000/datuplet-gateway-iter-abc1234", "abc1234"},
		{"digest-pinned iter ref", "ttl.sh/datuplet-gateway-iter-abc1234@sha256:deadbeef", "abc1234"},
		{"service name contains iter substring", "ttl.sh/datuplet-iter-extractor-iter-abc1234:24h", "abc1234"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := iterTagFromImage(tc.image)
			if got != tc.want {
				t.Errorf("iterTagFromImage(%q) = %q, want %q", tc.image, got, tc.want)
			}
		})
	}
}
