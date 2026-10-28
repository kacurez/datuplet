package auth_test

import (
	"strings"
	"testing"

	"github.com/datuplet/datuplet/pkg/pipelineapi/auth"
)

func TestHash_RoundTrip(t *testing.T) {
	pw := "correct horse battery staple"
	h, err := auth.HashPassword(pw)
	if err != nil {
		t.Fatalf("HashPassword: %v", err)
	}
	if !strings.HasPrefix(h, "$argon2id$") {
		t.Errorf("hash %q does not have PHC prefix", h)
	}
	ok, err := auth.VerifyPassword(pw, h)
	if err != nil {
		t.Fatalf("VerifyPassword: %v", err)
	}
	if !ok {
		t.Error("correct password did not verify")
	}
}

func TestVerify_RejectsWrongPassword(t *testing.T) {
	h, _ := auth.HashPassword("original")
	ok, err := auth.VerifyPassword("different", h)
	if err != nil {
		t.Fatalf("VerifyPassword: %v", err)
	}
	if ok {
		t.Error("wrong password verified as correct")
	}
}

func TestHash_EachCallProducesDistinctHash(t *testing.T) {
	pw := "same password"
	a, _ := auth.HashPassword(pw)
	b, _ := auth.HashPassword(pw)
	if a == b {
		t.Error("two hashes of the same password are identical — salt is not random")
	}
}

func TestVerify_RejectsMalformedHash(t *testing.T) {
	cases := []struct{ name, hash string }{
		{"empty", ""},
		{"not_phc", "not-a-phc"},
		{"truncated", "$argon2id$v=19$"},
		{"wrong_algorithm", "$scrypt$foo"},
		// Valid shape but t=0, which would panic inside x/crypto/argon2 if
		// passed through. Salt and hash use 4-byte placeholder data encoded
		// with RawStdEncoding.
		{"zero_time", "$argon2id$v=19$m=65536,t=0,p=4$AAAAAA$AAAAAA"},
		{"zero_threads", "$argon2id$v=19$m=65536,t=1,p=0$AAAAAA$AAAAAA"},
		// Empty hash (len(wantHash) == 0). RawStdEncoding of "" is "".
		{"empty_hash_segment", "$argon2id$v=19$m=65536,t=1,p=4$AAAAAA$"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ok, err := auth.VerifyPassword("any", tc.hash)
			if err == nil {
				t.Errorf("malformed hash %q must return an error (not panic); got ok=%v", tc.hash, ok)
			}
		})
	}
}
