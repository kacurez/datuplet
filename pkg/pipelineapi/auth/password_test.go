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
	for _, bad := range []string{
		"",
		"not-a-phc",
		"$argon2id$v=19$", // truncated
		"$scrypt$foo",     // wrong algorithm
	} {
		ok, err := auth.VerifyPassword("any", bad)
		if err == nil {
			t.Errorf("malformed hash %q should return an error; got ok=%v", bad, ok)
		}
	}
}

func TestVerify_RejectsZeroTime(t *testing.T) {
	// Craft a PHC with valid shape but t=0, which would panic inside
	// x/crypto/argon2 if passed through. Salt and hash use 4-byte placeholder
	// data encoded with RawStdEncoding.
	malformed := "$argon2id$v=19$m=65536,t=0,p=4$AAAAAA$AAAAAA"
	ok, err := auth.VerifyPassword("anything", malformed)
	if err == nil {
		t.Errorf("t=0 hash must return an error, not panic; got ok=%v", ok)
	}
}

func TestVerify_RejectsZeroThreads(t *testing.T) {
	malformed := "$argon2id$v=19$m=65536,t=1,p=0$AAAAAA$AAAAAA"
	ok, err := auth.VerifyPassword("anything", malformed)
	if err == nil {
		t.Errorf("p=0 hash must return an error; got ok=%v", ok)
	}
}

func TestVerify_RejectsEmptyHashSegment(t *testing.T) {
	// Empty hash (len(wantHash) == 0). RawStdEncoding of an empty string is "".
	malformed := "$argon2id$v=19$m=65536,t=1,p=4$AAAAAA$"
	ok, err := auth.VerifyPassword("anything", malformed)
	if err == nil {
		t.Errorf("empty hash segment must return an error; got ok=%v", ok)
	}
}
