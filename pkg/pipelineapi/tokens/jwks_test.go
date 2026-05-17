package tokens_test

import (
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"math/big"
	"testing"

	"github.com/datuplet/datuplet/pkg/pipelineapi/tokens"
)

func TestJWKSFromPublicKey_Shape(t *testing.T) {
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("gen: %v", err)
	}
	jwks := tokens.JWKSFromPublicKey(&priv.PublicKey, "kid-a")
	if len(jwks.Keys) != 1 {
		t.Fatalf("Keys len = %d, want 1", len(jwks.Keys))
	}
	k := jwks.Keys[0]
	if k.Kty != "RSA" {
		t.Errorf("Kty = %q, want RSA", k.Kty)
	}
	if k.Alg != "RS256" {
		t.Errorf("Alg = %q, want RS256", k.Alg)
	}
	if k.Use != "sig" {
		t.Errorf("Use = %q, want sig", k.Use)
	}
	if k.Kid != "kid-a" {
		t.Errorf("Kid = %q, want kid-a", k.Kid)
	}
	if k.N == "" || k.E == "" {
		t.Errorf("N/E must be populated; got N=%q E=%q", k.N, k.E)
	}
}

func TestJWKSFromPublicKey_Roundtrip(t *testing.T) {
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("gen: %v", err)
	}

	jwks := tokens.JWKSFromPublicKey(&priv.PublicKey, "kid-b")
	// Marshal → unmarshal to check the JSON shape is stable.
	buf, err := json.Marshal(jwks)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out tokens.JWKS
	if err := json.Unmarshal(buf, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(out.Keys) != 1 || out.Keys[0].Kid != "kid-b" {
		t.Errorf("roundtrip failed: %+v", out)
	}

	// Decode N back and confirm it matches the original public-key modulus.
	nBytes, err := base64.RawURLEncoding.DecodeString(out.Keys[0].N)
	if err != nil {
		t.Fatalf("decode n: %v", err)
	}
	gotN := new(big.Int).SetBytes(nBytes)
	if gotN.Cmp(priv.PublicKey.N) != 0 {
		t.Error("decoded modulus does not match original")
	}

	// E is typically AQAB (65537). Decode and check.
	eBytes, err := base64.RawURLEncoding.DecodeString(out.Keys[0].E)
	if err != nil {
		t.Fatalf("decode e: %v", err)
	}
	gotE := new(big.Int).SetBytes(eBytes).Int64()
	if int(gotE) != priv.PublicKey.E {
		t.Errorf("E mismatch: got %d, want %d", gotE, priv.PublicKey.E)
	}
}

func TestJWKSFromPublicKey_NilKey(t *testing.T) {
	got := tokens.JWKSFromPublicKey(nil, "kid")
	if len(got.Keys) != 0 {
		t.Errorf("nil pub: got %+v, want empty", got)
	}
}

func TestJWKSFromPublicKey_NilN(t *testing.T) {
	got := tokens.JWKSFromPublicKey(&rsa.PublicKey{}, "kid")
	if len(got.Keys) != 0 {
		t.Errorf("nil N: got %+v, want empty", got)
	}
}
