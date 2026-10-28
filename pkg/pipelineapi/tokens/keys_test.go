package tokens_test

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"os"
	"path/filepath"
	"testing"

	"github.com/datuplet/datuplet/pkg/pipelineapi/tokens"
)

func writeRSAPrivateKeyPEM(t *testing.T) (*rsa.PrivateKey, string) {
	t.Helper()
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	buf := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})
	dir := t.TempDir()
	path := filepath.Join(dir, "priv.pem")
	if err := os.WriteFile(path, buf, 0o400); err != nil {
		t.Fatalf("write: %v", err)
	}
	return priv, path
}

func TestLoadPrivateKeyFromPEMFile_OK(t *testing.T) {
	priv, path := writeRSAPrivateKeyPEM(t)

	signer, err := tokens.LoadPrivateKeyFromPEMFile(path, "key-1")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if signer.KeyID != "key-1" {
		t.Errorf("KeyID = %q, want key-1", signer.KeyID)
	}
	if signer.Private().N.Cmp(priv.N) != 0 {
		t.Error("loaded private key N does not match")
	}
}

func TestEnsureSigningKey_GeneratesOnFirstCall(t *testing.T) {
	dir := t.TempDir()
	priv := filepath.Join(dir, "signing-key.pem")
	pub := filepath.Join(dir, "signing-key.pub.pem")

	signer, err := tokens.EnsureSigningKey(priv, pub, "key-1")
	if err != nil {
		t.Fatalf("EnsureSigningKey: %v", err)
	}
	if signer == nil || signer.KeyID != "key-1" {
		t.Fatalf("signer = %v, KeyID = %q; want non-nil + key-1", signer, signer.KeyID)
	}
	// Mode 0400 on private key.
	st, err := os.Stat(priv)
	if err != nil {
		t.Fatalf("stat priv: %v", err)
	}
	if mode := st.Mode().Perm(); mode != 0o400 {
		t.Errorf("private key mode = %o, want 0400", mode)
	}
}

func TestEnsureSigningKey_ReusesExisting(t *testing.T) {
	dir := t.TempDir()
	priv := filepath.Join(dir, "signing-key.pem")
	pub := filepath.Join(dir, "signing-key.pub.pem")

	// First call generates.
	s1, err := tokens.EnsureSigningKey(priv, pub, "key-1")
	if err != nil {
		t.Fatalf("first EnsureSigningKey: %v", err)
	}
	// Snapshot file contents.
	priv1, err := os.ReadFile(priv)
	if err != nil {
		t.Fatalf("read priv: %v", err)
	}

	// Second call must reuse — file must be identical, modulus must match.
	s2, err := tokens.EnsureSigningKey(priv, pub, "key-1")
	if err != nil {
		t.Fatalf("second EnsureSigningKey: %v", err)
	}
	priv2, err := os.ReadFile(priv)
	if err != nil {
		t.Fatalf("read priv: %v", err)
	}
	if string(priv1) != string(priv2) {
		t.Error("private key file rewritten on second call (should reuse)")
	}
	if s1.Private().N.Cmp(s2.Private().N) != 0 {
		t.Error("loaded private key differs across calls")
	}
}

func TestEnsureSigningKey_HalfPresentRefuses(t *testing.T) {
	dir := t.TempDir()
	priv := filepath.Join(dir, "signing-key.pem")
	pub := filepath.Join(dir, "signing-key.pub.pem")
	// Drop only the public key file.
	if err := os.WriteFile(pub, []byte("orphan"), 0o444); err != nil {
		t.Fatalf("write orphan: %v", err)
	}
	if _, err := tokens.EnsureSigningKey(priv, pub, "key-1"); err == nil {
		t.Error("expected error when only one of priv/pub exists")
	}
}

func TestLoadPrivateKeyFromPEMFile_Rejects(t *testing.T) {
	cases := []struct {
		name string
		// setup returns the path and key ID to load.
		setup func(t *testing.T) (path, keyID string)
	}{
		{"missing_file", func(*testing.T) (string, string) { return "/nonexistent", "key-1" }},
		{"empty_key_id", func(t *testing.T) (string, string) {
			_, path := writeRSAPrivateKeyPEM(t)
			return path, ""
		}},
		{"bad_pem", func(t *testing.T) (string, string) {
			path := filepath.Join(t.TempDir(), "bad.pem")
			_ = os.WriteFile(path, []byte("not a PEM file"), 0o400)
			return path, "key-1"
		}},
		// EC key: should be rejected cleanly.
		{"non_rsa", func(t *testing.T) (string, string) {
			path := filepath.Join(t.TempDir(), "ec.pem")
			_ = os.WriteFile(path, []byte(`-----BEGIN PRIVATE KEY-----
MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgFq3lc0LiMjGn7+xt
bkH5+ghZ70vVZWX55W4oHX3Bf5mhRANCAAS+O1Dv7e1VVOQJhP5oUoCohJk2Mxft
/LCCLqvrcRQYuSU4tGErVcD+uaS82GpKkXt4mCYvrdNzN2Rlq/EPaYR/
-----END PRIVATE KEY-----
`), 0o400)
			return path, "key-1"
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path, keyID := tc.setup(t)
			if _, err := tokens.LoadPrivateKeyFromPEMFile(path, keyID); err == nil {
				t.Error("expected error")
			}
		})
	}
}
