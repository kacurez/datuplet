// Package tokens owns run-scoped JWT minting and JWKS publishing for
// pipeline-api.
package tokens

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// Signer holds the private key used to mint tokens and the KeyID that labels
// the corresponding JWK in the published JWKS.
type Signer struct {
	priv  *rsa.PrivateKey
	KeyID string
}

// Private returns the underlying RSA private key. Exposed for tests.
func (s *Signer) Private() *rsa.PrivateKey {
	return s.priv
}

// Public returns the RSA public key derived from the private key. The
// /auth/jwks.json endpoint publishes a JWK for this key.
func (s *Signer) Public() *rsa.PublicKey {
	return &s.priv.PublicKey
}

// LoadPrivateKeyFromPEMFile reads the PEM-encoded RSA private key at path
// and wraps it in a Signer labeled by keyID. Accepts PKCS#8 ("PRIVATE KEY")
// and PKCS#1 ("RSA PRIVATE KEY") blocks.
func LoadPrivateKeyFromPEMFile(path, keyID string) (*Signer, error) {
	if keyID == "" {
		return nil, errors.New("keyID is required")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read key: %w", err)
	}
	block, _ := pem.Decode(data)
	if block == nil {
		return nil, fmt.Errorf("decode PEM: no block found in %s", path)
	}

	var priv *rsa.PrivateKey
	switch block.Type {
	case "RSA PRIVATE KEY":
		priv, err = x509.ParsePKCS1PrivateKey(block.Bytes)
	case "PRIVATE KEY":
		var k any
		k, err = x509.ParsePKCS8PrivateKey(block.Bytes)
		if err == nil {
			var ok bool
			priv, ok = k.(*rsa.PrivateKey)
			if !ok {
				return nil, fmt.Errorf("key is not RSA (got %T)", k)
			}
		}
	default:
		return nil, fmt.Errorf("unsupported PEM block type %q", block.Type)
	}
	if err != nil {
		return nil, fmt.Errorf("parse key: %w", err)
	}
	return &Signer{priv: priv, KeyID: keyID}, nil
}

// EnsureSigningKey loads the RS256 private key at privPath, generating a
// fresh 2048-bit RSA keypair and persisting both PEMs (private at
// privPath, public at pubPath) when neither file exists. Returns a
// Signer wrapping the on-disk private key.
//
// Used by pipeline-api local mode to bootstrap a stable signing key
// under <dir>/signing-key.pem on first boot, so existing run tokens
// stay verifiable across restarts. The cluster path uses
// `pipeline-api admin keygen` to materialise the key explicitly and
// then references it via SIGNING_KEY_FILE — no auto-generation.
//
// Behaviour:
//   - Both PEMs exist: load and return.
//   - Both PEMs missing: generate, atomically write (.tmp+rename)
//     with mode 0400 (priv) / 0444 (pub), then load.
//   - Exactly one PEM exists: error. A half-written keypair is a
//     red flag (interrupted previous boot); refuse to silently
//     regenerate the missing half — surface so the operator can
//     decide.
//
// On error the returned *Signer is nil. Caller must hold an exclusive
// lock on the parent directory before calling so concurrent boots
// can't both generate at once; pipeline-api's flock on
// state.db.lock provides this for the local-mode call site.
func EnsureSigningKey(privPath, pubPath, keyID string) (*Signer, error) {
	if privPath == "" || pubPath == "" {
		return nil, errors.New("privPath and pubPath are required")
	}
	if privPath == pubPath {
		return nil, errors.New("privPath and pubPath must point to different files")
	}
	if keyID == "" {
		return nil, errors.New("keyID is required")
	}

	privExists, err := fileExists(privPath)
	if err != nil {
		return nil, fmt.Errorf("stat priv: %w", err)
	}
	pubExists, err := fileExists(pubPath)
	if err != nil {
		return nil, fmt.Errorf("stat pub: %w", err)
	}

	switch {
	case privExists && pubExists:
		return LoadPrivateKeyFromPEMFile(privPath, keyID)
	case privExists != pubExists:
		return nil, fmt.Errorf("inconsistent signing-key state: priv=%v pub=%v at %s / %s — refusing to silently regenerate; remove the orphan to retry", privExists, pubExists, privPath, pubPath)
	}

	// Both missing — generate and atomically persist.
	if err := os.MkdirAll(filepath.Dir(privPath), 0o755); err != nil {
		return nil, fmt.Errorf("mkdir for priv: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(pubPath), 0o755); err != nil {
		return nil, fmt.Errorf("mkdir for pub: %w", err)
	}

	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, fmt.Errorf("generate: %w", err)
	}
	privDER, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		return nil, fmt.Errorf("marshal priv: %w", err)
	}
	pubDER, err := x509.MarshalPKIXPublicKey(&priv.PublicKey)
	if err != nil {
		return nil, fmt.Errorf("marshal pub: %w", err)
	}

	privTmp := privPath + ".tmp"
	pubTmp := pubPath + ".tmp"
	if err := os.WriteFile(privTmp, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: privDER}), 0o400); err != nil {
		return nil, fmt.Errorf("write priv.tmp: %w", err)
	}
	if err := os.WriteFile(pubTmp, pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: pubDER}), 0o444); err != nil {
		_ = os.Remove(privTmp)
		return nil, fmt.Errorf("write pub.tmp: %w", err)
	}
	if err := os.Rename(privTmp, privPath); err != nil {
		_ = os.Remove(privTmp)
		_ = os.Remove(pubTmp)
		return nil, fmt.Errorf("rename priv: %w", err)
	}
	if err := os.Rename(pubTmp, pubPath); err != nil {
		_ = os.Remove(pubTmp)
		return nil, fmt.Errorf("rename pub: %w", err)
	}

	return LoadPrivateKeyFromPEMFile(privPath, keyID)
}

// fileExists returns (true, nil) if path exists, (false, nil) if it
// does not, and a non-nil error for any other stat failure (permission
// denied, etc.). Lets EnsureSigningKey distinguish "absent" from
// "unreadable".
func fileExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}
