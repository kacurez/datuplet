package tokens

import (
	"crypto/rsa"
	"encoding/base64"
	"encoding/binary"
)

// JWKS is the top-level JSON Web Key Set document at /auth/jwks.json.
type JWKS struct {
	Keys []JWK `json:"keys"`
}

// JWK is a single public-key entry. We only emit RS256 RSA keys, so the
// struct omits EC-specific fields.
type JWK struct {
	Kty string `json:"kty"` // "RSA"
	Use string `json:"use"` // "sig"
	Alg string `json:"alg"` // "RS256"
	Kid string `json:"kid"` // key id
	N   string `json:"n"`   // base64url(modulus)
	E   string `json:"e"`   // base64url(public exponent)
}

// JWKSFromPublicKey renders pub as a single-key JWKS with the given kid.
// All fields are base64url-encoded (no padding) per the JWK specification.
func JWKSFromPublicKey(pub *rsa.PublicKey, kid string) JWKS {
	if pub == nil || pub.N == nil {
		return JWKS{}
	}
	nBytes := pub.N.Bytes()
	// Encode E as the minimal big-endian byte representation.
	eBytes := encodeExponent(pub.E)
	return JWKS{
		Keys: []JWK{{
			Kty: "RSA",
			Use: "sig",
			Alg: "RS256",
			Kid: kid,
			N:   base64.RawURLEncoding.EncodeToString(nBytes),
			E:   base64.RawURLEncoding.EncodeToString(eBytes),
		}},
	}
}

// encodeExponent returns the minimal big-endian representation of e, which
// for RSA is almost always 65537 (3 bytes: 0x01 0x00 0x01). Trims leading
// zero bytes so the encoding matches what JWK consumers expect.
func encodeExponent(e int) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(e))
	// Trim leading zero bytes.
	i := 0
	for i < len(buf)-1 && buf[i] == 0 {
		i++
	}
	return buf[i:]
}
