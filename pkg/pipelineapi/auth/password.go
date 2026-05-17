// Package auth handles pipeline-api authentication: password hashing,
// session management, HTTP middleware.
package auth

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"

	"golang.org/x/crypto/argon2"
)

// Argon2id parameters. OWASP 2024 recommendations — tuned for ~100ms on
// a typical dev laptop. Production operators can raise memoryKB or iterations
// in a follow-up without invalidating existing hashes (the PHC-encoded hash
// carries its own parameters).
const (
	argonTime    = 1
	argonMemory  = 64 * 1024 // 64 MiB
	argonThreads = 4
	argonKeyLen  = 32
	argonSaltLen = 16
)

// HashPassword returns a PHC-encoded argon2id hash of pw.
func HashPassword(pw string) (string, error) {
	salt := make([]byte, argonSaltLen)
	if _, err := rand.Read(salt); err != nil {
		return "", fmt.Errorf("rand: %w", err)
	}
	hash := argon2.IDKey([]byte(pw), salt, argonTime, argonMemory, argonThreads, argonKeyLen)
	encoded := fmt.Sprintf("$argon2id$v=%d$m=%d,t=%d,p=%d$%s$%s",
		argon2.Version,
		argonMemory, argonTime, argonThreads,
		base64.RawStdEncoding.EncodeToString(salt),
		base64.RawStdEncoding.EncodeToString(hash),
	)
	return encoded, nil
}

// VerifyPassword checks pw against a PHC-encoded hash. Returns (true, nil) on
// match, (false, nil) on mismatch, (false, err) on malformed input.
func VerifyPassword(pw, encoded string) (bool, error) {
	parts := strings.Split(encoded, "$")
	// Expected: ["", "argon2id", "v=19", "m=...,t=...,p=...", "<salt>", "<hash>"]
	if len(parts) != 6 || parts[0] != "" || parts[1] != "argon2id" {
		return false, errors.New("not a PHC argon2id hash")
	}

	var version int
	if _, err := fmt.Sscanf(parts[2], "v=%d", &version); err != nil {
		return false, fmt.Errorf("parse version: %w", err)
	}
	if version != argon2.Version {
		return false, fmt.Errorf("unsupported argon2 version %d", version)
	}

	var memory, time uint32
	var threads uint8
	if _, err := fmt.Sscanf(parts[3], "m=%d,t=%d,p=%d", &memory, &time, &threads); err != nil {
		return false, fmt.Errorf("parse params: %w", err)
	}

	salt, err := base64.RawStdEncoding.DecodeString(parts[4])
	if err != nil {
		return false, fmt.Errorf("decode salt: %w", err)
	}
	wantHash, err := base64.RawStdEncoding.DecodeString(parts[5])
	if err != nil {
		return false, fmt.Errorf("decode hash: %w", err)
	}

	// Guard against parameters that cause argon2.IDKey to panic (t=0, p=0,
	// empty hash, zero memory, empty salt). A corrupted or attacker-controlled
	// hash string must surface as an error, not a process crash.
	if time == 0 || threads == 0 || memory == 0 || len(wantHash) == 0 || len(salt) == 0 {
		return false, errors.New("invalid argon2 parameters")
	}

	gotHash := argon2.IDKey([]byte(pw), salt, time, memory, threads, uint32(len(wantHash)))
	return subtle.ConstantTimeCompare(gotHash, wantHash) == 1, nil
}
