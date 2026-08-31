// Package scram builds Postgres SCRAM-SHA-256 verifiers. One verifier provisions the role and
// authenticates it at the proxy, so no password is ever stored.
package scram

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"

	"golang.org/x/crypto/pbkdf2"
	"golang.org/x/text/secure/precis"
)

const (
	DefaultIterations = 4096
	saltLength        = 16
)

func Verifier(password string) (string, error) {
	salt := make([]byte, saltLength)
	if _, err := rand.Read(salt); err != nil {
		return "", fmt.Errorf("scram: generating salt: %w", err)
	}
	return VerifierWith(password, salt, DefaultIterations), nil
}

func VerifierWith(password string, salt []byte, iterations int) string {
	salted := saltedPassword(password, salt, iterations)
	clientKey := mac(salted, "Client Key")
	storedKey := sha256.Sum256(clientKey)
	serverKey := mac(salted, "Server Key")

	encode := base64.StdEncoding.EncodeToString
	return fmt.Sprintf("SCRAM-SHA-256$%d:%s$%s:%s",
		iterations, encode(salt), encode(storedKey[:]), encode(serverKey))
}

// IsVerifier reports whether a string is already a verifier, so callers can accept either a
// password or a pre-computed secret without a separate flag.
func IsVerifier(secret string) bool {
	if !strings.HasPrefix(secret, "SCRAM-SHA-256$") {
		return false
	}
	params, keys, split := strings.Cut(strings.TrimPrefix(secret, "SCRAM-SHA-256$"), "$")
	if !split {
		return false
	}
	iterations, salt, ok := strings.Cut(params, ":")
	if !ok || salt == "" {
		return false
	}
	if _, err := strconv.Atoi(iterations); err != nil {
		return false
	}
	stored, server, ok := strings.Cut(keys, ":")
	return ok && stored != "" && server != ""
}

func mac(key []byte, message string) []byte {
	h := hmac.New(sha256.New, key)
	h.Write([]byte(message))
	return h.Sum(nil)
}

// Postgres applies SASLprep and keeps the password unchanged when preparation fails.
func normalize(password string) string {
	prepared, err := precis.OpaqueString.String(password)
	if err != nil {
		return password
	}
	return prepared
}

func saltedPassword(password string, salt []byte, iterations int) []byte {
	return pbkdf2.Key([]byte(normalize(password)), salt, iterations, sha256.Size, sha256.New)
}
