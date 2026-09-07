// Package secret seals values that have to be read back. A SCRAM verifier proves a password
// without keeping it, which is why the registry holds one; a password that must be shown again
// cannot be hashed, so it is encrypted under a key the registry file does not contain.
package secret

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
)

// KeySize is what a key must be. Nothing here stretches a short one: a passphrase would put the
// strength of every stored password in whatever somebody typed.
const KeySize = 32

type Box struct {
	aead cipher.AEAD
}

func New(key []byte) (*Box, error) {
	if len(key) != KeySize {
		return nil, fmt.Errorf("secret: key is %d bytes, want %d", len(key), KeySize)
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("secret: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("secret: %w", err)
	}
	return &Box{aead: aead}, nil
}

// The label is bound into the ciphertext, so a sealed value moved to another name stops opening
// rather than quietly becoming that name's password.
func (b *Box) Seal(plaintext, label string) (string, error) {
	nonce := make([]byte, b.aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return "", fmt.Errorf("secret: generating a nonce: %w", err)
	}
	sealed := b.aead.Seal(nonce, nonce, []byte(plaintext), []byte(label))
	return base64.RawURLEncoding.EncodeToString(sealed), nil
}

var ErrSealed = errors.New("secret: cannot be opened")

func (b *Box) Open(sealed, label string) (string, error) {
	raw, err := base64.RawURLEncoding.DecodeString(sealed)
	if err != nil || len(raw) < b.aead.NonceSize() {
		return "", ErrSealed
	}
	nonce, ciphertext := raw[:b.aead.NonceSize()], raw[b.aead.NonceSize():]
	plaintext, err := b.aead.Open(nil, nonce, ciphertext, []byte(label))
	if err != nil {
		return "", ErrSealed
	}
	return string(plaintext), nil
}
