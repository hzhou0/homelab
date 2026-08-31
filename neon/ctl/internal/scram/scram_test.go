package scram

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"strconv"
	"strings"
	"testing"
)

// The exchange published in RFC 7677 section 3 is the only external check available for a
// verifier: reproducing its client proof and server signature exercises the salted password, both
// keys and the stored key together.
func TestVerifierMatchesPublishedExchange(t *testing.T) {
	const (
		password    = "pencil"
		saltBase64  = "W22ZaJ0SNY7soEsUEjb6gQ=="
		iterations  = 4096
		authMessage = "n=user,r=rOprNGfwEbeRWgbNEkqO," +
			"r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096," +
			"c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0"
		wantProof           = "dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ="
		wantServerSignature = "6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4="
	)

	salt, err := base64.StdEncoding.DecodeString(saltBase64)
	if err != nil {
		t.Fatal(err)
	}

	verifier := VerifierWith(password, salt, iterations)
	if !IsVerifier(verifier) {
		t.Fatalf("VerifierWith produced something IsVerifier rejects: %q", verifier)
	}

	gotIterations, gotSalt, storedKey, serverKey := parse(t, verifier)
	if gotIterations != iterations {
		t.Errorf("iterations = %d, want %d", gotIterations, iterations)
	}
	if gotSalt != saltBase64 {
		t.Errorf("salt = %q, want %q", gotSalt, saltBase64)
	}

	clientSignature := hmacSHA256(storedKey, authMessage)
	clientKey := hmacSHA256(pbkdf2Key(t, password, salt, iterations), "Client Key")
	proof := make([]byte, len(clientKey))
	for i := range clientKey {
		proof[i] = clientKey[i] ^ clientSignature[i]
	}
	if got := base64.StdEncoding.EncodeToString(proof); got != wantProof {
		t.Errorf("client proof = %q, want %q", got, wantProof)
	}

	serverSignature := base64.StdEncoding.EncodeToString(hmacSHA256(serverKey, authMessage))
	if serverSignature != wantServerSignature {
		t.Errorf("server signature = %q, want %q", serverSignature, wantServerSignature)
	}
}

func TestVerifierIsSalted(t *testing.T) {
	first, err := Verifier("hunter2")
	if err != nil {
		t.Fatal(err)
	}
	second, err := Verifier("hunter2")
	if err != nil {
		t.Fatal(err)
	}
	if first == second {
		t.Error("two verifiers for the same password are identical, so the salt is not random")
	}
}

func TestIsVerifierRejectsPlainPasswords(t *testing.T) {
	for _, secret := range []string{
		"",
		"hunter2",
		"SCRAM-SHA-256$",
		"SCRAM-SHA-256$4096:c2FsdA==",
		"SCRAM-SHA-256$notanumber:c2FsdA==$a:b",
		"SCRAM-SHA-256$4096:$a:b",
		"SCRAM-SHA-256$4096:c2FsdA==$onlystored",
	} {
		if IsVerifier(secret) {
			t.Errorf("IsVerifier(%q) = true", secret)
		}
	}
}

func parse(t *testing.T, verifier string) (iterations int, salt string, storedKey, serverKey []byte) {
	t.Helper()
	params, keys, _ := strings.Cut(strings.TrimPrefix(verifier, "SCRAM-SHA-256$"), "$")
	rawIterations, salt, _ := strings.Cut(params, ":")
	iterations, err := strconv.Atoi(rawIterations)
	if err != nil {
		t.Fatal(err)
	}
	rawStored, rawServer, _ := strings.Cut(keys, ":")
	if storedKey, err = base64.StdEncoding.DecodeString(rawStored); err != nil {
		t.Fatal(err)
	}
	if serverKey, err = base64.StdEncoding.DecodeString(rawServer); err != nil {
		t.Fatal(err)
	}
	return iterations, salt, storedKey, serverKey
}

func hmacSHA256(key []byte, message string) []byte {
	h := hmac.New(sha256.New, key)
	h.Write([]byte(message))
	return h.Sum(nil)
}

func pbkdf2Key(t *testing.T, password string, salt []byte, iterations int) []byte {
	t.Helper()
	// Derived through the package's own normalisation so the test exercises the same input.
	return saltedPassword(password, salt, iterations)
}
