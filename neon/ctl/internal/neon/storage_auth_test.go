package neon

import "testing"

// Neon's own fixtures, so a token we mint is checked against the implementation that will read it
// rather than against our own understanding of it.
const (
	neonTestPrivateKey = `-----BEGIN PRIVATE KEY-----
MC4CAQAwBQYDK2VwBCIEID/Drmc1AA6U/znNRWpF3zEGegOATQxfkdWxitcOMsIH
-----END PRIVATE KEY-----
`
	neonTestPublicKey = `-----BEGIN PUBLIC KEY-----
MCowBQYDK2VwAyEARYwaNBayR+eGI0iXB4s3QxE3Nl2g1iWbr6KtLWeVD/w=
-----END PUBLIC KEY-----
`
	// Minted by Neon, carrying {scope: tenant, tenant_id: 3d1f...} plus iss and iat.
	neonTestToken = "eyJhbGciOiJFZERTQSIsInR5cCI6IkpXVCJ9.eyJzY29wZSI6InRlbmFudCIsInRlbmFudF9pZCI6IjNkMWY3NTk1YjQ2ODIzMDMwNGUwYjczY2VjYmNiMDgxIiwiaXNzIjoibmVvbi5jb250cm9scGxhbmUiLCJpYXQiOjE2Nzg0NDI0Nzl9.rNheBnluMJNgXzSTTJoTNIGy4P_qe0JUHl_nVEGuDCTgHOThPVr552EnmKccrCKquPeW3c2YUk0Y9Oh4KyASAw"
)

func TestVerifyAcceptsNeonsOwnToken(t *testing.T) {
	verifier, err := NewStorageVerifier([]byte(neonTestPublicKey))
	if err != nil {
		t.Fatal(err)
	}
	claims, err := verifier.Verify(neonTestToken)
	if err != nil {
		t.Fatal(err)
	}
	if claims.Scope != ScopeTenant {
		t.Errorf("scope = %q, want tenant", claims.Scope)
	}
	if claims.TenantID == nil || claims.TenantID.String() != "3d1f7595b468230304e0b73cecbcb081" {
		t.Errorf("tenant = %v", claims.TenantID)
	}
}

// The reverse direction: a token we sign has to carry the shape Neon deserialises, and unknown
// claims it does not send (iss, iat) must not be required.
func TestSignedTokenMatchesNeonsShape(t *testing.T) {
	key, err := NewStorageKey([]byte(neonTestPrivateKey))
	if err != nil {
		t.Fatal(err)
	}
	tenant, err := ParseTenantID("3d1f7595b468230304e0b73cecbcb081")
	if err != nil {
		t.Fatal(err)
	}
	token, err := key.Token(StorageClaims{TenantID: &tenant, Scope: ScopeTenant})
	if err != nil {
		t.Fatal(err)
	}

	verifier, err := NewStorageVerifier([]byte(neonTestPublicKey))
	if err != nil {
		t.Fatal(err)
	}
	claims, err := verifier.Verify(token)
	if err != nil {
		t.Fatalf("our own key could not validate our own token: %v", err)
	}
	if claims.Scope != ScopeTenant || claims.TenantID == nil || *claims.TenantID != tenant {
		t.Errorf("claims = %+v", claims)
	}
}

// An admin token carries no tenant, and the field has to be absent rather than null: Neon's
// deserialiser takes a missing tenant_id as None, and a scope without one is how blanket access
// is expressed.
func TestAdminTokenOmitsTenant(t *testing.T) {
	key, err := NewStorageKey([]byte(neonTestPrivateKey))
	if err != nil {
		t.Fatal(err)
	}
	token, err := key.Token(StorageClaims{Scope: ScopeAdmin})
	if err != nil {
		t.Fatal(err)
	}
	verifier, _ := NewStorageVerifier([]byte(neonTestPublicKey))
	claims, err := verifier.Verify(token)
	if err != nil {
		t.Fatal(err)
	}
	if claims.TenantID != nil || claims.Scope != ScopeAdmin {
		t.Errorf("claims = %+v", claims)
	}
}

func TestVerifyRejectsATamperedToken(t *testing.T) {
	verifier, _ := NewStorageVerifier([]byte(neonTestPublicKey))
	if _, err := verifier.Verify(neonTestToken[:len(neonTestToken)-4] + "AAAA"); err == nil {
		t.Error("a token with a broken signature was accepted")
	}
}
