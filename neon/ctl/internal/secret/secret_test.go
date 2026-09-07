package secret

import (
	"crypto/rand"
	"strings"
	"testing"
)

func testBox(t *testing.T) *Box {
	t.Helper()
	key := make([]byte, KeySize)
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}
	box, err := New(key)
	if err != nil {
		t.Fatal(err)
	}
	return box
}

func TestASealedValueComesBack(t *testing.T) {
	box := testBox(t)
	sealed, err := box.Seal("hunter2", "app")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(sealed, "hunter2") {
		t.Fatal("the plaintext is in the sealed value")
	}
	opened, err := box.Open(sealed, "app")
	if err != nil {
		t.Fatal(err)
	}
	if opened != "hunter2" {
		t.Errorf("opened %q", opened)
	}
}

// The label is what stops a sealed value being moved to another role and becoming its password.
func TestASealedValueWillNotOpenUnderAnotherLabel(t *testing.T) {
	box := testBox(t)
	sealed, err := box.Seal("hunter2", "app")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := box.Open(sealed, "reporting"); err == nil {
		t.Error("a sealed value opened under a label it was not sealed with")
	}
}

func TestAnotherKeyCannotOpenIt(t *testing.T) {
	sealed, err := testBox(t).Seal("hunter2", "app")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := testBox(t).Open(sealed, "app"); err == nil {
		t.Error("a value opened under a key it was not sealed with")
	}
}

func TestSealingIsSalted(t *testing.T) {
	box := testBox(t)
	first, _ := box.Seal("hunter2", "app")
	second, _ := box.Seal("hunter2", "app")
	if first == second {
		t.Error("the same password sealed twice is the same string, which leaks that it did not change")
	}
}

func TestAKeyMustBeTheRightSize(t *testing.T) {
	if _, err := New([]byte("short")); err == nil {
		t.Error("a short key was accepted")
	}
}
