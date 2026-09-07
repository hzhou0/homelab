package main

import (
	"os"
	"path/filepath"
	"testing"
)

// The directory survives the container, so the second run is the one that used to fail.
func TestWriteDerivedReplacesReadOnlyFile(t *testing.T) {
	dir := t.TempDir()

	if err := writeDerived(dir, "public.pem", []byte("first")); err != nil {
		t.Fatalf("first write: %v", err)
	}
	if err := writeDerived(dir, "public.pem", []byte("second")); err != nil {
		t.Fatalf("second write: %v", err)
	}

	content, err := os.ReadFile(filepath.Join(dir, "public.pem"))
	if err != nil {
		t.Fatal(err)
	}
	if string(content) != "second" {
		t.Fatalf("content = %q, want the value from the second run", content)
	}

	info, err := os.Stat(filepath.Join(dir, "public.pem"))
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o444 {
		t.Fatalf("mode = %v, want -r--r--r--", info.Mode().Perm())
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("%d files left behind, want only the written one", len(entries))
	}
}
