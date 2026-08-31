package kube

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

// These tests run against a real apiserver rather than a mock client, because most of what this
// package relies on is admission and validation the apiserver owns: immutable selectors, what a
// label value may contain, and defaulting on write.
var restConfig *rest.Config

func TestMain(m *testing.M) {
	assets, err := envtestAssets()
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}

	environment := &envtest.Environment{BinaryAssetsDirectory: assets}
	restConfig, err = environment.Start()
	if err != nil {
		fmt.Fprintf(os.Stderr, "starting the test apiserver: %v\n", err)
		os.Exit(1)
	}

	code := m.Run()
	if err := environment.Stop(); err != nil {
		fmt.Fprintf(os.Stderr, "stopping the test apiserver: %v\n", err)
	}
	os.Exit(code)
}

const assetHint = "run `make setup-envtest`, or set KUBEBUILDER_ASSETS"

// envtest reads KUBEBUILDER_ASSETS itself; the repo-local directory is the fallback for a run
// started from an editor rather than the Makefile.
func envtestAssets() (string, error) {
	if os.Getenv("KUBEBUILDER_ASSETS") != "" {
		return "", nil
	}
	base := filepath.Join("..", "..", "bin", "k8s")
	entries, err := os.ReadDir(base)
	if err != nil {
		return "", fmt.Errorf("no test apiserver binaries: %s", assetHint)
	}
	for _, entry := range entries {
		if entry.IsDir() {
			return filepath.Join(base, entry.Name()), nil
		}
	}
	return "", fmt.Errorf("no test apiserver binaries under %s: %s", base, assetHint)
}

// The apiserver has no namespace controller, so a namespace can be created but never removed.
// Every test gets its own rather than sharing one that accumulates objects.
func newNamespace(t *testing.T, client kubernetes.Interface) string {
	t.Helper()
	created, err := client.CoreV1().Namespaces().Create(context.Background(),
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{GenerateName: "test-"}}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return created.Name
}
