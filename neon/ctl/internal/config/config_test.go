package config

import (
	"strings"
	"testing"
	"time"
)

func minimal() []string {
	return []string{
		"--storage-controller-url=http://storage-controller.neon:8080",
		"--namespace=neon",
		"--compute-pod-template=/etc/neon-ctl/pod.yaml",
		"--compute-images=17=neondatabase/compute-node-v17:8464",
	}
}

func TestLoadRequiresWhatCannotBeGuessed(t *testing.T) {
	if _, err := Load(nil); err == nil {
		t.Fatal("Load accepted an empty configuration")
	} else {
		for _, required := range []string{"storage-controller-url", "namespace", "compute-pod-template", "compute-images"} {
			if !strings.Contains(err.Error(), required) {
				t.Errorf("the error does not mention %s: %v", required, err)
			}
		}
	}

	cfg, err := Load(minimal())
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Listen != ":8080" || cfg.WakeTimeout != 90*time.Second {
		t.Errorf("defaults = %+v", cfg)
	}
}

func TestEnvironmentSuppliesDefaults(t *testing.T) {
	t.Setenv("NEON_CTL_STORAGE_CONTROLLER_URL", "http://from-env:8080")
	t.Setenv("POD_NAMESPACE", "neon")
	t.Setenv("NEON_CTL_COMPUTE_POD_TEMPLATE", "/etc/neon-ctl/pod.yaml")
	t.Setenv("NEON_CTL_COMPUTE_IMAGES", "17=neondatabase/compute-node-v17:8464")
	t.Setenv("NEON_CTL_SUSPEND_TIMEOUT", "30m")

	cfg, err := Load(nil)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.StorageControllerURL != "http://from-env:8080" || cfg.Namespace != "neon" {
		t.Errorf("environment was not read: %+v", cfg)
	}
	if cfg.SuspendTimeout != 30*time.Minute {
		t.Errorf("suspend timeout = %v", cfg.SuspendTimeout)
	}

	// A flag has to win, so a chart can override one value without rewriting the environment.
	cfg, err = Load([]string{"--namespace=other"})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Namespace != "other" {
		t.Errorf("namespace = %q, want the flag to win", cfg.Namespace)
	}
}

// A malformed value must not take the process down at startup, and must not be silently read as
// zero: a zero suspend timeout never suspends anything.
func TestUnparseableEnvironmentFallsBack(t *testing.T) {
	t.Setenv("NEON_CTL_SUSPEND_TIMEOUT", "soon")
	t.Setenv("NEON_CTL_DEFAULT_PG_VERSION", "seventeen")

	cfg, err := Load(minimal())
	if err != nil {
		t.Fatal(err)
	}
	if cfg.SuspendTimeout != 5*time.Minute {
		t.Errorf("fallbacks = %v", cfg.SuspendTimeout)
	}
}

func TestComputeImagesMustBeParsable(t *testing.T) {
	for _, images := range []string{"", "17", "17=", "seventeen=image"} {
		if _, err := Load(append(minimal(), "--compute-images="+images)); err == nil {
			t.Errorf("compute-images=%q was accepted", images)
		}
	}
}
