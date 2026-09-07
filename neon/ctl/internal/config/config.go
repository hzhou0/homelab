// Package config resolves the service's configuration from flags, each defaulting to an
// environment variable so a chart can supply either.
package config

import (
	"encoding/base64"
	"errors"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/hzhou0/homelab/neon/ctl/internal/secret"
)

type Config struct {
	Listen string

	StorageControllerURL   string
	StorageControllerToken string

	Namespace          string
	ComputePodTemplate string
	ComputeImages      map[int]string

	ProxyDeployment string

	WakeTimeout    time.Duration
	SuspendTimeout time.Duration

	// PKCS#8 PEM. Empty leaves every storage call unauthenticated, which the controller permits
	// only under --dev.
	AuthKey string

	IdentityUserHeader    string
	IdentityDisplayHeader string
	IdentityGroupsHeader  string

	// The one identifier that may see every project.
	AdminUser string

	// The domain endpoint ids are shown under. Display only — the proxy takes the suffix it
	// actually routes on from its own certificate.
	EndpointSuffix string

	// Seals the passwords the registry keeps, and its absence is what stops it keeping any: a
	// password is otherwise only ever a verifier, which cannot be read back.
	PasswordKey []byte
}

func Load(args []string) (*Config, error) {
	cfg := &Config{}
	flags := flag.NewFlagSet("neon-ctl", flag.ContinueOnError)

	flags.StringVar(&cfg.Listen, "listen", env("NEON_CTL_LISTEN", ":8080"), "address to serve on")

	flags.StringVar(&cfg.IdentityUserHeader, "identity-user-header", env("NEON_CTL_IDENTITY_USER_HEADER", ""),
		"request header carrying a stable directory identifier for the user, not a login name")
	flags.StringVar(&cfg.IdentityDisplayHeader, "identity-display-header", env("NEON_CTL_IDENTITY_DISPLAY_HEADER", ""),
		"request header carrying the user's human-readable name, shown but never acted on")
	flags.StringVar(&cfg.IdentityGroupsHeader, "identity-groups-header", env("NEON_CTL_IDENTITY_GROUPS_HEADER", ""),
		"request header carrying the user's groups, comma separated")
	flags.StringVar(&cfg.AdminUser, "admin-user", env("NEON_CTL_ADMIN_USER", ""),
		"directory identifier of the one user who may see every project")

	flags.StringVar(&cfg.EndpointSuffix, "endpoint-suffix", env("NEON_CTL_ENDPOINT_SUFFIX", ""),
		"domain endpoint ids are shown under, so the page can print a connectable host")

	var passwordKey string
	flags.StringVar(&passwordKey, "password-key", env("NEON_CTL_PASSWORD_KEY", ""),
		"base64 32-byte key that seals stored passwords; unset keeps no password, only its verifier")

	flags.StringVar(&cfg.StorageControllerURL, "storage-controller-url", env("NEON_CTL_STORAGE_CONTROLLER_URL", ""), "base url of the storage controller")
	flags.StringVar(&cfg.StorageControllerToken, "storage-controller-token", env("NEON_CTL_STORAGE_CONTROLLER_TOKEN", ""), "bearer token for the storage controller")

	flags.StringVar(&cfg.Namespace, "namespace", env("NEON_CTL_NAMESPACE", env("POD_NAMESPACE", inClusterNamespace())), "namespace computes are created in; inferred from the service account when unset")
	flags.StringVar(&cfg.ComputePodTemplate, "compute-pod-template", env("NEON_CTL_COMPUTE_POD_TEMPLATE", ""), "path to the compute pod template")
	var images string
	flags.StringVar(&images, "compute-images", env("NEON_CTL_COMPUTE_IMAGES", ""), "compute image per postgres major version, as 17=repo/image:tag,16=...")

	flags.StringVar(&cfg.ProxyDeployment, "proxy-deployment", env("NEON_CTL_PROXY_DEPLOYMENT", ""), "proxy deployment to restart when a secret it mounts is renewed")

	flags.StringVar(&cfg.AuthKey, "auth-key", env("NEON_CTL_AUTH_KEY", ""), "ed25519 private key for storage authentication, as PKCS#8 PEM")

	flags.DurationVar(&cfg.WakeTimeout, "wake-timeout", envDuration("NEON_CTL_WAKE_TIMEOUT", 90*time.Second), "how long a connection waits for a suspended compute")
	flags.DurationVar(&cfg.SuspendTimeout, "suspend-timeout", envDuration("NEON_CTL_SUSPEND_TIMEOUT", 5*time.Minute), "idle time before a compute is scaled to zero; zero never suspends")

	if err := flags.Parse(args); err != nil {
		return nil, err
	}
	var err error
	if cfg.ComputeImages, err = parseImages(images); err != nil {
		return nil, err
	}
	if cfg.PasswordKey, err = parsePasswordKey(passwordKey); err != nil {
		return nil, err
	}
	return cfg, cfg.validate()
}

func (c *Config) validate() error {
	var problems []error
	if c.StorageControllerURL == "" {
		problems = append(problems, errors.New("storage-controller-url is required"))
	}
	if c.Namespace == "" {
		problems = append(problems, errors.New("namespace is required"))
	}
	if c.ComputePodTemplate == "" {
		problems = append(problems, errors.New("compute-pod-template is required"))
	}
	if len(c.ComputeImages) == 0 {
		problems = append(problems, errors.New("compute-images is required"))
	}
	if c.IdentityUserHeader == "" {
		problems = append(problems, errors.New("identity-user-header is required"))
	}
	return errors.Join(problems...)
}

// RegistryPath is where the registry database lives. The chart mounts a volume at its directory;
// nothing else needs to know or change it.
const RegistryPath = "/var/lib/neon-ctl/registry.db"

// A pod always has this file, so a namespace never has to be configured.
func inClusterNamespace() string {
	namespace, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(namespace))
}

// Refused rather than stretched: a passphrase would put every stored password behind whatever
// somebody typed, and a wrong-length key is far more likely a mistake than an intention.
func parsePasswordKey(value string) ([]byte, error) {
	if value == "" {
		return nil, nil
	}
	key, err := base64.StdEncoding.DecodeString(strings.TrimSpace(value))
	if err != nil {
		return nil, fmt.Errorf("password-key is not base64: %w", err)
	}
	if len(key) != secret.KeySize {
		return nil, fmt.Errorf("password-key is %d bytes, want %d", len(key), secret.KeySize)
	}
	return key, nil
}

func parseImages(value string) (map[int]string, error) {
	images := map[int]string{}
	for _, entry := range strings.Split(value, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		major, image, ok := strings.Cut(entry, "=")
		if !ok {
			return nil, fmt.Errorf("compute-images: %q is not <major>=<image>", entry)
		}
		version, err := strconv.Atoi(strings.TrimSpace(major))
		if err != nil {
			return nil, fmt.Errorf("compute-images: %q is not a postgres major version", major)
		}
		if images[version] = strings.TrimSpace(image); images[version] == "" {
			return nil, fmt.Errorf("compute-images: postgres %d has no image", version)
		}
	}
	return images, nil
}

func env(key, fallback string) string {
	if value, set := os.LookupEnv(key); set {
		return value
	}
	return fallback
}

func envDuration(key string, fallback time.Duration) time.Duration {
	value, set := os.LookupEnv(key)
	if !set {
		return fallback
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ignoring %s=%q: %v\n", key, value, err)
		return fallback
	}
	return parsed
}
