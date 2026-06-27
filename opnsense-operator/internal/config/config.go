// Package config loads the operator's runtime configuration from the
// environment. Secrets (OPNsense API key/secret) and connection details are
// injected by the Helm chart via a Kubernetes Secret.
package config

import (
	"fmt"
	"os"
	"strings"
)

// Config holds everything the operator needs to talk to OPNsense and to decide
// which DNS names it is allowed to manage.
type Config struct {
	// OPNsenseURL is the base URL of the OPNsense host, e.g. "https://10.0.0.1".
	OPNsenseURL string
	// APIKey / APISecret authenticate against the OPNsense API (HTTP Basic).
	APIKey    string
	APISecret string
	// WANInterface is the OPNsense interface name port-forwards are bound to.
	WANInterface string
	// ManagedDomains is the set of DNS zones the operator may create overrides
	// in (the external-dns "domain filter"). A hostname is accepted if it equals
	// or is a subdomain of one of these. Empty means "allow any".
	ManagedDomains []string
	// InsecureTLS trusts a self-signed OPNsense certificate when true.
	InsecureTLS bool
}

// FromEnv builds a Config from environment variables, applying defaults and
// validating the required fields.
func FromEnv() (*Config, error) {
	c := &Config{
		OPNsenseURL:    strings.TrimSpace(os.Getenv("OPNSENSE_URL")),
		APIKey:         os.Getenv("OPNSENSE_API_KEY"),
		APISecret:      os.Getenv("OPNSENSE_API_SECRET"),
		WANInterface:   getEnvDefault("OPNSENSE_WAN_INTERFACE", "wan"),
		ManagedDomains: splitAndTrim(os.Getenv("MANAGED_DOMAINS")),
		InsecureTLS:    boolEnv("OPNSENSE_INSECURE_TLS"),
	}

	if c.OPNsenseURL == "" {
		return nil, fmt.Errorf("config: OPNSENSE_URL is required")
	}
	if c.APIKey == "" || c.APISecret == "" {
		return nil, fmt.Errorf("config: OPNSENSE_API_KEY and OPNSENSE_API_SECRET are required")
	}
	return c, nil
}

// DomainAllowed reports whether fqdn falls within one of the managed domains.
// A wildcard such as "*.lab" is matched on its parent domain ("lab"). When no
// managed domains are configured, everything is allowed.
func (c *Config) DomainAllowed(fqdn string) bool {
	if len(c.ManagedDomains) == 0 {
		return true
	}
	host := strings.ToLower(strings.TrimSuffix(fqdn, "."))
	host = strings.TrimPrefix(host, "*.")
	for _, d := range c.ManagedDomains {
		d = strings.ToLower(strings.TrimSuffix(d, "."))
		if host == d || strings.HasSuffix(host, "."+d) {
			return true
		}
	}
	return false
}

func getEnvDefault(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

func splitAndTrim(s string) []string {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func boolEnv(key string) bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(key))) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}
