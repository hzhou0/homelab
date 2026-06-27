package controller

import (
	"fmt"
	"strings"

	"github.com/hzhou0/homelab/opnsense-operator/internal/config"
	"github.com/hzhou0/homelab/opnsense-operator/internal/opnsense"
)

// Annotation keys (the homelab.lab/ prefix is used throughout this repo).
const (
	AnnHostname     = "homelab.lab/hostname"
	AnnExpose       = "homelab.lab/expose"
	AnnExternalPort = "homelab.lab/external-port"
	AnnProtocol     = "homelab.lab/protocol"
	AnnInternalPort = "homelab.lab/internal-port"

	// AnnExposed records what the operator wired up (status, not input).
	AnnExposed = "homelab.lab/exposed"

	// Finalizer guards owned OPNsense objects against orphaning on delete.
	Finalizer = "homelab.lab/opnsense-operator"
)

// DesiredExposure is the parsed, validated intent for one source object.
type DesiredExposure struct {
	Hosts       []opnsense.HostOverride
	PortForward *opnsense.PortForward
}

// Empty reports whether the object asks for nothing (no DNS, no port-forward).
func (d DesiredExposure) Empty() bool {
	return len(d.Hosts) == 0 && d.PortForward == nil
}

// Summary is a compact, human-readable description of what was wired up, written
// back to the AnnExposed annotation.
func (d DesiredExposure) Summary() string {
	var parts []string
	if len(d.Hosts) > 0 {
		names := make([]string, 0, len(d.Hosts))
		for _, h := range d.Hosts {
			names = append(names, h.FQDN())
		}
		parts = append(parts, "dns="+strings.Join(names, ","))
	}
	if d.PortForward != nil {
		p := d.PortForward
		parts = append(parts, fmt.Sprintf("wan=%s/%s->%s:%s", p.Protocol, p.ExternalPort, p.TargetIP, p.LocalPort))
	}
	if len(parts) == 0 {
		return ""
	}
	return strings.Join(parts, " ")
}

// ExposureInput is the protocol-agnostic data the parser needs from either a
// Service or a Gateway.
type ExposureInput struct {
	Annotations map[string]string
	IP          string // resolved LoadBalancer IP
	// DefaultPort and DefaultProtocol come from the object's port/listener and
	// are used when the corresponding annotations are absent.
	DefaultPort     string
	DefaultProtocol string
}

// ParseExposure turns annotations + object defaults into a DesiredExposure,
// rejecting hostnames outside the managed domains. It returns an error for
// malformed input so the caller can surface it as an Event/condition.
func ParseExposure(in ExposureInput, cfg *config.Config) (DesiredExposure, error) {
	var d DesiredExposure

	for _, name := range splitList(in.Annotations[AnnHostname]) {
		host, domain, err := splitFQDN(name)
		if err != nil {
			return d, err
		}
		if !cfg.DomainAllowed(name) {
			return d, fmt.Errorf("hostname %q is outside managed domains %v", name, cfg.ManagedDomains)
		}
		d.Hosts = append(d.Hosts, opnsense.HostOverride{Host: host, Domain: domain, Address: in.IP})
	}

	if isTrue(in.Annotations[AnnExpose]) {
		proto := strings.ToLower(strings.TrimSpace(in.Annotations[AnnProtocol]))
		if proto == "" {
			proto = in.DefaultProtocol
		}
		if proto != "tcp" && proto != "udp" {
			return d, fmt.Errorf("invalid %s %q (want tcp or udp)", AnnProtocol, proto)
		}

		extPort := firstNonEmpty(in.Annotations[AnnExternalPort], in.DefaultPort)
		localPort := firstNonEmpty(in.Annotations[AnnInternalPort], in.DefaultPort)
		if extPort == "" || localPort == "" {
			return d, fmt.Errorf("%s set but no port available (annotate %s/%s)", AnnExpose, AnnExternalPort, AnnInternalPort)
		}

		d.PortForward = &opnsense.PortForward{
			Interface:    cfg.WANInterface,
			Protocol:     proto,
			ExternalPort: extPort,
			TargetIP:     in.IP,
			LocalPort:    localPort,
		}
	}

	return d, nil
}

// splitFQDN splits a DNS name into its first label and the remaining domain.
// A leading "*" is preserved as a wildcard host. The name must contain at least
// one dot (a bare TLD-less label is rejected).
func splitFQDN(name string) (host, domain string, err error) {
	name = strings.TrimSuffix(strings.TrimSpace(name), ".")
	if name == "" {
		return "", "", fmt.Errorf("empty hostname")
	}
	idx := strings.IndexByte(name, '.')
	if idx <= 0 || idx == len(name)-1 {
		return "", "", fmt.Errorf("hostname %q must be of the form host.domain", name)
	}
	host = name[:idx]
	domain = name[idx+1:]
	if host != "*" && strings.ContainsAny(host, "*") {
		return "", "", fmt.Errorf("hostname %q: wildcard must be the whole first label", name)
	}
	return host, domain, nil
}

func splitList(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func isTrue(s string) bool {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v = strings.TrimSpace(v); v != "" {
			return v
		}
	}
	return ""
}
