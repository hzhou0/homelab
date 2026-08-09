package controller

import (
	"fmt"
	"net/netip"
	"sort"
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
	PassRules   []opnsense.PassRule
	// HasIntent distinguishes "nothing was asked for" from "something was asked
	// for but no address has been assigned yet"; the latter must wait rather
	// than tear down.
	HasIntent bool
}

// Summary is a compact, human-readable description of what was wired up, written
// back to the AnnExposed annotation.
func (d DesiredExposure) Summary() string {
	var parts []string
	if len(d.Hosts) > 0 {
		names := make([]string, 0, len(d.Hosts))
		for _, h := range d.Hosts {
			names = append(names, fmt.Sprintf("%s(%s)", h.FQDN(), h.RecordType))
		}
		sort.Strings(names)
		parts = append(parts, "dns="+strings.Join(names, ","))
	}
	if d.PortForward != nil {
		p := d.PortForward
		parts = append(parts, fmt.Sprintf("wan=%s/%s->%s:%s", p.Protocol, p.ExternalPort, p.TargetIP, p.LocalPort))
	}
	if len(d.PassRules) > 0 {
		dests := make([]string, 0, len(d.PassRules))
		for _, p := range d.PassRules {
			dests = append(dests, fmt.Sprintf("%s/%s->[%s]", p.Protocol, p.Port, p.Destination))
		}
		sort.Strings(dests)
		parts = append(parts, "wan6="+strings.Join(dests, ","))
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
	Addresses   []string
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
	addresses, err := parseAddresses(in.Addresses)
	if err != nil {
		return d, err
	}

	for _, name := range splitList(in.Annotations[AnnHostname]) {
		d.HasIntent = true
		host, domain, err := splitFQDN(name)
		if err != nil {
			return d, err
		}
		if !cfg.DomainAllowed(name) {
			return d, fmt.Errorf("hostname %q is outside managed domains %v", name, cfg.ManagedDomains)
		}
		for _, address := range addresses {
			d.Hosts = append(d.Hosts, opnsense.HostOverride{
				Host:       host,
				Domain:     domain,
				Address:    address.addr.String(),
				RecordType: address.recordType,
			})
		}
	}

	if isTrue(in.Annotations[AnnExpose]) {
		d.HasIntent = true
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

		// Only translation can remap a port, and IPv6 is passed through
		// untranslated. Refusing the whole object beats wiring up IPv4 and
		// leaving IPv6 admitted on a port nothing listens on.
		if extPort != localPort && hasIPv6(addresses) {
			return d, fmt.Errorf("%s %s differs from %s %s, which cannot be honoured for an IPv6 address: IPv6 is not translated",
				AnnExternalPort, extPort, AnnInternalPort, localPort)
		}

		// IPv4 is reached by translating the WAN address; IPv6 is globally
		// routable, so it only needs the WAN's default-deny lifted.
		for _, address := range addresses {
			if !address.addr.Is4() {
				d.PassRules = append(d.PassRules, opnsense.PassRule{
					Interface:   cfg.WANInterface,
					Protocol:    proto,
					Destination: address.addr.String(),
					Port:        extPort,
				})
				continue
			}
			if d.PortForward == nil {
				d.PortForward = &opnsense.PortForward{
					Interface:    cfg.WANInterface,
					Protocol:     proto,
					ExternalPort: extPort,
					TargetIP:     address.addr.String(),
					LocalPort:    localPort,
				}
			}
		}
	}

	return d, nil
}

type parsedAddress struct {
	addr       netip.Addr
	recordType string
}

func hasIPv6(addresses []parsedAddress) bool {
	for _, address := range addresses {
		if !address.addr.Is4() {
			return true
		}
	}
	return false
}

func parseAddresses(values []string) ([]parsedAddress, error) {
	addresses := make([]parsedAddress, 0, len(values))
	seen := make(map[netip.Addr]struct{}, len(values))
	for _, value := range values {
		addr, err := netip.ParseAddr(strings.TrimSpace(value))
		if err != nil {
			return nil, fmt.Errorf("invalid LoadBalancer IP %q: %w", value, err)
		}
		addr = addr.Unmap()
		if _, ok := seen[addr]; ok {
			continue
		}
		seen[addr] = struct{}{}
		recordType := "AAAA"
		if addr.Is4() {
			recordType = "A"
		}
		addresses = append(addresses, parsedAddress{addr: addr, recordType: recordType})
	}
	return addresses, nil
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
