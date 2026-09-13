package opnsense

import (
	"fmt"
	"strings"
)

// ManagedPrefix marks every OPNsense object created by this operator. Combined
// with the owner tag it forms our "registry" (the external-dns TXT-owner
// analog): the operator only ever reads, updates, or deletes rows whose
// description begins with this prefix and names the owning Kubernetes object.
const ManagedPrefix = "k8s:opnsense-operator"

// Owner identifies the Kubernetes object a set of OPNsense objects belongs to.
type Owner struct {
	Kind      string // "Service" or "Gateway"
	Namespace string
	Name      string
}

// Tag is the stable string embedded in OPNsense descriptions, e.g.
// "Service/app-grafana/grafana".
func (o Owner) Tag() string {
	return fmt.Sprintf("%s/%s/%s", o.Kind, o.Namespace, o.Name)
}

// HostOverride is a single Unbound DNS host override the operator manages.
type HostOverride struct {
	Host       string // left label, e.g. "grafana" or "*" for a wildcard
	Domain     string // zone, e.g. "lab"
	Address    string // target IP (the service's LoadBalancer IP)
	RecordType string // A or AAAA
}

// FQDN reconstructs the full name, e.g. "grafana.lab" or "*.lab".
func (h HostOverride) FQDN() string {
	return h.Host + "." + h.Domain
}

// PortForward is a single WAN DNAT rule the operator manages.
type PortForward struct {
	Interface    string // OPNsense interface name, e.g. "wan"
	Protocol     string // "tcp" or "udp"
	ExternalPort string // port exposed on the WAN
	TargetIP     string // service LoadBalancer IP
	LocalPort    string // forwarded-to port
}

// PassRule is a single WAN filter rule the operator manages. A globally
// routable IPv6 LoadBalancer address needs no translation, so exposing it is a
// matter of admitting the traffic rather than redirecting it; the DNAT rule's
// implicit "pass" has no IPv6 equivalent to piggyback on.
type PassRule struct {
	Interface   string // OPNsense interface name, e.g. "wan"
	Protocol    string // "tcp" or "udp"
	Destination string // globally routable LoadBalancer address
	Port        string // destination port admitted on the WAN
}

// dnsDescription renders the full, human-readable description for a host
// override. Equality of this string is used as the change detector during
// reconciliation, so it must capture every mutable field.
func dnsDescription(o Owner, h HostOverride) string {
	return fmt.Sprintf("%s owner=%s host=%s ip=%s rr=%s", ManagedPrefix, o.Tag(), h.FQDN(), h.Address, h.RecordType)
}

// natDescription renders the full description for a port-forward rule.
func natDescription(o Owner, p PortForward) string {
	return fmt.Sprintf("%s owner=%s pf=%s/%s target=%s:%s iface=%s",
		ManagedPrefix, o.Tag(), p.Protocol, p.ExternalPort, p.TargetIP, p.LocalPort, p.Interface)
}

// passDescription renders the full description for a WAN pass rule.
func passDescription(o Owner, p PassRule) string {
	return fmt.Sprintf("%s owner=%s pass=%s/%s dest=%s iface=%s",
		ManagedPrefix, o.Tag(), p.Protocol, p.Port, p.Destination, p.Interface)
}

// describesOwner reports whether an OPNsense description belongs to this owner.
func describesOwner(desc string, o Owner) bool {
	return strings.HasPrefix(desc, ManagedPrefix) && strings.Contains(desc, " owner="+o.Tag()+" ")
}

func hostOverrideKey(h HostOverride) string {
	return h.FQDN() + "\x00" + h.Address
}

func hostOverrideKeyFromDescription(desc string) string {
	return tokenValue(desc, "host=") + "\x00" + tokenValue(desc, "ip=")
}

// One object names a single protocol, so an owner's rules differ only by the
// address they admit and the port they admit it on. Protocol is therefore an
// attribute to correct in place, not part of what identifies a rule.
func passRuleKey(p PassRule) string {
	return p.Destination + "\x00" + p.Port
}

func passRuleKeyFromDescription(desc string) string {
	return tokenValue(desc, "dest=") + "\x00" + portOfPair(tokenValue(desc, "pass="))
}

// A forward is identified by what it answers on the WAN: an owner may forward
// several ports, but never the same one twice.
func portForwardKey(p PortForward) string {
	return p.ExternalPort
}

func portForwardKeyFromDescription(desc string) string {
	return portOfPair(tokenValue(desc, "pf="))
}

// Both descriptions render the pair as "protocol/port"; only the port is identity.
func portOfPair(pair string) string {
	if i := strings.LastIndexByte(pair, '/'); i >= 0 {
		return pair[i+1:]
	}
	return pair
}

// tokenValue returns the value of a "key=value" token in a space-delimited
// description, or "" if absent.
func tokenValue(desc, key string) string {
	idx := strings.Index(desc, key)
	if idx < 0 {
		return ""
	}
	rest := desc[idx+len(key):]
	if sp := strings.IndexByte(rest, ' '); sp >= 0 {
		return rest[:sp]
	}
	return rest
}

func strptr(s string) *string { return &s }
