package opnsense

import "testing"

func TestDescribesOwner(t *testing.T) {
	owner := Owner{Kind: "Service", Namespace: "app-grafana", Name: "grafana"}
	other := Owner{Kind: "Service", Namespace: "app-grafana", Name: "grafana2"}
	desc := dnsDescription(owner, HostOverride{Host: "grafana", Domain: "lab", Address: "10.0.0.100", RecordType: "A"})

	if !describesOwner(desc, owner) {
		t.Fatalf("expected %q to be owned by %s", desc, owner.Tag())
	}
	// Prefix collision (grafana vs grafana2) must not match.
	if describesOwner(desc, other) {
		t.Fatalf("description for %s wrongly matched %s", owner.Tag(), other.Tag())
	}
	if describesOwner("some unrelated description", owner) {
		t.Fatal("unrelated description should not be owned")
	}
}

func TestHostOverrideKeyFromDescription(t *testing.T) {
	owner := Owner{Kind: "Gateway", Namespace: "infra", Name: "gw"}
	host := HostOverride{Host: "*", Domain: "lab", Address: "2001:db8::100", RecordType: "AAAA"}
	desc := dnsDescription(owner, host)
	if got, want := hostOverrideKeyFromDescription(desc), hostOverrideKey(host); got != want {
		t.Fatalf("hostOverrideKeyFromDescription = %q, want %q", got, want)
	}
}

func TestNATDescriptionStable(t *testing.T) {
	owner := Owner{Kind: "Service", Namespace: "app-foo", Name: "foo"}
	pf := PortForward{Interface: "wan", Protocol: "tcp", ExternalPort: "443", TargetIP: "10.0.0.100", LocalPort: "443"}
	a := natDescription(owner, pf)
	b := natDescription(owner, pf)
	if a != b {
		t.Fatalf("natDescription not stable: %q vs %q", a, b)
	}
	if !describesOwner(a, owner) {
		t.Fatalf("nat description %q not owned by %s", a, owner.Tag())
	}
}
