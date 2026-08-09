package controller

import (
	"testing"

	corev1 "k8s.io/api/core/v1"

	"github.com/hzhou0/homelab/opnsense-operator/internal/config"
)

func testCfg() *config.Config {
	return &config.Config{
		WANInterface:   "wan",
		ManagedDomains: []string{"lab", "home.example.com"},
	}
}

func TestSplitFQDN(t *testing.T) {
	tests := []struct {
		in      string
		host    string
		domain  string
		wantErr bool
	}{
		{in: "grafana.lab", host: "grafana", domain: "lab"},
		{in: "*.lab", host: "*", domain: "lab"},
		{in: "a.b.lab", host: "a", domain: "b.lab"},
		{in: "grafana.home.example.com", host: "grafana", domain: "home.example.com"},
		{in: "trailingdot.lab.", host: "trailingdot", domain: "lab"},
		{in: "lab", wantErr: true},
		{in: "", wantErr: true},
		{in: ".lab", wantErr: true},
		{in: "host.", wantErr: true},
		{in: "*x.lab", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			host, domain, err := splitFQDN(tt.in)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error for %q, got host=%q domain=%q", tt.in, host, domain)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if host != tt.host || domain != tt.domain {
				t.Fatalf("got host=%q domain=%q, want host=%q domain=%q", host, domain, tt.host, tt.domain)
			}
		})
	}
}

func TestParseExposure(t *testing.T) {
	cfg := testCfg()

	t.Run("single hostname, no port-forward", func(t *testing.T) {
		d, err := ParseExposure(ExposureInput{
			Annotations: map[string]string{AnnHostname: "grafana.lab"},
			Addresses:   []string{"10.0.0.100"},
		}, cfg)
		if err != nil {
			t.Fatal(err)
		}
		if len(d.Hosts) != 1 || d.Hosts[0].FQDN() != "grafana.lab" ||
			d.Hosts[0].Address != "10.0.0.100" || d.Hosts[0].RecordType != "A" {
			t.Fatalf("unexpected hosts: %+v", d.Hosts)
		}
		if d.PortForward != nil {
			t.Fatalf("expected no port-forward, got %+v", d.PortForward)
		}
	})

	t.Run("wildcard and multi-host", func(t *testing.T) {
		d, err := ParseExposure(ExposureInput{
			Annotations: map[string]string{AnnHostname: "*.lab, grafana.home.example.com"},
			Addresses:   []string{"10.0.0.100"},
		}, cfg)
		if err != nil {
			t.Fatal(err)
		}
		if len(d.Hosts) != 2 {
			t.Fatalf("want 2 hosts, got %d", len(d.Hosts))
		}
		if d.Hosts[0].Host != "*" || d.Hosts[0].Domain != "lab" {
			t.Fatalf("wildcard not parsed: %+v", d.Hosts[0])
		}
	})

	t.Run("domain filter rejects foreign zone", func(t *testing.T) {
		_, err := ParseExposure(ExposureInput{
			Annotations: map[string]string{AnnHostname: "evil.example.org"},
			Addresses:   []string{"10.0.0.100"},
		}, cfg)
		if err == nil {
			t.Fatal("expected domain-filter rejection")
		}
	})

	t.Run("expose with defaults", func(t *testing.T) {
		d, err := ParseExposure(ExposureInput{
			Annotations:     map[string]string{AnnExpose: "true"},
			Addresses:       []string{"10.0.0.100"},
			DefaultPort:     "443",
			DefaultProtocol: "tcp",
		}, cfg)
		if err != nil {
			t.Fatal(err)
		}
		pf := d.PortForward
		if pf == nil {
			t.Fatal("expected port-forward")
		}
		if pf.Protocol != "tcp" || pf.ExternalPort != "443" || pf.LocalPort != "443" ||
			pf.TargetIP != "10.0.0.100" || pf.Interface != "wan" {
			t.Fatalf("unexpected port-forward: %+v", pf)
		}
	})

	t.Run("expose udp with explicit ports", func(t *testing.T) {
		d, err := ParseExposure(ExposureInput{
			Annotations: map[string]string{
				AnnExpose:       "true",
				AnnProtocol:     "udp",
				AnnExternalPort: "27015",
				AnnInternalPort: "27016",
			},
			Addresses:       []string{"10.0.0.101"},
			DefaultPort:     "27015",
			DefaultProtocol: "udp",
		}, cfg)
		if err != nil {
			t.Fatal(err)
		}
		pf := d.PortForward
		if pf.Protocol != "udp" || pf.ExternalPort != "27015" || pf.LocalPort != "27016" {
			t.Fatalf("unexpected port-forward: %+v", pf)
		}
	})

	t.Run("expose without any port errors", func(t *testing.T) {
		_, err := ParseExposure(ExposureInput{
			Annotations:     map[string]string{AnnExpose: "true"},
			Addresses:       []string{"10.0.0.100"},
			DefaultProtocol: "tcp",
		}, cfg)
		if err == nil {
			t.Fatal("expected error when no port available")
		}
	})

	t.Run("invalid protocol errors", func(t *testing.T) {
		_, err := ParseExposure(ExposureInput{
			Annotations:     map[string]string{AnnExpose: "true", AnnProtocol: "sctp"},
			Addresses:       []string{"10.0.0.100"},
			DefaultPort:     "443",
			DefaultProtocol: "tcp",
		}, cfg)
		if err == nil {
			t.Fatal("expected invalid-protocol error")
		}
	})

	t.Run("empty is empty", func(t *testing.T) {
		d, err := ParseExposure(ExposureInput{Annotations: map[string]string{}, Addresses: []string{"10.0.0.100"}}, cfg)
		if err != nil {
			t.Fatal(err)
		}
		if d.HasIntent || len(d.Hosts) != 0 || d.PortForward != nil || len(d.PassRules) != 0 {
			t.Fatalf("expected empty exposure, got %+v", d)
		}
	})

	t.Run("dual-stack creates A and AAAA but only IPv4 DNAT", func(t *testing.T) {
		d, err := ParseExposure(ExposureInput{
			Annotations: map[string]string{
				AnnHostname: "media.lab",
				AnnExpose:   "true",
				AnnProtocol: "udp",
			},
			Addresses:       []string{"10.0.0.101", "2001:db8::101"},
			DefaultPort:     "3478",
			DefaultProtocol: "udp",
		}, cfg)
		if err != nil {
			t.Fatal(err)
		}
		if len(d.Hosts) != 2 || d.Hosts[0].RecordType != "A" || d.Hosts[1].RecordType != "AAAA" {
			t.Fatalf("unexpected dual-stack hosts: %+v", d.Hosts)
		}
		if d.PortForward == nil || d.PortForward.TargetIP != "10.0.0.101" {
			t.Fatalf("DNAT must target only IPv4: %+v", d.PortForward)
		}
		if len(d.PassRules) != 1 || d.PassRules[0].Destination != "2001:db8::101" ||
			d.PassRules[0].Port != "3478" || d.PassRules[0].Protocol != "udp" {
			t.Fatalf("IPv6 must be exposed by a pass rule: %+v", d.PassRules)
		}
		want := "dns=media.lab(A),media.lab(AAAA) wan=udp/3478->10.0.0.101:3478 wan6=udp/3478->[2001:db8::101]"
		if got := d.Summary(); got != want {
			t.Fatalf("Summary() = %q, want %q", got, want)
		}
	})

	t.Run("IPv6-only expose creates a pass rule and no DNAT", func(t *testing.T) {
		d, err := ParseExposure(ExposureInput{
			Annotations:     map[string]string{AnnExpose: "true"},
			Addresses:       []string{"2001:db8::101"},
			DefaultPort:     "3478",
			DefaultProtocol: "udp",
		}, cfg)
		if err != nil {
			t.Fatal(err)
		}
		if d.PortForward != nil {
			t.Fatalf("IPv6 needs no translation: %+v", d.PortForward)
		}
		if len(d.PassRules) != 1 || d.PassRules[0].Interface != "wan" ||
			d.PassRules[0].Destination != "2001:db8::101" {
			t.Fatalf("unexpected pass rules: %+v", d.PassRules)
		}
	})

	// The external port is what arrives on the wire; IPv6 has no translation to
	// rewrite it, so a pass rule admits exactly that port.
	t.Run("pass rule admits the external port", func(t *testing.T) {
		d, err := ParseExposure(ExposureInput{
			Annotations: map[string]string{
				AnnExpose:       "true",
				AnnExternalPort: "27015",
				AnnInternalPort: "27015",
			},
			Addresses:       []string{"2001:db8::101"},
			DefaultProtocol: "udp",
		}, cfg)
		if err != nil {
			t.Fatal(err)
		}
		if len(d.PassRules) != 1 || d.PassRules[0].Port != "27015" {
			t.Fatalf("unexpected pass rules: %+v", d.PassRules)
		}
	})

	// Silently wiring up IPv4 only would leave IPv6 admitted on a port nothing
	// listens on, with no signal that half the exposure is dead.
	t.Run("port remap on an IPv6 address is rejected", func(t *testing.T) {
		_, err := ParseExposure(ExposureInput{
			Annotations: map[string]string{
				AnnExpose:       "true",
				AnnExternalPort: "27015",
				AnnInternalPort: "27016",
			},
			Addresses:       []string{"10.0.0.101", "2001:db8::101"},
			DefaultProtocol: "udp",
		}, cfg)
		if err == nil {
			t.Fatal("expected a port-remap rejection on a dual-stack object")
		}
	})

	t.Run("port remap is allowed without an IPv6 address", func(t *testing.T) {
		d, err := ParseExposure(ExposureInput{
			Annotations: map[string]string{
				AnnExpose:       "true",
				AnnExternalPort: "27015",
				AnnInternalPort: "27016",
			},
			Addresses:       []string{"10.0.0.101"},
			DefaultProtocol: "udp",
		}, cfg)
		if err != nil {
			t.Fatal(err)
		}
		if d.PortForward == nil || d.PortForward.LocalPort != "27016" {
			t.Fatalf("IPv4-only remap must still work: %+v", d.PortForward)
		}
	})

	t.Run("multiple IPv6 addresses each get a pass rule", func(t *testing.T) {
		d, err := ParseExposure(ExposureInput{
			Annotations:     map[string]string{AnnExpose: "true"},
			Addresses:       []string{"2001:db8::101", "2001:db8::102"},
			DefaultPort:     "443",
			DefaultProtocol: "tcp",
		}, cfg)
		if err != nil {
			t.Fatal(err)
		}
		if len(d.PassRules) != 2 ||
			d.PassRules[0].Destination != "2001:db8::101" ||
			d.PassRules[1].Destination != "2001:db8::102" {
			t.Fatalf("unexpected pass rules: %+v", d.PassRules)
		}
	})

	t.Run("hostname without expose creates no pass rule", func(t *testing.T) {
		d, err := ParseExposure(ExposureInput{
			Annotations: map[string]string{AnnHostname: "media.lab"},
			Addresses:   []string{"2001:db8::101"},
		}, cfg)
		if err != nil {
			t.Fatal(err)
		}
		if len(d.PassRules) != 0 {
			t.Fatalf("DNS alone must not open the firewall: %+v", d.PassRules)
		}
	})

	t.Run("normalizes and deduplicates addresses", func(t *testing.T) {
		d, err := ParseExposure(ExposureInput{
			Annotations: map[string]string{AnnHostname: "media.lab"},
			Addresses:   []string{"2001:0db8::1", "2001:db8::1", "::ffff:10.0.0.101"},
		}, cfg)
		if err != nil {
			t.Fatal(err)
		}
		if len(d.Hosts) != 2 || d.Hosts[0].Address != "2001:db8::1" ||
			d.Hosts[1].Address != "10.0.0.101" || d.Hosts[1].RecordType != "A" {
			t.Fatalf("unexpected normalized hosts: %+v", d.Hosts)
		}
	})
}

func TestLoadBalancerAddresses(t *testing.T) {
	svc := &corev1.Service{Status: corev1.ServiceStatus{LoadBalancer: corev1.LoadBalancerStatus{
		Ingress: []corev1.LoadBalancerIngress{
			{IP: "10.0.0.101"},
			{IP: "2001:db8::101"},
			{Hostname: "ignored.example.com"},
		},
	}}}
	want := []string{"10.0.0.101", "2001:db8::101"}
	got := loadBalancerAddresses(svc)
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("loadBalancerAddresses = %v, want %v", got, want)
	}
}
