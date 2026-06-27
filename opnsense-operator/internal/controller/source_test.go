package controller

import (
	"testing"

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
			IP:          "10.0.0.100",
		}, cfg)
		if err != nil {
			t.Fatal(err)
		}
		if len(d.Hosts) != 1 || d.Hosts[0].FQDN() != "grafana.lab" || d.Hosts[0].Address != "10.0.0.100" {
			t.Fatalf("unexpected hosts: %+v", d.Hosts)
		}
		if d.PortForward != nil {
			t.Fatalf("expected no port-forward, got %+v", d.PortForward)
		}
	})

	t.Run("wildcard and multi-host", func(t *testing.T) {
		d, err := ParseExposure(ExposureInput{
			Annotations: map[string]string{AnnHostname: "*.lab, grafana.home.example.com"},
			IP:          "10.0.0.100",
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
			IP:          "10.0.0.100",
		}, cfg)
		if err == nil {
			t.Fatal("expected domain-filter rejection")
		}
	})

	t.Run("expose with defaults", func(t *testing.T) {
		d, err := ParseExposure(ExposureInput{
			Annotations:     map[string]string{AnnExpose: "true"},
			IP:              "10.0.0.100",
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
			IP:              "10.0.0.101",
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
			IP:              "10.0.0.100",
			DefaultProtocol: "tcp",
		}, cfg)
		if err == nil {
			t.Fatal("expected error when no port available")
		}
	})

	t.Run("invalid protocol errors", func(t *testing.T) {
		_, err := ParseExposure(ExposureInput{
			Annotations:     map[string]string{AnnExpose: "true", AnnProtocol: "sctp"},
			IP:              "10.0.0.100",
			DefaultPort:     "443",
			DefaultProtocol: "tcp",
		}, cfg)
		if err == nil {
			t.Fatal("expected invalid-protocol error")
		}
	})

	t.Run("empty is empty", func(t *testing.T) {
		d, err := ParseExposure(ExposureInput{Annotations: map[string]string{}, IP: "10.0.0.100"}, cfg)
		if err != nil {
			t.Fatal(err)
		}
		if !d.Empty() {
			t.Fatalf("expected empty exposure, got %+v", d)
		}
	})
}
