package opnsense

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/hzhou0/homelab/opnsense-operator/internal/config"
)

// fakeOPN is a minimal stand-in for the OPNsense API. It routes by path
// substring (the generated client's paths embed the action name) and records
// every call so tests can assert what the wrapper did.
type fakeOPN struct {
	mu         sync.Mutex
	hostRows   []map[string]string
	natRows    []map[string]string
	filterRows []map[string]string
	calls      []string
	addBody    map[string]json.RawMessage // last add body per kind
	addBodies  map[string][]json.RawMessage
}

func newFake() *fakeOPN {
	return &fakeOPN{
		addBody:   map[string]json.RawMessage{},
		addBodies: map[string][]json.RawMessage{},
	}
}

func (f *fakeOPN) record(token string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, token)
}

func (f *fakeOPN) called(token string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, c := range f.calls {
		if c == token {
			n++
		}
	}
	return n
}

func (f *fakeOPN) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := r.URL.Path
		body, _ := io.ReadAll(r.Body)
		switch {
		case strings.Contains(p, "search_host_override"):
			f.record("dns.search")
			writeRows(w, f.hostRows)
		case strings.Contains(p, "add_host_override"):
			f.record("dns.add")
			f.mu.Lock()
			f.addBody["dns"] = body
			f.addBodies["dns"] = append(f.addBodies["dns"], body)
			f.mu.Unlock()
			writeSaved(w, "new-dns-uuid")
		case strings.Contains(p, "set_host_override"):
			f.record("dns.set")
			writeSaved(w, "")
		case strings.Contains(p, "del_host_override"):
			f.record("dns.del")
			writeJSON(w, map[string]string{"result": "deleted"})
		case strings.Contains(p, "unbound/service/reconfigure"):
			f.record("dns.reconfigure")
			writeJSON(w, map[string]string{"status": "ok"})
		case strings.Contains(p, "d_nat/search_rule"):
			f.record("nat.search")
			writeRows(w, f.natRows)
		case strings.Contains(p, "d_nat/add_rule"):
			f.record("nat.add")
			f.mu.Lock()
			f.addBody["nat"] = body
			f.mu.Unlock()
			writeSaved(w, "new-nat-uuid")
		case strings.Contains(p, "d_nat/set_rule"):
			f.record("nat.set")
			writeSaved(w, "")
		case strings.Contains(p, "d_nat/del_rule"):
			f.record("nat.del")
			writeJSON(w, map[string]string{"result": "deleted"})
		case strings.Contains(p, "d_nat/apply"):
			f.record("nat.apply")
			writeJSON(w, map[string]string{"status": "ok"})
		case strings.Contains(p, "filter/search_rule"):
			f.record("filter.search")
			writeRows(w, f.filterRows)
		case strings.Contains(p, "filter/add_rule"):
			f.record("filter.add")
			f.mu.Lock()
			f.addBody["filter"] = body
			f.addBodies["filter"] = append(f.addBodies["filter"], body)
			f.mu.Unlock()
			writeSaved(w, "new-filter-uuid")
		case strings.Contains(p, "filter/set_rule"):
			f.record("filter.set")
			writeSaved(w, "")
		case strings.Contains(p, "filter/del_rule"):
			f.record("filter.del")
			writeJSON(w, map[string]string{"result": "deleted"})
		case strings.Contains(p, "filter/apply"):
			f.record("filter.apply")
			writeJSON(w, map[string]string{"status": "ok"})
		default:
			http.Error(w, "unhandled path: "+p, http.StatusNotFound)
		}
	})
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func writeRows(w http.ResponseWriter, rows []map[string]string) {
	writeJSON(w, map[string]any{"rows": rows, "total": len(rows)})
}

func writeSaved(w http.ResponseWriter, uuid string) {
	writeJSON(w, map[string]string{"result": "saved", "uuid": uuid})
}

func newTestClient(t *testing.T, f *fakeOPN) *Client {
	t.Helper()
	srv := httptest.NewServer(f.handler())
	t.Cleanup(srv.Close)
	c, err := New(&config.Config{
		OPNsenseURL:  srv.URL,
		APIKey:       "k",
		APISecret:    "s",
		WANInterface: "wan",
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return c
}

func TestSyncCreatesDNSAndNAT(t *testing.T) {
	f := newFake()
	c := newTestClient(t, f)
	owner := Owner{Kind: "Service", Namespace: "app-grafana", Name: "grafana"}

	err := c.Sync(context.Background(), owner,
		[]HostOverride{{Host: "grafana", Domain: "lab", Address: "10.0.0.100", RecordType: "A"}},
		&PortForward{Interface: "wan", Protocol: "tcp", ExternalPort: "443", TargetIP: "10.0.0.100", LocalPort: "443"},
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}

	if f.called("dns.add") != 1 {
		t.Errorf("dns.add called %d times, want 1", f.called("dns.add"))
	}
	if f.called("dns.reconfigure") != 1 {
		t.Errorf("dns.reconfigure called %d times, want 1", f.called("dns.reconfigure"))
	}
	if f.called("nat.add") != 1 {
		t.Errorf("nat.add called %d times, want 1", f.called("nat.add"))
	}
	if f.called("nat.apply") != 1 {
		t.Errorf("nat.apply called %d times, want 1", f.called("nat.apply"))
	}

	// The DNS add body should carry our host/domain/server and a description.
	var dnsBody struct {
		Host struct {
			Hostname, Domain, Server, Description, Rr string
		}
	}
	if err := json.Unmarshal(f.addBody["dns"], &dnsBody); err != nil {
		t.Fatalf("decode dns add body: %v", err)
	}
	if dnsBody.Host.Hostname != "grafana" || dnsBody.Host.Domain != "lab" ||
		dnsBody.Host.Server != "10.0.0.100" || dnsBody.Host.Rr != "A" {
		t.Errorf("unexpected dns add body: %+v", dnsBody.Host)
	}
	if !strings.Contains(dnsBody.Host.Description, "owner=Service/app-grafana/grafana") {
		t.Errorf("dns description missing owner tag: %q", dnsBody.Host.Description)
	}

	// The NAT add body must (1) carry a non-empty sequence — OPNsense rejects it as required and it
	// is non-omitempty, so a zero value serializes and fails — and (2) put the operator description
	// in `descr`, the field OPNsense persists; writing `description` leaves the stored rule blank,
	// which breaks ownership matching and causes a duplicate rule every reconcile.
	var natBody struct {
		Rule struct {
			Protocol    string `json:"protocol"`
			Target      string `json:"target"`
			Sequence    string `json:"sequence"`
			Descr       string `json:"descr"`
			Description string `json:"description"`
			// The match port is destination.port, not a flat rule-level port.
			Destination struct {
				Port string `json:"port"`
			} `json:"destination"`
		}
	}
	if err := json.Unmarshal(f.addBody["nat"], &natBody); err != nil {
		t.Fatalf("decode nat add body: %v", err)
	}
	if natBody.Rule.Sequence == "" {
		t.Errorf("nat add body missing required sequence: %s", f.addBody["nat"])
	}
	if !strings.Contains(natBody.Rule.Descr, "owner=Service/app-grafana/grafana") {
		t.Errorf("nat descr must carry the owner tag (got descr=%q description=%q)", natBody.Rule.Descr, natBody.Rule.Description)
	}
	if natBody.Rule.Destination.Port != "443" || natBody.Rule.Target != "10.0.0.100" || natBody.Rule.Protocol != "tcp" {
		t.Errorf("unexpected nat add body: %+v", natBody.Rule)
	}
}

func TestSyncIdempotentNoChange(t *testing.T) {
	owner := Owner{Kind: "Service", Namespace: "app-grafana", Name: "grafana"}
	ho := HostOverride{Host: "grafana", Domain: "lab", Address: "10.0.0.100", RecordType: "A"}

	f := newFake()
	// Pre-seed an existing row whose description already matches desired.
	f.hostRows = []map[string]string{
		{"uuid": "existing-uuid", "description": dnsDescription(owner, ho)},
	}
	c := newTestClient(t, f)

	if err := c.Sync(context.Background(), owner, []HostOverride{ho}, nil, nil); err != nil {
		t.Fatal(err)
	}

	if f.called("dns.add") != 0 || f.called("dns.set") != 0 || f.called("dns.del") != 0 {
		t.Errorf("expected no DNS mutations; calls=%v", f.calls)
	}
	if f.called("dns.reconfigure") != 0 {
		t.Errorf("expected no reconfigure when nothing changed; calls=%v", f.calls)
	}
}

func TestSyncReplacesChangedDNSAddress(t *testing.T) {
	owner := Owner{Kind: "Service", Namespace: "app-grafana", Name: "grafana"}
	old := HostOverride{Host: "grafana", Domain: "lab", Address: "10.0.0.99", RecordType: "A"}
	want := HostOverride{Host: "grafana", Domain: "lab", Address: "10.0.0.100", RecordType: "A"}

	f := newFake()
	f.hostRows = []map[string]string{
		{"uuid": "existing-uuid", "description": dnsDescription(owner, old)},
	}
	c := newTestClient(t, f)

	if err := c.Sync(context.Background(), owner, []HostOverride{want}, nil, nil); err != nil {
		t.Fatal(err)
	}
	if f.called("dns.add") != 1 || f.called("dns.del") != 1 || f.called("dns.set") != 0 {
		t.Errorf("expected changed address to be added and the old one deleted; calls=%v", f.calls)
	}
	if f.called("dns.reconfigure") != 1 {
		t.Errorf("expected reconfigure after update; calls=%v", f.calls)
	}
}

func TestDeleteRemovesOwnedObjects(t *testing.T) {
	owner := Owner{Kind: "Service", Namespace: "app-foo", Name: "foo"}
	otherOwner := Owner{Kind: "Service", Namespace: "app-bar", Name: "bar"}
	ho := HostOverride{Host: "foo", Domain: "lab", Address: "10.0.0.100", RecordType: "A"}
	pf := PortForward{Interface: "wan", Protocol: "tcp", ExternalPort: "443", TargetIP: "10.0.0.100", LocalPort: "443"}
	pass := PassRule{Interface: "wan", Protocol: "tcp", Destination: "2001:db8::100", Port: "443"}

	f := newFake()
	f.hostRows = []map[string]string{
		{"uuid": "foo-dns", "description": dnsDescription(owner, ho)},
		{"uuid": "bar-dns", "description": dnsDescription(otherOwner, HostOverride{Host: "bar", Domain: "lab", Address: "10.0.0.101", RecordType: "A"})},
	}
	// DNAT search rows carry the description under `descr` (not `description`); the ownership match
	// must read it back through that key or an owned rule looks unowned and is never cleaned up.
	f.natRows = []map[string]string{
		{"uuid": "foo-nat", "descr": natDescription(owner, pf)},
	}
	f.filterRows = []map[string]string{
		{"uuid": "foo-filter", "description": passDescription(owner, pass)},
		{"uuid": "bar-filter", "description": passDescription(otherOwner, pass)},
	}
	c := newTestClient(t, f)

	if err := c.Delete(context.Background(), owner); err != nil {
		t.Fatal(err)
	}
	if f.called("dns.del") != 1 {
		t.Errorf("expected 1 dns.del (only owned), calls=%v", f.calls)
	}
	if f.called("nat.del") != 1 {
		t.Errorf("expected 1 nat.del, calls=%v", f.calls)
	}
	if f.called("filter.del") != 1 {
		t.Errorf("expected 1 filter.del (only owned), calls=%v", f.calls)
	}
	if f.called("dns.reconfigure") != 1 || f.called("nat.apply") != 1 || f.called("filter.apply") != 1 {
		t.Errorf("expected reconfigure+apply after deletes, calls=%v", f.calls)
	}
}

func TestSyncCreatesDualStackDNS(t *testing.T) {
	f := newFake()
	c := newTestClient(t, f)
	owner := Owner{Kind: "Service", Namespace: "app-media", Name: "media"}

	err := c.Sync(context.Background(), owner, []HostOverride{
		{Host: "media", Domain: "lab", Address: "10.0.0.101", RecordType: "A"},
		{Host: "media", Domain: "lab", Address: "2001:db8::101", RecordType: "AAAA"},
	}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if f.called("dns.add") != 2 || f.called("dns.reconfigure") != 1 {
		t.Fatalf("expected two adds and one reconfigure; calls=%v", f.calls)
	}

	got := map[string]string{}
	for _, raw := range f.addBodies["dns"] {
		var body struct {
			Host struct {
				Server string `json:"server"`
				Rr     string `json:"rr"`
			}
		}
		if err := json.Unmarshal(raw, &body); err != nil {
			t.Fatalf("decode DNS body: %v", err)
		}
		got[body.Host.Server] = body.Host.Rr
	}
	if got["10.0.0.101"] != "A" || got["2001:db8::101"] != "AAAA" {
		t.Fatalf("unexpected record types: %v", got)
	}
}

func TestSyncRejectsIPv6DNAT(t *testing.T) {
	f := newFake()
	c := newTestClient(t, f)
	owner := Owner{Kind: "Service", Namespace: "app-media", Name: "media"}
	pf := &PortForward{
		Interface:    "wan",
		Protocol:     "udp",
		ExternalPort: "3478",
		TargetIP:     "2001:db8::101",
		LocalPort:    "3478",
	}

	err := c.Sync(context.Background(), owner, nil, pf, nil)
	if err == nil || !strings.Contains(err.Error(), "is not IPv4") {
		t.Fatalf("expected IPv6 DNAT rejection, got %v", err)
	}
	if len(f.calls) != 0 {
		t.Fatalf("IPv6 target must be rejected before OPNsense calls; calls=%v", f.calls)
	}
}

func TestSyncAdoptsLegacyDNSDescription(t *testing.T) {
	owner := Owner{Kind: "Service", Namespace: "app-media", Name: "media"}
	want := HostOverride{Host: "media", Domain: "lab", Address: "10.0.0.101", RecordType: "A"}
	legacyDescription := ManagedPrefix + " owner=" + owner.Tag() + " host=media.lab ip=10.0.0.101"

	f := newFake()
	f.hostRows = []map[string]string{{"uuid": "legacy", "description": legacyDescription}}
	c := newTestClient(t, f)
	if err := c.Sync(context.Background(), owner, []HostOverride{want}, nil, nil); err != nil {
		t.Fatal(err)
	}
	if f.called("dns.set") != 1 || f.called("dns.add") != 0 || f.called("dns.del") != 0 {
		t.Fatalf("legacy row should be adopted and updated; calls=%v", f.calls)
	}
}

func TestSyncCreatesPassRule(t *testing.T) {
	f := newFake()
	c := newTestClient(t, f)
	owner := Owner{Kind: "Service", Namespace: "app-media", Name: "media"}
	pass := PassRule{Interface: "wan", Protocol: "udp", Destination: "2001:db8::101", Port: "3478"}

	if err := c.Sync(context.Background(), owner, nil, nil, []PassRule{pass}); err != nil {
		t.Fatal(err)
	}
	if f.called("filter.add") != 1 || f.called("filter.apply") != 1 {
		t.Fatalf("expected one add and one apply; calls=%v", f.calls)
	}
	if f.called("nat.add") != 0 {
		t.Fatalf("a pass rule must not create DNAT; calls=%v", f.calls)
	}

	// Every required non-omitempty flag must be populated, and the protocol must
	// be upper case: the filter model rejects the lower-case spelling DNAT uses.
	var body struct {
		Rule struct {
			Action          string `json:"action"`
			Direction       string `json:"direction"`
			Ipprotocol      string `json:"ipprotocol"`
			Protocol        string `json:"protocol"`
			Statetype       string `json:"statetype"`
			Enabled         string `json:"enabled"`
			Quick           string `json:"quick"`
			Sequence        string `json:"sequence"`
			Interface       string `json:"interface"`
			SourceNet       string `json:"source_net"`
			DestinationNet  string `json:"destination_net"`
			DestinationPort string `json:"destination_port"`
			Description     string `json:"description"`
		}
	}
	if err := json.Unmarshal(f.addBody["filter"], &body); err != nil {
		t.Fatalf("decode filter add body: %v", err)
	}
	r := body.Rule
	if r.Action != "pass" || r.Direction != "in" || r.Ipprotocol != "inet6" || r.Protocol != "UDP" ||
		r.Statetype != "keep" || r.Enabled != "1" || r.Quick != "1" || r.Sequence == "" {
		t.Errorf("unexpected filter rule: %+v", r)
	}
	if r.Interface != "wan" || r.SourceNet != "any" ||
		r.DestinationNet != "2001:db8::101" || r.DestinationPort != "3478" {
		t.Errorf("unexpected filter match: %+v", r)
	}
	if !strings.Contains(r.Description, "owner=Service/app-media/media") {
		t.Errorf("filter description missing owner tag: %q", r.Description)
	}
}

func TestSyncReplacesChangedPassDestination(t *testing.T) {
	owner := Owner{Kind: "Service", Namespace: "app-media", Name: "media"}
	old := PassRule{Interface: "wan", Protocol: "udp", Destination: "2001:db8::99", Port: "3478"}
	want := PassRule{Interface: "wan", Protocol: "udp", Destination: "2001:db8::101", Port: "3478"}

	f := newFake()
	f.filterRows = []map[string]string{
		{"uuid": "stale", "description": passDescription(owner, old)},
	}
	c := newTestClient(t, f)
	if err := c.Sync(context.Background(), owner, nil, nil, []PassRule{want}); err != nil {
		t.Fatal(err)
	}
	if f.called("filter.add") != 1 || f.called("filter.del") != 1 {
		t.Fatalf("a renumbered address must not leave the old rule behind; calls=%v", f.calls)
	}
}

func TestSyncPassRuleIdempotent(t *testing.T) {
	owner := Owner{Kind: "Service", Namespace: "app-media", Name: "media"}
	pass := PassRule{Interface: "wan", Protocol: "udp", Destination: "2001:db8::101", Port: "3478"}

	f := newFake()
	f.filterRows = []map[string]string{
		{"uuid": "existing", "description": passDescription(owner, pass)},
	}
	c := newTestClient(t, f)
	if err := c.Sync(context.Background(), owner, nil, nil, []PassRule{pass}); err != nil {
		t.Fatal(err)
	}
	if f.called("filter.add")+f.called("filter.set")+f.called("filter.del") != 0 {
		t.Errorf("steady state must not mutate; calls=%v", f.calls)
	}
	if f.called("filter.apply") != 0 {
		t.Errorf("no apply without a mutation; calls=%v", f.calls)
	}
}

func TestSyncUpdatesDriftedPassRule(t *testing.T) {
	owner := Owner{Kind: "Service", Namespace: "app-media", Name: "media"}
	pass := PassRule{Interface: "wan", Protocol: "udp", Destination: "2001:db8::101", Port: "3478"}
	drifted := passDescription(owner, PassRule{
		Interface: "wan", Protocol: "tcp", Destination: "2001:db8::101", Port: "3478",
	})

	f := newFake()
	f.filterRows = []map[string]string{{"uuid": "drifted", "description": drifted}}
	c := newTestClient(t, f)
	if err := c.Sync(context.Background(), owner, nil, nil, []PassRule{pass}); err != nil {
		t.Fatal(err)
	}
	if f.called("filter.set") != 1 || f.called("filter.add") != 0 || f.called("filter.del") != 0 {
		t.Fatalf("same destination should be corrected in place; calls=%v", f.calls)
	}
	if f.called("filter.apply") != 1 {
		t.Fatalf("expected apply after update; calls=%v", f.calls)
	}
}

func TestSyncRejectsIPv4PassRule(t *testing.T) {
	f := newFake()
	c := newTestClient(t, f)
	owner := Owner{Kind: "Service", Namespace: "app-media", Name: "media"}
	pass := PassRule{Interface: "wan", Protocol: "tcp", Destination: "10.0.0.101", Port: "443"}

	err := c.Sync(context.Background(), owner, nil, nil, []PassRule{pass})
	if err == nil || !strings.Contains(err.Error(), "is not IPv6") {
		t.Fatalf("expected IPv4 pass-rule rejection, got %v", err)
	}
	if len(f.calls) != 0 {
		t.Fatalf("IPv4 destination must be rejected before OPNsense calls; calls=%v", f.calls)
	}
}
