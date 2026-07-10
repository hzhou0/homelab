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
	mu       sync.Mutex
	hostRows []map[string]string
	natRows  []map[string]string
	calls    []string
	addBody  map[string]json.RawMessage // last add body per kind
}

func newFake() *fakeOPN {
	return &fakeOPN{addBody: map[string]json.RawMessage{}}
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
		[]HostOverride{{Host: "grafana", Domain: "lab", Address: "10.0.0.100"}},
		&PortForward{Interface: "wan", Protocol: "tcp", ExternalPort: "443", TargetIP: "10.0.0.100", LocalPort: "443"},
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
			Port        string `json:"port"`
			Target      string `json:"target"`
			Sequence    string `json:"sequence"`
			Descr       string `json:"descr"`
			Description string `json:"description"`
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
	if natBody.Rule.Port != "443" || natBody.Rule.Target != "10.0.0.100" || natBody.Rule.Protocol != "tcp" {
		t.Errorf("unexpected nat add body: %+v", natBody.Rule)
	}
}

func TestSyncIdempotentNoChange(t *testing.T) {
	owner := Owner{Kind: "Service", Namespace: "app-grafana", Name: "grafana"}
	ho := HostOverride{Host: "grafana", Domain: "lab", Address: "10.0.0.100"}

	f := newFake()
	// Pre-seed an existing row whose description already matches desired.
	f.hostRows = []map[string]string{
		{"uuid": "existing-uuid", "description": dnsDescription(owner, ho)},
	}
	c := newTestClient(t, f)

	if err := c.Sync(context.Background(), owner, []HostOverride{ho}, nil); err != nil {
		t.Fatal(err)
	}

	if f.called("dns.add") != 0 || f.called("dns.set") != 0 || f.called("dns.del") != 0 {
		t.Errorf("expected no DNS mutations; calls=%v", f.calls)
	}
	if f.called("dns.reconfigure") != 0 {
		t.Errorf("expected no reconfigure when nothing changed; calls=%v", f.calls)
	}
}

func TestSyncUpdatesDriftedDNS(t *testing.T) {
	owner := Owner{Kind: "Service", Namespace: "app-grafana", Name: "grafana"}
	old := HostOverride{Host: "grafana", Domain: "lab", Address: "10.0.0.99"}
	want := HostOverride{Host: "grafana", Domain: "lab", Address: "10.0.0.100"}

	f := newFake()
	f.hostRows = []map[string]string{
		{"uuid": "existing-uuid", "description": dnsDescription(owner, old)},
	}
	c := newTestClient(t, f)

	if err := c.Sync(context.Background(), owner, []HostOverride{want}, nil); err != nil {
		t.Fatal(err)
	}
	if f.called("dns.set") != 1 {
		t.Errorf("expected one dns.set on drift; calls=%v", f.calls)
	}
	if f.called("dns.reconfigure") != 1 {
		t.Errorf("expected reconfigure after update; calls=%v", f.calls)
	}
}

func TestDeleteRemovesOwnedObjects(t *testing.T) {
	owner := Owner{Kind: "Service", Namespace: "app-foo", Name: "foo"}
	otherOwner := Owner{Kind: "Service", Namespace: "app-bar", Name: "bar"}
	ho := HostOverride{Host: "foo", Domain: "lab", Address: "10.0.0.100"}
	pf := PortForward{Interface: "wan", Protocol: "tcp", ExternalPort: "443", TargetIP: "10.0.0.100", LocalPort: "443"}

	f := newFake()
	f.hostRows = []map[string]string{
		{"uuid": "foo-dns", "description": dnsDescription(owner, ho)},
		{"uuid": "bar-dns", "description": dnsDescription(otherOwner, HostOverride{Host: "bar", Domain: "lab", Address: "10.0.0.101"})},
	}
	// DNAT search rows carry the description under `descr` (not `description`); the ownership match
	// must read it back through that key or an owned rule looks unowned and is never cleaned up.
	f.natRows = []map[string]string{
		{"uuid": "foo-nat", "descr": natDescription(owner, pf)},
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
	if f.called("dns.reconfigure") != 1 || f.called("nat.apply") != 1 {
		t.Errorf("expected reconfigure+apply after deletes, calls=%v", f.calls)
	}
}
