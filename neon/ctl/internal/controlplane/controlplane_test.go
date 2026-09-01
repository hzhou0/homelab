package controlplane

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hzhou0/homelab/neon/ctl/internal/kube"
	"github.com/hzhou0/homelab/neon/ctl/internal/neon"
	"github.com/hzhou0/homelab/neon/ctl/internal/registry"
	"github.com/hzhou0/homelab/neon/ctl/internal/scram"
)

func newStore(t *testing.T) *registry.Store {
	t.Helper()
	store, err := registry.Open(filepath.Join(t.TempDir(), "registry.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	return store
}

const (
	tenantHex   = "1a2b3344556677881122334455667788"
	timelineHex = "aa223344556677881122334455667788"
)

func mustTenant(t *testing.T) neon.TenantID {
	t.Helper()
	id, err := neon.ParseTenantID(tenantHex)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func mustTimeline(t *testing.T) neon.TimelineID {
	t.Helper()
	id, err := neon.ParseTimelineID(timelineHex)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

type fakeRuntime struct {
	mu        sync.Mutex
	instances map[string]*kube.Instance
	readyOn   bool
	failList  error
	ensures   int
}

func newFakeRuntime() *fakeRuntime {
	return &fakeRuntime{instances: map[string]*kube.Instance{}, readyOn: true}
}

func (f *fakeRuntime) add(instance kube.Instance) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.instances[instance.ID] = &instance
}

func (f *fakeRuntime) PgVersions() []int { return []int{16, 17} }

func (f *fakeRuntime) Get(ctx context.Context, id string) (*kube.Instance, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	instance, ok := f.instances[id]
	if !ok {
		return nil, fmt.Errorf("%w: %s", kube.ErrNotFound, id)
	}
	copied := *instance
	return &copied, nil
}

func (f *fakeRuntime) List(ctx context.Context) ([]kube.Instance, error) {
	return f.filter(func(*kube.Instance) bool { return true })
}

func (f *fakeRuntime) ListByTenant(ctx context.Context, tenant neon.TenantID) ([]kube.Instance, error) {
	return f.filter(func(i *kube.Instance) bool { return i.TenantID == tenant })
}

func (f *fakeRuntime) ListByTimeline(ctx context.Context, tenant neon.TenantID, timeline neon.TimelineID) ([]kube.Instance, error) {
	return f.filter(func(i *kube.Instance) bool { return i.TenantID == tenant && i.TimelineID == timeline })
}

func (f *fakeRuntime) filter(keep func(*kube.Instance) bool) ([]kube.Instance, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failList != nil {
		return nil, f.failList
	}
	var matched []kube.Instance
	for _, instance := range f.instances {
		if keep(instance) {
			matched = append(matched, *instance)
		}
	}
	return matched, nil
}

func (f *fakeRuntime) Ensure(ctx context.Context, binding kube.Binding, pgVersion int) (*kube.Instance, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ensures++
	instance, ok := f.instances[binding.ID]
	if !ok {
		instance = &kube.Instance{
			ControlURL: "http://compute.invalid:3080",
			PgAddress:  binding.ID + ".neon.svc.cluster.local:55433",
		}
		f.instances[binding.ID] = instance
	}
	instance.Binding = binding
	instance.Replicas = 1
	instance.Ready = f.readyOn
	copied := *instance
	return &copied, nil
}

func (f *fakeRuntime) Scale(ctx context.Context, id string, replicas int32) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	instance, ok := f.instances[id]
	if !ok {
		return fmt.Errorf("%w: %s", kube.ErrNotFound, id)
	}
	instance.Replicas = replicas
	instance.Ready = replicas > 0 && f.readyOn
	return nil
}

func (f *fakeRuntime) Delete(ctx context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.instances, id)
	return nil
}

// fakeStorcon answers the four controller reads the spec path depends on, and can be told to
// report a placement that has not caught up with what it just announced.
type fakeStorcon struct {
	server *httptest.Server

	mu         sync.Mutex
	shardNode  neon.NodeID
	generation uint32
	safekeeper []neon.SafekeeperDescribe
	created    []neon.TimelineCreateRequest
	status     int

	timelineRefused bool
	deletedTenants  []string
}

func newFakeStorcon(t *testing.T) *fakeStorcon {
	t.Helper()
	fake := &fakeStorcon{
		shardNode:  1,
		generation: 3,
		safekeeper: []neon.SafekeeperDescribe{
			{ID: 11, Host: "sk-0.neon", Port: 5454},
			{ID: 12, Host: "sk-1.neon", Port: 5454},
			{ID: 13, Host: "sk-2.neon", Port: 5454},
		},
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /control/v1/node", func(w http.ResponseWriter, r *http.Request) {
		fake.respond(w, []any{})
	})
	mux.HandleFunc("GET /control/v1/safekeeper", func(w http.ResponseWriter, r *http.Request) {
		fake.mu.Lock()
		safekeepers := fake.safekeeper
		fake.mu.Unlock()
		fake.respond(w, safekeepers)
	})
	mux.HandleFunc("GET /debug/v1/tenant/{tenant}/locate", func(w http.ResponseWriter, r *http.Request) {
		fake.mu.Lock()
		node := fake.shardNode
		fake.mu.Unlock()
		fake.respond(w, neon.TenantLocateResponse{
			Shards: []neon.TenantLocateShard{{
				ShardID:      r.PathValue("tenant"),
				NodeID:       node,
				ListenPgAddr: fmt.Sprintf("ps-%d.neon", node-1),
				ListenPgPort: 6400,
			}},
		})
	})
	mux.HandleFunc("GET /debug/v1/tenant/{tenant}/timeline/{timeline}/locate", func(w http.ResponseWriter, r *http.Request) {
		fake.mu.Lock()
		generation := fake.generation
		fake.mu.Unlock()
		fake.respond(w, neon.TimelineLocateResponse{Generation: generation, SkSet: []neon.NodeID{11, 12, 13}})
	})

	mux.HandleFunc("POST /v1/tenant", func(w http.ResponseWriter, r *http.Request) {
		fake.respond(w, map[string]string{})
	})
	mux.HandleFunc("POST /v1/tenant/{tenant}/timeline", func(w http.ResponseWriter, r *http.Request) {
		var request neon.TimelineCreateRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		fake.mu.Lock()
		refused := fake.timelineRefused
		if !refused {
			fake.created = append(fake.created, request)
		}
		fake.mu.Unlock()
		if refused {
			http.Error(w, "no safekeepers", http.StatusInternalServerError)
			return
		}
		fake.respond(w, map[string]string{})
	})
	mux.HandleFunc("DELETE /v1/tenant/{tenant}", func(w http.ResponseWriter, r *http.Request) {
		fake.mu.Lock()
		fake.deletedTenants = append(fake.deletedTenants, r.PathValue("tenant"))
		fake.mu.Unlock()
		writeJSON(w, http.StatusOK, map[string]string{})
	})
	mux.HandleFunc("DELETE /v1/tenant/{tenant}/timeline/{timeline}", func(w http.ResponseWriter, r *http.Request) {
		fake.respond(w, map[string]string{})
	})

	fake.server = httptest.NewServer(mux)
	t.Cleanup(fake.server.Close)
	return fake
}

func (f *fakeStorcon) respond(w http.ResponseWriter, body any) {
	f.mu.Lock()
	status := f.status
	f.mu.Unlock()
	if status != 0 {
		w.WriteHeader(status)
		return
	}
	writeJSON(w, http.StatusOK, body)
}

func (f *fakeStorcon) client(t *testing.T) *neon.StorageController {
	t.Helper()
	client, err := neon.NewStorageController(f.server.URL, "", f.server.Client())
	if err != nil {
		t.Fatal(err)
	}
	return client
}

// fakeCompute stands in for compute_ctl, recording what was pushed to it.
type fakeCompute struct {
	server *httptest.Server

	mu            sync.Mutex
	configured    []neon.ComputeSpec
	configureFail bool
	status        neon.ComputeStatus
	lastActive    *time.Time
	terminated    bool
}

func newFakeCompute(t *testing.T) *fakeCompute {
	t.Helper()
	fake := &fakeCompute{status: neon.ComputeRunning}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /configure", func(w http.ResponseWriter, r *http.Request) {
		fake.mu.Lock()
		defer fake.mu.Unlock()
		if fake.configureFail {
			http.Error(w, "no", http.StatusInternalServerError)
			return
		}
		body, _ := io.ReadAll(r.Body)
		var config neon.ComputeConfig
		if err := json.Unmarshal(body, &config); err != nil || config.Spec == nil {
			http.Error(w, "malformed", http.StatusBadRequest)
			return
		}
		fake.configured = append(fake.configured, *config.Spec)
		writeJSON(w, http.StatusOK, map[string]string{})
	})
	mux.HandleFunc("GET /status", func(w http.ResponseWriter, r *http.Request) {
		fake.mu.Lock()
		defer fake.mu.Unlock()
		writeJSON(w, http.StatusOK, neon.ComputeStatusResponse{
			StartTime:  time.Now().Add(-time.Hour),
			Status:     fake.status,
			LastActive: fake.lastActive,
		})
	})
	mux.HandleFunc("POST /terminate", func(w http.ResponseWriter, r *http.Request) {
		fake.mu.Lock()
		defer fake.mu.Unlock()
		fake.terminated = true
		writeJSON(w, http.StatusOK, map[string]any{"lsn": nil})
	})

	fake.server = httptest.NewServer(mux)
	t.Cleanup(fake.server.Close)
	return fake
}

func (f *fakeCompute) specs() []neon.ComputeSpec {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]neon.ComputeSpec{}, f.configured...)
}

func newTestServer(t *testing.T, storcon *fakeStorcon, store *registry.Store, runtime *fakeRuntime, computes *fakeCompute) *Server {
	t.Helper()
	seed, err := neon.NewSeed()
	if err != nil {
		t.Fatal(err)
	}
	key, err := neon.NewSigningKey(seed)
	if err != nil {
		t.Fatal(err)
	}
	server := New(storcon.client(t), store, runtime, key, nil, slog.New(slog.NewTextHandler(io.Discard, nil)), Options{
		WakeTimeout:    2 * time.Second,
		SuspendTimeout: time.Minute,
	})
	if computes != nil {
		server.computeClient = func(instance *kube.Instance) (*neon.ComputeCtl, error) {
			return neon.NewComputeCtl(computes.server.URL, instance.ID, key, computes.server.Client())
		}
	}
	return server
}

func seedBranch(t *testing.T, store *registry.Store) *registry.Branch {
	t.Helper()
	branch := &registry.Branch{
		Name:       "main",
		TenantID:   mustTenant(t),
		TimelineID: mustTimeline(t),
		PgVersion:  17,
		Mode:       neon.ComputeMode{Kind: neon.ModePrimary},
		Roles:      []registry.Role{{Name: "app", Verifier: "SCRAM-SHA-256$4096:c2FsdA==$c3RvcmVk:c2VydmVy"}},
		Databases:  []registry.Database{{Name: "appdb", Owner: "app"}},
		Settings:   []registry.Setting{{Name: "max_connections", Value: "100", VarType: "integer"}},
	}
	if err := store.Put(context.Background(), branch); err != nil {
		t.Fatal(err)
	}
	return branch
}

func seedCompute(t *testing.T, runtime *fakeRuntime, running bool) kube.Instance {
	t.Helper()
	instance := kube.Instance{
		Binding: kube.Binding{
			ID:         "main",
			TenantID:   mustTenant(t),
			TimelineID: mustTimeline(t),
			Mode:       neon.ComputeMode{Kind: neon.ModePrimary},
		},
		ControlURL: "http://compute-main.neon:3080",
		PgAddress:  "compute-main.neon:55433",
	}
	if running {
		instance.Replicas = 1
		instance.Ready = true
	}
	runtime.add(instance)
	return instance
}

func do(t *testing.T, server *Server, method, target string, body string) *httptest.ResponseRecorder {
	t.Helper()
	var reader io.Reader
	if body != "" {
		reader = strings.NewReader(body)
	}
	request := httptest.NewRequest(method, target, reader)
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, request)
	return recorder
}

func unboundInstance() kube.Instance {
	return kube.Instance{
		Binding:    kube.Binding{ID: "unbound"},
		ControlURL: "http://compute-unbound.neon:3080",
		PgAddress:  "compute-unbound.neon:55433",
		Replicas:   1,
		Ready:      true,
	}
}

var update = flag.Bool("update", false, "rewrite the golden spec")

// The spec is Neon's internal format and changes across releases. Pinning it turns a shape change
// into a test failure at upgrade rather than a compute that will not start.
func TestRenderSpecGolden(t *testing.T) {
	storcon := newFakeStorcon(t)
	store := newStore(t)
	branch := seedBranch(t, store)
	runtime := newFakeRuntime()
	instance := seedCompute(t, runtime, true)

	server := newTestServer(t, storcon, store, runtime, nil)

	spec, err := server.renderSpec(context.Background(), &instance)
	if err != nil {
		t.Fatal(err)
	}
	rendered, err := json.MarshalIndent(spec, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	rendered = append(rendered, '\n')

	golden := filepath.Join("testdata", "spec.golden.json")
	if *update {
		if err := os.MkdirAll("testdata", 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(golden, rendered, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(golden)
	if err != nil {
		t.Fatal(err)
	}
	if string(rendered) != string(want) {
		t.Errorf("rendered spec differs from %s:\n%s", golden, rendered)
	}
	_ = branch
}

// An unsharded tenant must carry no stripe size: supplying one alongside a single connection
// string is a spec Neon rejects, and regenerating the golden would not catch it.
func TestUnshardedSpecCarriesNoStripeSize(t *testing.T) {
	storcon := newFakeStorcon(t)
	store := newStore(t)
	seedBranch(t, store)
	runtime := newFakeRuntime()
	instance := seedCompute(t, runtime, true)
	server := newTestServer(t, storcon, store, runtime, nil)

	spec, err := server.renderSpec(context.Background(), &instance)
	if err != nil {
		t.Fatal(err)
	}
	if spec.ShardStripeSize != nil {
		t.Errorf("stripe size = %v, want none", *spec.ShardStripeSize)
	}
}

// A registry that is empty or failing must not reach a compute that is booting or recovering: the
// catalog lives in the timeline, so a spec with none mutates nothing and the compute still starts.
func TestRenderSpecDegradesWithoutRegistry(t *testing.T) {
	for _, tc := range []struct {
		name    string
		prepare func(t *testing.T, store *registry.Store)
	}{
		{"no entry", func(*testing.T, *registry.Store) {}},
		{"registry failing", func(t *testing.T, store *registry.Store) {
			seedBranch(t, store)
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			storcon := newFakeStorcon(t)
			store := newStore(t)
			runtime := newFakeRuntime()
			instance := seedCompute(t, runtime, true)
			server := newTestServer(t, storcon, store, runtime, nil)
			tc.prepare(t, store)

			spec, err := server.renderSpec(context.Background(), &instance)
			if err != nil {
				t.Fatalf("the spec path must not fail: %v", err)
			}
			if !spec.SkipPgCatalogUpdates {
				t.Error("a spec without catalog contents must skip catalog updates")
			}
			if len(spec.Cluster.Roles) != 0 || len(spec.Cluster.Databases) != 0 {
				t.Error("a degraded spec must not invent catalog contents")
			}
			if spec.TenantID == nil || spec.PageserverConnstring == nil || len(spec.SafekeeperConnstrings) == 0 {
				t.Error("a degraded spec must still carry placement")
			}
		})
	}
}

func TestSpecEndpointStatuses(t *testing.T) {
	storcon := newFakeStorcon(t)
	store := newStore(t)
	seedBranch(t, store)
	runtime := newFakeRuntime()
	seedCompute(t, runtime, true)
	server := newTestServer(t, storcon, store, runtime, nil)

	t.Run("attached", func(t *testing.T) {
		response := do(t, server, http.MethodGet, "/compute/api/v2/computes/main/spec", "")
		if response.Code != http.StatusOK {
			t.Fatalf("status = %d, body = %s", response.Code, response.Body)
		}
		var body struct {
			Spec   *json.RawMessage `json:"spec"`
			Status string           `json:"status"`
			Config struct {
				JWKS struct {
					Keys []json.RawMessage `json:"keys"`
				} `json:"jwks"`
			} `json:"compute_ctl_config"`
		}
		if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
			t.Fatal(err)
		}
		if body.Status != "attached" || body.Spec == nil {
			t.Errorf("status = %q, spec present = %v", body.Status, body.Spec != nil)
		}
		if body.Config.JWKS.Keys == nil {
			t.Error("compute_ctl_config must always carry a key set, empty or not")
		}
	})

	// compute_ctl gives up on 404 and 500 but backs off on 503, so an unknown compute is a 404
	// and an unresolvable one is retryable.
	t.Run("unknown compute", func(t *testing.T) {
		response := do(t, server, http.MethodGet, "/compute/api/v2/computes/absent/spec", "")
		if response.Code != http.StatusNotFound {
			t.Errorf("status = %d, want 404", response.Code)
		}
	})

	t.Run("controller unreachable", func(t *testing.T) {
		storcon.mu.Lock()
		storcon.status = http.StatusBadGateway
		storcon.mu.Unlock()
		defer func() {
			storcon.mu.Lock()
			storcon.status = 0
			storcon.mu.Unlock()
		}()

		response := do(t, server, http.MethodGet, "/compute/api/v2/computes/main/spec", "")
		if response.Code != http.StatusServiceUnavailable {
			t.Errorf("status = %d, want 503", response.Code)
		}
	})
}

func TestSpecEndpointReportsEmptyBinding(t *testing.T) {
	storcon := newFakeStorcon(t)
	runtime := newFakeRuntime()
	runtime.add(unboundInstance())
	server := newTestServer(t, storcon, newStore(t), runtime, nil)

	response := do(t, server, http.MethodGet, "/compute/api/v2/computes/unbound/spec", "")
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d", response.Code)
	}
	var body struct {
		Spec   json.RawMessage `json:"spec"`
		Status string          `json:"status"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Status != "empty" || string(body.Spec) != "null" {
		t.Errorf("status = %q, spec = %s", body.Status, body.Spec)
	}
}

func TestReadinessFollowsTheController(t *testing.T) {
	storcon := newFakeStorcon(t)
	server := newTestServer(t, storcon, newStore(t), newFakeRuntime(), nil)

	if response := do(t, server, http.MethodGet, "/readyz", ""); response.Code != http.StatusOK {
		t.Errorf("ready status = %d", response.Code)
	}

	storcon.mu.Lock()
	storcon.status = http.StatusInternalServerError
	storcon.mu.Unlock()

	if response := do(t, server, http.MethodGet, "/readyz", ""); response.Code != http.StatusServiceUnavailable {
		t.Errorf("ready status with an unreachable controller = %d, want 503", response.Code)
	}
}

func attachBody(node int) string {
	return fmt.Sprintf(`{"tenant_id":%q,"preferred_az":null,"stripe_size":null,
		"shards":[{"node_id":%d,"shard_number":0}]}`, tenantHex, node)
}

func safekeepersBody(generation int) string {
	return fmt.Sprintf(`{"tenant_id":%q,"timeline_id":%q,"generation":%d,
		"safekeepers":[{"id":11,"hostname":"sk-0.neon"},{"id":12,"hostname":null},{"id":13,"hostname":null}]}`,
		tenantHex, timelineHex, generation)
}

// The controller never retries 400, 401 or 403, so a transient failure returned as one leaves a
// tenant mis-notified until something else kicks a reconcile. Only an unparseable body earns one.
func TestNotifyAttachStatusContract(t *testing.T) {
	t.Run("no compute bound is not an error", func(t *testing.T) {
		storcon := newFakeStorcon(t)
		server := newTestServer(t, storcon, newStore(t), newFakeRuntime(), nil)

		response := do(t, server, http.MethodPut, "/notify-attach", attachBody(1))
		if response.Code != http.StatusOK {
			t.Errorf("status = %d, want 200: a tenant with no compute is a normal state", response.Code)
		}
	})

	t.Run("malformed body is fatal", func(t *testing.T) {
		storcon := newFakeStorcon(t)
		server := newTestServer(t, storcon, newStore(t), newFakeRuntime(), nil)

		response := do(t, server, http.MethodPut, "/notify-attach", `{"tenant_id":`)
		if response.Code != http.StatusBadRequest {
			t.Errorf("status = %d, want 400", response.Code)
		}
	})

	t.Run("unreachable controller is retryable", func(t *testing.T) {
		storcon := newFakeStorcon(t)
		storcon.mu.Lock()
		storcon.status = http.StatusInternalServerError
		storcon.mu.Unlock()
		server := newTestServer(t, storcon, newStore(t), newFakeRuntime(), nil)

		response := do(t, server, http.MethodPut, "/notify-attach", attachBody(1))
		if response.Code != http.StatusServiceUnavailable {
			t.Errorf("status = %d, want 503", response.Code)
		}
	})

	// Addresses are resolved live, so pushing before the controller answers with the placement it
	// announced would send a stale one and report success. Nothing would correct that.
	t.Run("placement not yet visible is retryable", func(t *testing.T) {
		storcon := newFakeStorcon(t)
		store := newStore(t)
		seedBranch(t, store)
		runtime := newFakeRuntime()
		seedCompute(t, runtime, true)
		computes := newFakeCompute(t)
		server := newTestServer(t, storcon, store, runtime, computes)

		response := do(t, server, http.MethodPut, "/notify-attach", attachBody(2))
		if response.Code != http.StatusServiceUnavailable {
			t.Errorf("status = %d, want 503", response.Code)
		}
		if len(computes.specs()) != 0 {
			t.Error("a compute was reconfigured from a placement the controller had not yet published")
		}
	})

	t.Run("a compute that refuses the push is retryable", func(t *testing.T) {
		storcon := newFakeStorcon(t)
		store := newStore(t)
		seedBranch(t, store)
		runtime := newFakeRuntime()
		seedCompute(t, runtime, true)
		computes := newFakeCompute(t)
		computes.configureFail = true
		server := newTestServer(t, storcon, store, runtime, computes)

		response := do(t, server, http.MethodPut, "/notify-attach", attachBody(1))
		if response.Code != http.StatusServiceUnavailable {
			t.Errorf("status = %d, want 503", response.Code)
		}
	})
}

func TestNotifyAttachPushesToRunningComputes(t *testing.T) {
	storcon := newFakeStorcon(t)
	store := newStore(t)
	seedBranch(t, store)
	runtime := newFakeRuntime()
	seedCompute(t, runtime, true)
	computes := newFakeCompute(t)
	server := newTestServer(t, storcon, store, runtime, computes)

	response := do(t, server, http.MethodPut, "/notify-attach", attachBody(1))
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body)
	}

	pushed := computes.specs()
	if len(pushed) != 1 {
		t.Fatalf("pushed %d specs, want 1", len(pushed))
	}
	if pushed[0].PageserverConnstring == nil || *pushed[0].PageserverConnstring != "postgresql://no_user@ps-0.neon:6400" {
		t.Errorf("pushed pageserver = %v", pushed[0].PageserverConnstring)
	}
}

// A suspended compute reads the same document from the spec endpoint when it boots, so failing
// the notification on its account would wedge the controller into retrying forever.
func TestNotifyAttachSkipsSuspendedComputes(t *testing.T) {
	storcon := newFakeStorcon(t)
	store := newStore(t)
	seedBranch(t, store)
	runtime := newFakeRuntime()
	seedCompute(t, runtime, false)
	computes := newFakeCompute(t)
	server := newTestServer(t, storcon, store, runtime, computes)

	response := do(t, server, http.MethodPut, "/notify-attach", attachBody(1))
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d", response.Code)
	}
	var counts map[string]int
	if err := json.Unmarshal(response.Body.Bytes(), &counts); err != nil {
		t.Fatal(err)
	}
	if counts["reconfigured"] != 0 || counts["skipped"] != 1 {
		t.Errorf("counts = %v", counts)
	}
	if len(computes.specs()) != 0 {
		t.Error("a suspended compute was pushed to")
	}
}

// The generation must never regress on a compute: walproposer compares it to decide whether an
// incoming membership configuration is newer than the one it is already using.
func TestNotifySafekeepersWaitsForTheGeneration(t *testing.T) {
	storcon := newFakeStorcon(t)
	store := newStore(t)
	seedBranch(t, store)
	runtime := newFakeRuntime()
	seedCompute(t, runtime, true)
	computes := newFakeCompute(t)
	server := newTestServer(t, storcon, store, runtime, computes)

	response := do(t, server, http.MethodPut, "/notify-safekeepers", safekeepersBody(4))
	if response.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503 while the controller still reports generation 3", response.Code)
	}
	if len(computes.specs()) != 0 {
		t.Error("a compute was reconfigured with a membership the controller had not committed")
	}

	response = do(t, server, http.MethodPut, "/notify-safekeepers", safekeepersBody(3))
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body)
	}
	pushed := computes.specs()
	if len(pushed) != 1 {
		t.Fatalf("pushed %d specs, want 1", len(pushed))
	}
	if pushed[0].SafekeepersGeneration == nil || *pushed[0].SafekeepersGeneration != 3 {
		t.Errorf("pushed generation = %v", pushed[0].SafekeepersGeneration)
	}
	if len(pushed[0].SafekeeperConnstrings) != 3 {
		t.Errorf("pushed safekeepers = %v", pushed[0].SafekeeperConnstrings)
	}
}

func TestNotifySafekeepersMalformedBodyIsFatal(t *testing.T) {
	storcon := newFakeStorcon(t)
	server := newTestServer(t, storcon, newStore(t), newFakeRuntime(), nil)

	response := do(t, server, http.MethodPut, "/notify-safekeepers", `not json`)
	if response.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", response.Code)
	}
}

func TestEndpointAccessControl(t *testing.T) {
	storcon := newFakeStorcon(t)
	store := newStore(t)
	branch := seedBranch(t, store)
	server := newTestServer(t, storcon, store, newFakeRuntime(), nil)

	t.Run("returns the role secret", func(t *testing.T) {
		response := do(t, server, http.MethodGet, "/proxy/v1/get_endpoint_access_control?endpointish=main&role=app", "")
		if response.Code != http.StatusOK {
			t.Fatalf("status = %d, body = %s", response.Code, response.Body)
		}
		var body struct {
			RoleSecret string `json:"role_secret"`
		}
		if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
			t.Fatal(err)
		}
		if body.RoleSecret != branch.Roles[0].Verifier {
			t.Errorf("role_secret = %q", body.RoleSecret)
		}
	})

	// The proxy reads the reason out of the error body to tell a missing secret from an outage,
	// and treats a not-found reason as a failed authentication rather than a retryable error.
	for _, tc := range []struct {
		name   string
		target string
		reason string
	}{
		{"unknown endpoint", "/proxy/v1/get_endpoint_access_control?endpointish=absent&role=app", reasonEndpointNotFound},
		{"unknown role", "/proxy/v1/get_endpoint_access_control?endpointish=main&role=nobody", reasonRoleNotFound},
		{"unusable endpoint name", "/proxy/v1/get_endpoint_access_control?endpointish=../etc&role=app", reasonEndpointNotFound},
	} {
		t.Run(tc.name, func(t *testing.T) {
			response := do(t, server, http.MethodGet, tc.target, "")
			if response.Code != http.StatusNotFound {
				t.Fatalf("status = %d, want 404", response.Code)
			}
			var body struct {
				Status struct {
					Details struct {
						ErrorInfo struct {
							Reason string `json:"reason"`
						} `json:"error_info"`
					} `json:"details"`
				} `json:"status"`
			}
			if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
				t.Fatal(err)
			}
			if body.Status.Details.ErrorInfo.Reason != tc.reason {
				t.Errorf("reason = %q, want %q", body.Status.Details.ErrorInfo.Reason, tc.reason)
			}
		})
	}
}

func TestWakeComputeStartsASuspendedBranch(t *testing.T) {
	storcon := newFakeStorcon(t)
	store := newStore(t)
	seedBranch(t, store)
	runtime := newFakeRuntime()
	seedCompute(t, runtime, false)
	server := newTestServer(t, storcon, store, runtime, nil)

	response := do(t, server, http.MethodGet, "/proxy/v1/wake_compute?endpointish=main", "")
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body)
	}

	var body struct {
		Address    string  `json:"address"`
		ServerName *string `json:"server_name"`
		Aux        struct {
			EndpointID string `json:"endpoint_id"`
			ProjectID  string `json:"project_id"`
			BranchID   string `json:"branch_id"`
			ComputeID  string `json:"compute_id"`
		} `json:"aux"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Address != "compute-main.neon:55433" {
		t.Errorf("address = %q", body.Address)
	}
	// A null server name is what tells the proxy to reach the compute without TLS.
	if body.ServerName != nil {
		t.Errorf("server_name = %v, want null", *body.ServerName)
	}
	if body.Aux.EndpointID != "main" || body.Aux.ProjectID != tenantHex || body.Aux.BranchID != timelineHex {
		t.Errorf("aux = %+v", body.Aux)
	}

	instance, err := runtime.Get(context.Background(), "main")
	if err != nil {
		t.Fatal(err)
	}
	if !instance.Running() {
		t.Error("wake_compute returned without the compute running")
	}
}

func TestWakeComputeGivesUpWhenTheComputeStaysDown(t *testing.T) {
	storcon := newFakeStorcon(t)
	store := newStore(t)
	seedBranch(t, store)
	runtime := newFakeRuntime()
	runtime.readyOn = false
	seedCompute(t, runtime, false)
	server := newTestServer(t, storcon, store, runtime, nil)

	response := do(t, server, http.MethodGet, "/proxy/v1/wake_compute?endpointish=main", "")
	if response.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", response.Code)
	}
}

func TestEndpointJWKSIsEmpty(t *testing.T) {
	storcon := newFakeStorcon(t)
	server := newTestServer(t, storcon, newStore(t), newFakeRuntime(), nil)

	response := do(t, server, http.MethodGet, "/proxy/v1/endpoints/main/jwks", "")
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d", response.Code)
	}
	var body struct {
		JWKS []json.RawMessage `json:"jwks"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.JWKS == nil || len(body.JWKS) != 0 {
		t.Errorf("jwks = %v, want an empty list", body.JWKS)
	}
}

// A cold start is slow enough that a burst of connections arrives while it is still in progress.
// Each one racing an update of the same object would be both wasteful and conflict-prone.
func TestConcurrentWakesShareOneAttempt(t *testing.T) {
	storcon := newFakeStorcon(t)
	store := newStore(t)
	seedBranch(t, store)
	runtime := newFakeRuntime()
	runtime.readyOn = false
	seedCompute(t, runtime, false)
	server := newTestServer(t, storcon, store, runtime, nil)

	var waiting sync.WaitGroup
	for i := 0; i < 5; i++ {
		waiting.Add(1)
		go func() {
			defer waiting.Done()
			do(t, server, http.MethodGet, "/proxy/v1/wake_compute?endpointish=main", "")
		}()
	}
	waiting.Wait()

	runtime.mu.Lock()
	ensures := runtime.ensures
	runtime.mu.Unlock()
	if ensures != 1 {
		t.Errorf("five concurrent wakes produced %d scale-up attempts, want 1", ensures)
	}
}

func decodeBranch(t *testing.T, body []byte) branchView {
	t.Helper()
	var view branchView
	if err := json.Unmarshal(body, &view); err != nil {
		t.Fatalf("%v: %s", err, body)
	}
	return view
}

func TestCreateBranchStoresAVerifierNotAPassword(t *testing.T) {
	storcon := newFakeStorcon(t)
	store := newStore(t)
	server := newTestServer(t, storcon, store, newFakeRuntime(), nil)

	response := do(t, server, http.MethodPost, "/api/branches", `{
		"name":"main",
		"roles":[{"name":"app","password":"hunter2"}],
		"databases":[{"name":"appdb","owner":"app"}]}`)
	if response.Code != http.StatusCreated {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body)
	}

	branch, err := store.Get(context.Background(), "main")
	if err != nil {
		t.Fatal(err)
	}
	if len(branch.Roles) != 1 {
		t.Fatalf("roles = %+v", branch.Roles)
	}
	if !scram.IsVerifier(branch.Roles[0].Verifier) {
		t.Errorf("stored secret is not a verifier: %q", branch.Roles[0].Verifier)
	}
	if branch.TenantID.IsZero() || branch.TimelineID.IsZero() {
		t.Errorf("branch was recorded without ids: %+v", branch)
	}

	// The response must never echo the secret back, in either direction.
	view := decodeBranch(t, response.Body.Bytes())
	if len(view.Roles) != 1 || view.Roles[0] != "app" {
		t.Errorf("roles in view = %v", view.Roles)
	}
	if bytesContain(response.Body.Bytes(), "hunter2") || bytesContain(response.Body.Bytes(), "SCRAM-SHA-256") {
		t.Error("the branch view leaked a credential")
	}
}

func TestCreateBranchValidatesInput(t *testing.T) {
	storcon := newFakeStorcon(t)
	server := newTestServer(t, storcon, newStore(t), newFakeRuntime(), nil)

	for _, tc := range []struct {
		name string
		body string
		want int
	}{
		{"malformed body", `{`, http.StatusBadRequest},
		{"unknown parent", `{"name":"child","parent":"absent","roles":[{"name":"a","password":"b"}]}`, http.StatusBadRequest},
		{"unusable mode", `{"name":"main","mode":"Sideways","roles":[{"name":"a","password":"b"}]}`, http.StatusBadRequest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			response := do(t, server, http.MethodPost, "/api/branches", tc.body)
			if response.Code != tc.want {
				t.Errorf("status = %d, want %d: %s", response.Code, tc.want, response.Body)
			}
		})
	}
}

func TestCreateBranchRejectsADuplicate(t *testing.T) {
	storcon := newFakeStorcon(t)
	store := newStore(t)
	seedBranch(t, store)
	server := newTestServer(t, storcon, store, newFakeRuntime(), nil)

	response := do(t, server, http.MethodPost, "/api/branches", `{"name":"main","roles":[{"name":"a","password":"b"}]}`)
	if response.Code != http.StatusConflict {
		t.Errorf("status = %d, want 409", response.Code)
	}
}

// What the HTTP layer owns here is the request the controller receives: the model's own
// inheritance rules are exercised against the model.
func TestForkAsksTheControllerForABranch(t *testing.T) {
	storcon := newFakeStorcon(t)
	store := newStore(t)
	parent := seedBranch(t, store)
	server := newTestServer(t, storcon, store, newFakeRuntime(), nil)

	response := do(t, server, http.MethodPost, "/api/branches",
		`{"name":"feature","parent":"main","parent_lsn":"16/B374D848"}`)
	if response.Code != http.StatusCreated {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body)
	}

	child, err := store.Get(context.Background(), "feature")
	if err != nil {
		t.Fatal(err)
	}

	storcon.mu.Lock()
	created := storcon.created
	storcon.mu.Unlock()
	if len(created) != 1 {
		t.Fatalf("the controller was asked to create %d timelines, want 1", len(created))
	}
	if created[0].AncestorTimelineID == nil || *created[0].AncestorTimelineID != parent.TimelineID {
		t.Errorf("ancestor = %v, want the parent timeline", created[0].AncestorTimelineID)
	}
	if created[0].AncestorStartLSN == nil || created[0].AncestorStartLSN.String() != "16/B374D848" {
		t.Errorf("ancestor lsn = %v", created[0].AncestorStartLSN)
	}
	if created[0].NewTimelineID != child.TimelineID {
		t.Error("the recorded timeline is not the one that was created")
	}
}

func TestPatchBranchReconfiguresARunningCompute(t *testing.T) {
	storcon := newFakeStorcon(t)
	store := newStore(t)
	seedBranch(t, store)
	runtime := newFakeRuntime()
	seedCompute(t, runtime, true)
	computes := newFakeCompute(t)
	server := newTestServer(t, storcon, store, runtime, computes)

	response := do(t, server, http.MethodPatch, "/api/branches/main",
		`{"settings":[{"name":"work_mem","value":"64MB","vartype":"string"}]}`)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body)
	}

	pushed := computes.specs()
	if len(pushed) != 1 {
		t.Fatalf("pushed %d specs, want 1", len(pushed))
	}
	var found bool
	for _, setting := range pushed[0].Cluster.Settings {
		if setting.Name == "work_mem" {
			found = true
		}
	}
	if !found {
		t.Errorf("the patched setting did not reach the compute: %+v", pushed[0].Cluster.Settings)
	}
}

func TestDeleteBranchRemovesComputeAndEntry(t *testing.T) {
	storcon := newFakeStorcon(t)
	store := newStore(t)
	seedBranch(t, store)
	runtime := newFakeRuntime()
	seedCompute(t, runtime, true)
	server := newTestServer(t, storcon, store, runtime, nil)

	response := do(t, server, http.MethodDelete, "/api/branches/main", "")
	if response.Code != http.StatusNoContent {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body)
	}
	if _, err := store.Get(context.Background(), "main"); !errors.Is(err, registry.ErrNotFound) {
		t.Errorf("branch survived deletion: %v", err)
	}
	if _, err := runtime.Get(context.Background(), "main"); err == nil {
		t.Error("compute survived deletion")
	}
}

func TestBranchViewReportsLiveComputeState(t *testing.T) {
	storcon := newFakeStorcon(t)
	store := newStore(t)
	seedBranch(t, store)
	runtime := newFakeRuntime()
	computes := newFakeCompute(t)
	active := time.Now().Add(-2 * time.Minute)
	computes.lastActive = &active
	server := newTestServer(t, storcon, store, runtime, computes)

	response := do(t, server, http.MethodGet, "/api/branches/main", "")
	view := decodeBranch(t, response.Body.Bytes())
	if view.Compute.Status != "absent" {
		t.Errorf("status with no compute = %q", view.Compute.Status)
	}

	seedCompute(t, runtime, true)
	response = do(t, server, http.MethodGet, "/api/branches/main", "")
	view = decodeBranch(t, response.Body.Bytes())
	if view.Compute.Status != "running" || view.Compute.LastActive == nil {
		t.Errorf("view = %+v", view.Compute)
	}
}

func TestStartAndStopBranch(t *testing.T) {
	storcon := newFakeStorcon(t)
	store := newStore(t)
	seedBranch(t, store)
	runtime := newFakeRuntime()
	seedCompute(t, runtime, false)
	computes := newFakeCompute(t)
	server := newTestServer(t, storcon, store, runtime, computes)

	if response := do(t, server, http.MethodPost, "/api/branches/main/start", ""); response.Code != http.StatusOK {
		t.Fatalf("start status = %d, body = %s", response.Code, response.Body)
	}
	instance, err := runtime.Get(context.Background(), "main")
	if err != nil || !instance.Running() {
		t.Fatalf("compute after start = %+v, %v", instance, err)
	}

	if response := do(t, server, http.MethodPost, "/api/branches/main/stop", ""); response.Code != http.StatusOK {
		t.Fatalf("stop status = %d", response.Code)
	}
	instance, err = runtime.Get(context.Background(), "main")
	if err != nil {
		t.Fatal(err)
	}
	if instance.Replicas != 0 {
		t.Errorf("replicas after stop = %d", instance.Replicas)
	}
	// A crash would be safe, but a clean shutdown restarts faster and reports its flush LSN.
	if !computes.terminated {
		t.Error("the compute was scaled down without being asked to terminate")
	}
}

func TestUnknownBranchIsNotFound(t *testing.T) {
	storcon := newFakeStorcon(t)
	server := newTestServer(t, storcon, newStore(t), newFakeRuntime(), nil)

	for _, target := range []string{"/api/branches/absent", "/api/branches/absent/start", "/api/branches/absent/stop"} {
		method := http.MethodGet
		if target != "/api/branches/absent" {
			method = http.MethodPost
		}
		if response := do(t, server, method, target, ""); response.Code != http.StatusNotFound {
			t.Errorf("%s %s = %d, want 404", method, target, response.Code)
		}
	}
	if response := do(t, server, http.MethodGet, "/api/branches/NotALegalName", ""); response.Code != http.StatusBadRequest {
		t.Errorf("status for an unusable name = %d, want 400", response.Code)
	}
}

func bytesContain(haystack []byte, needle string) bool {
	return len(needle) > 0 && len(haystack) >= len(needle) && indexOf(haystack, needle) >= 0
}

func indexOf(haystack []byte, needle string) int {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if string(haystack[i:i+len(needle)]) == needle {
			return i
		}
	}
	return -1
}

// A tenant may hold branches of different Postgres versions — the storage layer records one per
// timeline — so what a branch may ask for is whichever compute images the deployment supplies.
func TestCreateBranchChoosesAmongAvailableVersions(t *testing.T) {
	storcon := newFakeStorcon(t)
	store := newStore(t)
	server := newTestServer(t, storcon, store, newFakeRuntime(), nil)

	body := func(name string, version int) string {
		return fmt.Sprintf(`{"name":%q,"pg_version":%d,"roles":[{"name":"app","password":"x"}]}`, name, version)
	}

	if response := do(t, server, http.MethodPost, "/api/branches", body("older", 16)); response.Code != http.StatusCreated {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body)
	}
	older, err := store.Get(context.Background(), "older")
	if err != nil {
		t.Fatal(err)
	}
	if older.PgVersion != 16 {
		t.Errorf("pg version = %d, want the requested one", older.PgVersion)
	}

	// Unavailable versions are refused where the branch is created, not later as a pod that
	// cannot start.
	response := do(t, server, http.MethodPost, "/api/branches", body("ancient", 13))
	if response.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", response.Code)
	}

	// Saying nothing takes the configured default.
	if response := do(t, server, http.MethodPost, "/api/branches",
		`{"name":"plain","roles":[{"name":"app","password":"x"}]}`); response.Code != http.StatusCreated {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body)
	}
	plain, err := store.Get(context.Background(), "plain")
	if err != nil {
		t.Fatal(err)
	}
	if plain.PgVersion != 17 {
		t.Errorf("pg version = %d, want the default", plain.PgVersion)
	}
}

// A fork cannot differ from its ancestor, so a version on the request is meaningless there.
func TestForkKeepsTheAncestorsVersion(t *testing.T) {
	storcon := newFakeStorcon(t)
	store := newStore(t)
	server := newTestServer(t, storcon, store, newFakeRuntime(), nil)

	if response := do(t, server, http.MethodPost, "/api/branches",
		`{"name":"older","pg_version":16,"roles":[{"name":"app","password":"x"}]}`); response.Code != http.StatusCreated {
		t.Fatal(response.Body)
	}
	if response := do(t, server, http.MethodPost, "/api/branches",
		`{"name":"child","parent":"older","pg_version":17}`); response.Code != http.StatusCreated {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body)
	}

	child, err := store.Get(context.Background(), "child")
	if err != nil {
		t.Fatal(err)
	}
	if child.PgVersion != 16 {
		t.Errorf("pg version = %d, want the ancestor's", child.PgVersion)
	}
}

// Creating a branch is two controller calls and the second is the one that fails in practice. The
// tenant from the first is then unreachable by any name, so it has to be taken back.
func TestCreateBranchDeletesTheTenantItCannotUse(t *testing.T) {
	storcon := newFakeStorcon(t)
	storcon.timelineRefused = true
	store := newStore(t)
	server := newTestServer(t, storcon, store, newFakeRuntime(), nil)

	response := do(t, server, http.MethodPost, "/api/branches", `{
		"name":"main",
		"roles":[{"name":"app","password":"hunter2"}]}`)
	if response.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body)
	}

	storcon.mu.Lock()
	deleted := storcon.deletedTenants
	storcon.mu.Unlock()
	if len(deleted) != 1 {
		t.Fatalf("deleted tenants = %v, want exactly the one just created", deleted)
	}
	if _, err := store.Get(context.Background(), "main"); !errors.Is(err, registry.ErrNotFound) {
		t.Errorf("a failed branch was recorded anyway: %v", err)
	}
}

// A tenant the caller named is not ours to delete: the failure says nothing about whatever else
// already lives on it.
func TestCreateBranchLeavesAnAdoptedTenantAlone(t *testing.T) {
	storcon := newFakeStorcon(t)
	storcon.timelineRefused = true
	server := newTestServer(t, storcon, newStore(t), newFakeRuntime(), nil)

	response := do(t, server, http.MethodPost, "/api/branches", `{
		"name":"main",
		"tenant_id":"aa7d0e4f02c00ce5e0a4c405b6850585",
		"roles":[{"name":"app","password":"hunter2"}]}`)
	if response.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body)
	}

	storcon.mu.Lock()
	deleted := storcon.deletedTenants
	storcon.mu.Unlock()
	if len(deleted) != 0 {
		t.Errorf("deleted a tenant the caller supplied: %v", deleted)
	}
}
