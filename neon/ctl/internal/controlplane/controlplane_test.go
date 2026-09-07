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
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hzhou0/homelab/neon/ctl/internal/kube"
	"github.com/hzhou0/homelab/neon/ctl/internal/neon"
	"github.com/hzhou0/homelab/neon/ctl/internal/registry"
	"github.com/hzhou0/homelab/neon/ctl/internal/scram"
)

// Every test runs inside one project, because after multitenancy there is no such thing as a
// branch outside one.
const (
	testProjectName = "acme"
	testProjectID   = "pr-test-project-aaaaaaaa"
	testEndpointID  = "ep-test-branch-aaaaaaaa"
)

// Fixed rather than generated, so a route in a test can name the project literally.
func seedProject(t *testing.T, store *registry.Store) *registry.Project {
	t.Helper()
	now := time.Now().UTC()
	project := &registry.Project{
		ID:        testProjectID,
		Name:      testProjectName,
		TenantID:  mustTenant(t),
		Owner:     "tester",
		CreatedAt: now,
		UpdatedAt: now,
	}
	if err := store.CreateProject(context.Background(), project); err != nil {
		t.Fatal(err)
	}
	return project
}

func newStore(t *testing.T) *registry.Store {
	t.Helper()
	store, err := registry.Open(filepath.Join(t.TempDir(), "registry.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	seedProject(t, store)
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

	logicalSize  uint64
	sizeAccurate bool

	timelineRefused  bool
	deletedTenants   []string
	deletedTimelines []string
}

func newFakeStorcon(t *testing.T) *fakeStorcon {
	t.Helper()
	fake := &fakeStorcon{
		shardNode:    1,
		generation:   3,
		logicalSize:  42 * 1024 * 1024,
		sizeAccurate: true,
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

	// The controller forwards any tenant GET it does not implement to shard zero's pageserver.
	mux.HandleFunc("GET /v1/tenant/{tenant}/timeline/{timeline}", func(w http.ResponseWriter, r *http.Request) {
		fake.mu.Lock()
		size, accurate := fake.logicalSize, fake.sizeAccurate
		fake.mu.Unlock()
		fake.respond(w, neon.TimelineInfo{CurrentLogicalSize: size, CurrentLogicalSizeIsAccurate: accurate})
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
		fake.mu.Lock()
		fake.deletedTimelines = append(fake.deletedTimelines, r.PathValue("timeline"))
		fake.mu.Unlock()
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
	catalogFail   bool
	status        neon.ComputeStatus
	lastActive    *time.Time
	terminated    bool
	catalog       neon.CatalogObjects
	// Kept as it arrived: decoding into the same types that encoded it would agree with any
	// spelling, including one compute_ctl does not read.
	bodies [][]byte
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
		fake.bodies = append(fake.bodies, body)
		fake.apply(config.Spec)
		writeJSON(w, http.StatusOK, map[string]string{})
	})
	mux.HandleFunc("GET /dbs_and_roles", func(w http.ResponseWriter, r *http.Request) {
		fake.mu.Lock()
		defer fake.mu.Unlock()
		if fake.catalogFail {
			http.Error(w, "no", http.StatusInternalServerError)
			return
		}
		writeJSON(w, http.StatusOK, fake.catalog)
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

// The catalog answers with what it was sent, the way compute_ctl does: the cluster states what
// should exist and a delta is the only thing that takes anything away.
func (f *fakeCompute) apply(spec *neon.ComputeSpec) {
	for _, role := range spec.Cluster.Roles {
		f.catalog.Roles = upsert(f.catalog.Roles, role, func(r neon.Role) string { return r.Name })
	}
	for _, database := range spec.Cluster.Databases {
		f.catalog.Databases = upsert(f.catalog.Databases, database, func(d neon.Database) string { return d.Name })
	}
	for _, delta := range spec.DeltaOperations {
		switch delta.Action {
		case neon.DeleteRole:
			f.catalog.Roles = slices.DeleteFunc(f.catalog.Roles, func(r neon.Role) bool { return r.Name == delta.Name })
		case neon.DeleteDatabase:
			f.catalog.Databases = slices.DeleteFunc(f.catalog.Databases, func(d neon.Database) bool { return d.Name == delta.Name })
		}
	}
}

func upsert[T any](existing []T, item T, name func(T) string) []T {
	for i := range existing {
		if name(existing[i]) == name(item) {
			existing[i] = item
			return existing
		}
	}
	return append(existing, item)
}

// A compute that has been running holds what the branch was created with, which is what a test
// that seeds a branch and a live compute is describing.
func primeCatalog(computes *fakeCompute, branches []registry.Branch) {
	for _, branch := range branches {
		for _, role := range branch.Roles {
			verifier := role.Verifier
			computes.catalog.Roles = upsert(computes.catalog.Roles,
				neon.Role{Name: role.Name, EncryptedPassword: &verifier},
				func(r neon.Role) string { return r.Name })
		}
		for _, database := range branch.Databases {
			computes.catalog.Databases = upsert(computes.catalog.Databases,
				neon.Database{Name: database.Name, Owner: database.Owner},
				func(d neon.Database) string { return d.Name })
		}
	}
}

func (f *fakeCompute) lastBody() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.bodies) == 0 {
		return ""
	}
	return string(f.bodies[len(f.bodies)-1])
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
		Identity:       IdentityOptions{UserHeader: "Remote-User", GroupsHeader: "Remote-Groups", Admin: "tester"},
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
		ProjectID:  testProjectID,
		EndpointID: testEndpointID,
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
			ID:         testEndpointID,
			TenantID:   mustTenant(t),
			TimelineID: mustTimeline(t),
			Mode:       neon.ComputeMode{Kind: neon.ModePrimary},
		},
		ControlURL: "http://compute-" + testEndpointID + ".neon:3080",
		PgAddress:  "compute-" + testEndpointID + ".neon:55433",
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
	request.Header.Set("Remote-User", "tester")
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

// A compute reads its spec once, at startup, and nothing revisits it. Serving one without the
// catalog would strand a branch running that no role can log in to, so the spec is refused and the
// pod restart becomes the retry.
func TestASpecIsRefusedWithoutACatalog(t *testing.T) {
	for _, tc := range []struct {
		name    string
		status  int
		prepare func(t *testing.T, store *registry.Store)
	}{
		{"no entry", http.StatusNotFound, func(*testing.T, *registry.Store) {}},
		{"registry failing", http.StatusServiceUnavailable, func(t *testing.T, store *registry.Store) {
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

			if _, err := server.renderSpec(context.Background(), &instance); err == nil {
				t.Fatal("a spec with no catalog must not be served")
			} else if got := statusOf(err); got != tc.status {
				t.Errorf("status = %d, want %d", got, tc.status)
			}

			response := do(t, server, http.MethodGet, "/compute/api/v2/computes/"+testEndpointID+"/spec", "")
			if response.Code != tc.status {
				t.Errorf("the spec endpoint answered %d, want %d", response.Code, tc.status)
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
		response := do(t, server, http.MethodGet, "/compute/api/v2/computes/"+testEndpointID+"/spec", "")
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

		response := do(t, server, http.MethodGet, "/compute/api/v2/computes/"+testEndpointID+"/spec", "")
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
		response := do(t, server, http.MethodGet, "/proxy/v1/get_endpoint_access_control?endpointish="+testEndpointID+"&role=app", "")
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
		{"unknown role", "/proxy/v1/get_endpoint_access_control?endpointish=" + testEndpointID + "&role=nobody", reasonRoleNotFound},
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

	response := do(t, server, http.MethodGet, "/proxy/v1/wake_compute?endpointish="+testEndpointID, "")
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
	if body.Address != "compute-"+testEndpointID+".neon:55433" {
		t.Errorf("address = %q", body.Address)
	}
	// A null server name is what tells the proxy to reach the compute without TLS.
	if body.ServerName != nil {
		t.Errorf("server_name = %v, want null", *body.ServerName)
	}
	if body.Aux.EndpointID != testEndpointID || body.Aux.ProjectID != tenantHex || body.Aux.BranchID != timelineHex {
		t.Errorf("aux = %+v", body.Aux)
	}

	instance, err := runtime.Get(context.Background(), testEndpointID)
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

	response := do(t, server, http.MethodGet, "/proxy/v1/wake_compute?endpointish="+testEndpointID, "")
	if response.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", response.Code)
	}
}

func TestEndpointJWKSIsEmpty(t *testing.T) {
	storcon := newFakeStorcon(t)
	server := newTestServer(t, storcon, newStore(t), newFakeRuntime(), nil)

	response := do(t, server, http.MethodGet, "/proxy/v1/endpoints/"+testEndpointID+"/jwks", "")
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
			do(t, server, http.MethodGet, "/proxy/v1/wake_compute?endpointish="+testEndpointID, "")
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

	response := do(t, server, http.MethodPost, "/api/projects/"+testProjectID+"/branches", `{
		"name":"main",
		"roles":[{"name":"app","password":"hunter2"}],
		"databases":[{"name":"appdb","owner":"app"}]}`)
	if response.Code != http.StatusCreated {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body)
	}

	branch, err := store.Branch(context.Background(), testProjectID, "main")
	if err != nil {
		t.Fatal(err)
	}
	if len(branch.Roles) != 1 {
		t.Fatalf("roles = %+v", branch.Roles)
	}
	if !scram.IsVerifier(branch.Roles[0].Verifier) {
		t.Errorf("stored secret is not a verifier: %q", branch.Roles[0].Verifier)
	}
	// No key was configured, so the password itself was not kept in any form.
	if branch.Roles[0].Secret != "" {
		t.Error("a password was kept by a deployment that has nowhere to keep one")
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
			response := do(t, server, http.MethodPost, "/api/projects/"+testProjectID+"/branches", tc.body)
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

	response := do(t, server, http.MethodPost, "/api/projects/"+testProjectID+"/branches", `{"name":"main","roles":[{"name":"a","password":"b"}]}`)
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

	response := do(t, server, http.MethodPost, "/api/projects/"+testProjectID+"/branches",
		`{"name":"feature","parent":"main","parent_lsn":"16/B374D848"}`)
	if response.Code != http.StatusCreated {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body)
	}

	child, err := store.Branch(context.Background(), testProjectID, "feature")
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

	response := do(t, server, http.MethodPatch, "/api/projects/"+testProjectID+"/branches/main",
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

	response := do(t, server, http.MethodDelete, "/api/projects/"+testProjectID+"/branches/main", "")
	if response.Code != http.StatusNoContent {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body)
	}
	if _, err := store.Branch(context.Background(), testProjectID, "main"); !errors.Is(err, registry.ErrNotFound) {
		t.Errorf("branch survived deletion: %v", err)
	}
	if _, err := runtime.Get(context.Background(), testEndpointID); err == nil {
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

	response := do(t, server, http.MethodGet, "/api/projects/"+testProjectID+"/branches/main", "")
	view := decodeBranch(t, response.Body.Bytes())
	if view.Compute.Status != "absent" {
		t.Errorf("status with no compute = %q", view.Compute.Status)
	}

	seedCompute(t, runtime, true)
	response = do(t, server, http.MethodGet, "/api/projects/"+testProjectID+"/branches/main", "")
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

	if response := do(t, server, http.MethodPost, "/api/projects/"+testProjectID+"/branches/main/start", ""); response.Code != http.StatusOK {
		t.Fatalf("start status = %d, body = %s", response.Code, response.Body)
	}
	instance, err := runtime.Get(context.Background(), testEndpointID)
	if err != nil || !instance.Running() {
		t.Fatalf("compute after start = %+v, %v", instance, err)
	}

	if response := do(t, server, http.MethodPost, "/api/projects/"+testProjectID+"/branches/main/stop", ""); response.Code != http.StatusOK {
		t.Fatalf("stop status = %d", response.Code)
	}
	instance, err = runtime.Get(context.Background(), testEndpointID)
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

	for _, target := range []string{"/api/projects/" + testProjectID + "/branches/absent", "/api/projects/" + testProjectID + "/branches/absent/start", "/api/projects/" + testProjectID + "/branches/absent/stop"} {
		method := http.MethodGet
		if target != "/api/projects/"+testProjectID+"/branches/absent" {
			method = http.MethodPost
		}
		if response := do(t, server, method, target, ""); response.Code != http.StatusNotFound {
			t.Errorf("%s %s = %d, want 404", method, target, response.Code)
		}
	}
	if response := do(t, server, http.MethodGet, "/api/projects/"+testProjectID+"/branches/NotALegalName", ""); response.Code != http.StatusBadRequest {
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

	if response := do(t, server, http.MethodPost, "/api/projects/"+testProjectID+"/branches", body("older", 16)); response.Code != http.StatusCreated {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body)
	}
	older, err := store.Branch(context.Background(), testProjectID, "older")
	if err != nil {
		t.Fatal(err)
	}
	if older.PgVersion != 16 {
		t.Errorf("pg version = %d, want the requested one", older.PgVersion)
	}

	// Unavailable versions are refused where the branch is created, not later as a pod that
	// cannot start.
	response := do(t, server, http.MethodPost, "/api/projects/"+testProjectID+"/branches", body("ancient", 13))
	if response.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", response.Code)
	}

	// Saying nothing takes the configured default.
	if response := do(t, server, http.MethodPost, "/api/projects/"+testProjectID+"/branches",
		`{"name":"plain","roles":[{"name":"app","password":"x"}]}`); response.Code != http.StatusCreated {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body)
	}
	plain, err := store.Branch(context.Background(), testProjectID, "plain")
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

	if response := do(t, server, http.MethodPost, "/api/projects/"+testProjectID+"/branches",
		`{"name":"older","pg_version":16,"roles":[{"name":"app","password":"x"}]}`); response.Code != http.StatusCreated {
		t.Fatal(response.Body)
	}
	if response := do(t, server, http.MethodPost, "/api/projects/"+testProjectID+"/branches",
		`{"name":"child","parent":"older","pg_version":17}`); response.Code != http.StatusCreated {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body)
	}

	child, err := store.Branch(context.Background(), testProjectID, "child")
	if err != nil {
		t.Fatal(err)
	}
	if child.PgVersion != 16 {
		t.Errorf("pg version = %d, want the ancestor's", child.PgVersion)
	}
}

// A branch that cannot be recorded must not leave its timeline behind: nothing would ever name it
// again, and only the controller would still know it exists.
func TestCreateBranchDeletesTheTimelineItCannotRecord(t *testing.T) {
	storcon := newFakeStorcon(t)
	store := newStore(t)
	server := newTestServer(t, storcon, store, newFakeRuntime(), nil)

	// Claim the name first, so the create reaches the registry and is refused by it.
	seedBranch(t, store)

	response := do(t, server, http.MethodPost, "/api/projects/"+testProjectID+"/branches", `{
		"name":"main",
		"roles":[{"name":"app","password":"hunter2"}]}`)
	if response.Code != http.StatusConflict {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body)
	}
	storcon.mu.Lock()
	deleted := storcon.deletedTimelines
	storcon.mu.Unlock()
	if len(deleted) != 1 {
		t.Errorf("deleted timelines = %v, want the one just created", deleted)
	}
}

// The tenant belongs to the project, so a branch failing must never take it: every other branch in
// the project is on it.
func TestCreateBranchNeverDeletesTheProjectsTenant(t *testing.T) {
	storcon := newFakeStorcon(t)
	storcon.timelineRefused = true
	store := newStore(t)
	server := newTestServer(t, storcon, store, newFakeRuntime(), nil)

	response := do(t, server, http.MethodPost, "/api/projects/"+testProjectID+"/branches", `{
		"name":"main",
		"roles":[{"name":"app","password":"hunter2"}]}`)
	if response.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body)
	}
	storcon.mu.Lock()
	deleted := storcon.deletedTenants
	storcon.mu.Unlock()
	if len(deleted) != 0 {
		t.Errorf("a failed branch deleted its project's tenant: %v", deleted)
	}
	if _, err := store.Branch(context.Background(), testProjectID, "main"); !errors.Is(err, registry.ErrNotFound) {
		t.Errorf("a failed branch was recorded anyway: %v", err)
	}
}

// identityServer runs the way a deployment behind an authenticating proxy does: headers are
// believed, and there is no single owner to fall back on.
// Nothing about a branch is shown that a compute has not just confirmed, so a server a page will
// be rendered from needs a running compute holding whatever the store was seeded with.
func liveServer(t *testing.T, store *registry.Store) (*Server, *fakeCompute) {
	t.Helper()
	computes := newFakeCompute(t)
	runtime := newFakeRuntime()
	seedCompute(t, runtime, true)
	if branches, err := store.Branches(context.Background(), testProjectID); err == nil {
		primeCatalog(computes, branches)
	}
	server := newTestServer(t, newFakeStorcon(t), store, runtime, computes)
	server.identity = IdentityOptions{UserHeader: "Remote-User", GroupsHeader: "Remote-Groups", Admin: "root"}
	return server, computes
}

func identityServer(t *testing.T, store *registry.Store) *Server {
	t.Helper()
	server, _ := liveServer(t, store)
	server.identity.DisplayHeader = "Remote-Name"
	return server
}

func doAs(t *testing.T, server *Server, method, target, user, groups, body string) *httptest.ResponseRecorder {
	t.Helper()
	var reader io.Reader
	if body != "" {
		reader = strings.NewReader(body)
	}
	request := httptest.NewRequest(method, target, reader)
	if user != "" {
		request.Header.Set("Remote-User", user)
	}
	if groups != "" {
		request.Header.Set("Remote-Groups", groups)
	}
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, request)
	return recorder
}

// Without a proxy in front there is no identity, and a request carrying none is refused rather
// than treated as anonymous.
func TestUnidentifiedRequestIsRefused(t *testing.T) {
	server := identityServer(t, newStore(t))
	for _, target := range []string{"/api/projects", "/api/projects/" + testProjectID + "", "/api/projects/" + testProjectID + "/branches"} {
		if got := doAs(t, server, http.MethodGet, target, "", "", "").Code; got != http.StatusUnauthorized {
			t.Errorf("GET %s = %d, want 401", target, got)
		}
	}
}

// The whole point of the model: one tenant's branches are not another's to see, and the answer
// does not even admit the project exists.
func TestProjectsAreInvisibleToOtherPeople(t *testing.T) {
	store := newStore(t)
	seedBranch(t, store)
	server := identityServer(t, store)

	stranger := doAs(t, server, http.MethodGet, "/api/projects", "mallory", "outsiders", "")
	if stranger.Code != http.StatusOK {
		t.Fatalf("status = %d", stranger.Code)
	}
	if !strings.Contains(stranger.Body.String(), `"projects":[]`) {
		t.Errorf("a stranger was shown projects: %s", stranger.Body)
	}

	for _, target := range []string{"/api/projects/" + testProjectID + "", "/api/projects/" + testProjectID + "/branches", "/api/projects/" + testProjectID + "/branches/main"} {
		if got := doAs(t, server, http.MethodGet, target, "mallory", "outsiders", "").Code; got != http.StatusNotFound {
			t.Errorf("GET %s as a stranger = %d, want 404", target, got)
		}
	}
	// Refusal must not depend on the branch being absent, so the owner sees the same routes work.
	for _, target := range []string{"/api/projects/" + testProjectID + "", "/api/projects/" + testProjectID + "/branches", "/api/projects/" + testProjectID + "/branches/main"} {
		if got := doAs(t, server, http.MethodGet, target, "tester", "", "").Code; got != http.StatusOK {
			t.Errorf("GET %s as the owner = %d, want 200", target, got)
		}
	}
}

func TestGroupGrantAndAdministrator(t *testing.T) {
	store := newStore(t)
	project, err := registry.NewProject("shared", "someone", []string{"platform"}, mustTenant(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CreateProject(context.Background(), project); err != nil {
		t.Fatal(err)
	}
	server := identityServer(t, store)
	shared := "/api/projects/" + project.ID

	if got := doAs(t, server, http.MethodGet, shared, "carol", "platform", "").Code; got != http.StatusOK {
		t.Errorf("a member of a granted group = %d, want 200", got)
	}
	if got := doAs(t, server, http.MethodGet, shared, "carol", "other", "").Code; got != http.StatusNotFound {
		t.Errorf("a non-member = %d, want 404", got)
	}
	if got := doAs(t, server, http.MethodGet, shared, "root", "", "").Code; got != http.StatusOK {
		t.Errorf("the administrator = %d, want 200", got)
	}
}

// Creating a project for somebody else is an admin act; otherwise anyone could hand one to a
// person who never asked for it.
func TestOnlyAnAdminCreatesAProjectForSomeoneElse(t *testing.T) {
	server := identityServer(t, newStore(t))

	body := `{"name":"handed","owner":"victim"}`
	if got := doAs(t, server, http.MethodPost, "/api/projects", "mallory", "", body).Code; got != http.StatusForbidden {
		t.Errorf("a non-admin naming another owner = %d, want 403", got)
	}
	if got := doAs(t, server, http.MethodPost, "/api/projects", "root", "", body).Code; got != http.StatusCreated {
		t.Errorf("an admin naming another owner = %d, want 201", got)
	}
}

// Ownership keys off the stable identifier, never the display name. An account destroyed and
// remade takes a new identifier, and must not inherit what the old one owned.
func TestOwnershipFollowsTheStableIdentifierNotTheName(t *testing.T) {
	store := newStore(t)
	project, err := registry.NewProject("owned", "uuid-original", nil, mustTenant(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CreateProject(context.Background(), project); err != nil {
		t.Fatal(err)
	}
	server := identityServer(t, store)

	request := func(user, display string) int {
		req := httptest.NewRequest(http.MethodGet, "/api/projects/"+project.ID, nil)
		req.Header.Set("Remote-User", user)
		req.Header.Set("Remote-Name", display)
		recorder := httptest.NewRecorder()
		server.Handler().ServeHTTP(recorder, req)
		return recorder.Code
	}

	if got := request("uuid-original", "Henry"); got != http.StatusOK {
		t.Errorf("the owner = %d, want 200", got)
	}
	// Same human name, new directory identifier: a recreated account, and a different principal.
	if got := request("uuid-recreated", "Henry"); got != http.StatusNotFound {
		t.Errorf("a recreated account with the same display name = %d, want 404", got)
	}
	// The display name is not consulted at all, so its absence changes nothing.
	if got := request("uuid-original", ""); got != http.StatusOK {
		t.Errorf("the owner without a display name = %d, want 200", got)
	}
}

// Access can only be given away by someone who has it, so a grant naming a group the caller is
// not in is refused the same way naming another owner is.
func TestGrantsAreLimitedToGroupsTheCallerHolds(t *testing.T) {
	server := identityServer(t, newStore(t))

	foreign := doAs(t, server, http.MethodPost, "/api/projects", "mallory", "",
		`{"name":"grabbed","groups":["platform"]}`)
	if foreign.Code != http.StatusForbidden {
		t.Errorf("granting a group the caller is not in = %d, want 403", foreign.Code)
	}
	if seen := doAs(t, server, http.MethodGet, "/api/projects", "carol", "platform", ""); strings.Contains(seen.Body.String(), "grabbed") {
		t.Errorf("the refused project reached a stranger's listing: %s", seen.Body)
	}

	held := doAs(t, server, http.MethodPost, "/api/projects", "mallory", "platform",
		`{"name":"shared","groups":["platform"]}`)
	if held.Code != http.StatusCreated {
		t.Errorf("granting a group the caller holds = %d, want 201: %s", held.Code, held.Body)
	}

	// The administrator is the one account that can grant on behalf of a team it is not in.
	asAdmin := doAs(t, server, http.MethodPost, "/api/projects", "root", "",
		`{"name":"onbehalf","groups":["platform"]}`)
	if asAdmin.Code != http.StatusCreated {
		t.Errorf("the administrator granting a group = %d, want 201: %s", asAdmin.Code, asAdmin.Body)
	}
}

func TestPatchProjectGrants(t *testing.T) {
	store := newStore(t)
	server := identityServer(t, store)

	created := doAs(t, server, http.MethodPost, "/api/projects", "henry", "platform", `{"name":"team"}`)
	if created.Code != http.StatusCreated {
		t.Fatalf("create = %d: %s", created.Code, created.Body)
	}
	var project struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(created.Body.Bytes(), &project); err != nil {
		t.Fatal(err)
	}
	team := "/api/projects/" + project.ID

	if got := doAs(t, server, http.MethodPatch, team, "henry", "platform",
		`{"groups":["platform"]}`); got.Code != http.StatusOK {
		t.Errorf("the owner granting a group they hold = %d, want 200: %s", got.Code, got.Body)
	}
	if got := doAs(t, server, http.MethodGet, team, "carol", "platform", ""); got.Code != http.StatusOK {
		t.Errorf("the newly granted group cannot reach it = %d", got.Code)
	}

	if got := doAs(t, server, http.MethodPatch, team, "henry", "platform",
		`{"groups":["finance"]}`); got.Code != http.StatusForbidden {
		t.Errorf("granting a group the owner does not hold = %d, want 403", got.Code)
	}

	// A member may use the project but not decide who else may.
	if got := doAs(t, server, http.MethodPatch, team, "carol", "platform",
		`{"groups":[]}`); got.Code != http.StatusForbidden {
		t.Errorf("a group member re-granting = %d, want 403", got.Code)
	}
	if got := doAs(t, server, http.MethodPatch, team, "henry", "platform",
		`{"owner":"someone-else"}`); got.Code != http.StatusForbidden {
		t.Errorf("the owner handing the project away = %d, want 403", got.Code)
	}
	if got := doAs(t, server, http.MethodPatch, team, "root", "",
		`{"owner":"someone-else"}`); got.Code != http.StatusOK {
		t.Errorf("the administrator handing the project on = %d, want 200: %s", got.Code, got.Body)
	}

	// A stranger still learns nothing, including that the project exists.
	if got := doAs(t, server, http.MethodPatch, team, "mallory", "",
		`{"groups":[]}`); got.Code != http.StatusNotFound {
		t.Errorf("a stranger = %d, want 404", got.Code)
	}
}

// A project name belongs to its owner. Two people may each have one called the same thing, and
// neither can see the other's.
func TestTwoOwnersMayShareAProjectName(t *testing.T) {
	server := identityServer(t, newStore(t))

	id := func(user string) string {
		t.Helper()
		created := doAs(t, server, http.MethodPost, "/api/projects", user, "", `{"name":"app"}`)
		if created.Code != http.StatusCreated {
			t.Fatalf("%s creating app = %d: %s", user, created.Code, created.Body)
		}
		var project struct {
			ID string `json:"id"`
		}
		if err := json.Unmarshal(created.Body.Bytes(), &project); err != nil {
			t.Fatal(err)
		}
		return project.ID
	}

	henry, carol := id("henry"), id("carol")
	if henry == carol {
		t.Fatal("both owners were given the same project")
	}

	// Neither can reach the other's, and the same owner cannot take the name twice.
	if got := doAs(t, server, http.MethodGet, "/api/projects/"+carol, "henry", "", "").Code; got != http.StatusNotFound {
		t.Errorf("reaching another owner's project of the same name = %d, want 404", got)
	}
	if got := doAs(t, server, http.MethodPost, "/api/projects", "henry", "", `{"name":"app"}`).Code; got != http.StatusConflict {
		t.Errorf("one owner reusing their own name = %d, want 409", got)
	}
}

// A snapshot describes a compute that has stopped and not run since. Starting one invalidates it,
// so a compute that later dies without being read leaves nothing behind to be believed.
func TestStartingABranchDiscardsItsSnapshot(t *testing.T) {
	store := newStore(t)
	branch := seedBranch(t, store)
	branch.LastSeen = &registry.LastSeen{At: time.Now().UTC(), Roles: []string{"app"}}
	if err := store.Put(context.Background(), branch); err != nil {
		t.Fatal(err)
	}
	runtime := newFakeRuntime()
	seedCompute(t, runtime, false)
	server := newTestServer(t, newFakeStorcon(t), store, runtime, newFakeCompute(t))

	if _, err := server.ensureRunning(context.Background(), branch); err != nil {
		t.Fatal(err)
	}
	after, err := store.Branch(context.Background(), testProjectID, "main")
	if err != nil {
		t.Fatal(err)
	}
	if after.LastSeen != nil {
		t.Error("a started branch kept a snapshot of what it held before it started")
	}
}

// Suspending without being able to read the catalog leaves nothing known, so the snapshot that was
// there has to go with it rather than describe a compute that has been running since.
func TestSuspendingWithoutAReadDiscardsTheSnapshot(t *testing.T) {
	store := newStore(t)
	branch := seedBranch(t, store)
	branch.LastSeen = &registry.LastSeen{At: time.Now().UTC(), Roles: []string{"app"}}
	if err := store.Put(context.Background(), branch); err != nil {
		t.Fatal(err)
	}
	computes := newFakeCompute(t)
	computes.catalogFail = true
	runtime := newFakeRuntime()
	instance := seedCompute(t, runtime, true)
	server := newTestServer(t, newFakeStorcon(t), store, runtime, computes)

	if err := server.suspend(context.Background(), &instance); err != nil {
		t.Fatal(err)
	}
	after, err := store.Branch(context.Background(), testProjectID, "main")
	if err != nil {
		t.Fatal(err)
	}
	if after.LastSeen != nil {
		t.Error("a snapshot survived a suspension that could not read the catalog")
	}
}

// Upstream reads any non-SCRAM string as an md5 hash, so an empty one is not an absent password:
// it renders as PASSWORD 'md5', and the ALTER carrying it grants LOGIN to a role that had neither.
func TestARoleWithNoVerifierTravelsAsNull(t *testing.T) {
	store := newStore(t)
	branch := seedBranch(t, store)
	branch.Roles = append(branch.Roles, registry.Role{Name: "analytics"})
	if err := store.Put(context.Background(), branch); err != nil {
		t.Fatal(err)
	}
	runtime := newFakeRuntime()
	instance := seedCompute(t, runtime, true)
	server := newTestServer(t, newFakeStorcon(t), store, runtime, nil)

	spec, err := server.renderSpec(context.Background(), &instance)
	if err != nil {
		t.Fatal(err)
	}
	rendered, err := json.Marshal(spec)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(rendered), `{"name":"analytics","encrypted_password":null`) {
		t.Errorf("a role with no verifier is not rendered as null: %s", rendered)
	}
}
