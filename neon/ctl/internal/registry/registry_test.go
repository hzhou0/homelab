package registry

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/hzhou0/homelab/neon/ctl/internal/neon"
)

func newStore(t *testing.T) *Store {
	t.Helper()
	store, err := Open(filepath.Join(t.TempDir(), "registry.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	return store
}

func TestStoreRoundTrip(t *testing.T) {
	ctx := context.Background()
	store := newStore(t)

	if _, err := store.Get(ctx, "missing"); !errors.Is(err, ErrNotFound) {
		t.Errorf("Get on an absent branch = %v, want ErrNotFound", err)
	}
	if err := store.Delete(ctx, "missing"); !errors.Is(err, ErrNotFound) {
		t.Errorf("Delete on an absent branch = %v, want ErrNotFound", err)
	}

	main := mustNew(t, rootSpec())
	if err := store.Put(ctx, main); err != nil {
		t.Fatal(err)
	}

	loaded, err := store.Get(ctx, "main")
	if err != nil {
		t.Fatal(err)
	}
	if loaded.TenantID != main.TenantID || loaded.TimelineID != main.TimelineID {
		t.Errorf("ids did not survive: %+v", loaded)
	}
	if len(loaded.Roles) != 1 || loaded.Roles[0] != main.Roles[0] {
		t.Errorf("roles did not survive: %+v", loaded.Roles)
	}
	if len(loaded.Databases) != 1 || len(loaded.Settings) != 1 {
		t.Errorf("catalog did not survive: %+v / %+v", loaded.Databases, loaded.Settings)
	}
	if !loaded.CreatedAt.Equal(main.CreatedAt) {
		t.Errorf("created_at = %v, want %v", loaded.CreatedAt, main.CreatedAt)
	}

	child, err := main.Fork(Spec{Name: "feature", ParentLSN: ptr(neon.LSN(0x16B374D848))})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put(ctx, child); err != nil {
		t.Fatal(err)
	}

	branches, err := store.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(branches) != 2 || branches[0].Name != "feature" || branches[1].Name != "main" {
		t.Fatalf("List returned %+v, want feature then main", branches)
	}
	if branches[0].ParentTimelineID == nil || *branches[0].ParentTimelineID != main.TimelineID {
		t.Errorf("ancestry did not survive: %+v", branches[0].ParentTimelineID)
	}
	if branches[0].ParentLSN == nil || branches[0].ParentLSN.String() != "16/B374D848" {
		t.Errorf("parent lsn did not survive: %+v", branches[0].ParentLSN)
	}

	if err := store.Delete(ctx, "feature"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Get(ctx, "feature"); !errors.Is(err, ErrNotFound) {
		t.Errorf("Get after Delete = %v, want ErrNotFound", err)
	}
}

// A static compute pins an LSN, which has to survive separately from the mode itself.
func TestStaticModeSurvivesAWrite(t *testing.T) {
	ctx := context.Background()
	store := newStore(t)

	branch := mustNew(t, rootSpec())
	branch.Mode = neon.ComputeMode{Kind: neon.ModeStatic, LSN: 0x16B374D848}
	if err := store.Put(ctx, branch); err != nil {
		t.Fatal(err)
	}
	loaded, err := store.Get(ctx, "main")
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Mode != branch.Mode {
		t.Errorf("mode = %+v, want %+v", loaded.Mode, branch.Mode)
	}
}

// A rewrite replaces the child rows rather than merging them: dropping a setting has to remove it.
// A branch name reaches a Kubernetes object name, a proxy endpoint id and a path, so anything
// that would be legal in only some of those has to be refused at the door.
func TestValidateName(t *testing.T) {
	for _, name := range []string{"main", "feature-1", "a", "a1"} {
		if err := ValidateName(name); err != nil {
			t.Errorf("ValidateName(%q) = %v", name, err)
		}
	}
	for _, name := range []string{
		"", "-main", "main-", "Main", "main_branch", "main.branch", "../escape", "main/sub",
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	} {
		if err := ValidateName(name); !errors.Is(err, ErrInvalidName) {
			t.Errorf("ValidateName(%q) = %v, want ErrInvalidName", name, err)
		}
	}
}

const pgVersion = 17

func testTenant(t *testing.T) neon.TenantID {
	t.Helper()
	tenant, err := neon.ParseTenantID("1a2b3344556677881122334455667788")
	if err != nil {
		t.Fatal(err)
	}
	return tenant
}

func rootSpec() Spec {
	return Spec{
		Name:      "main",
		Roles:     []RoleSpec{{Name: "app", Password: "hunter2"}},
		Databases: []Database{{Name: "appdb", Owner: "app"}},
		Settings:  []Setting{{Name: "work_mem", Value: "64MB", VarType: "string"}},
	}
}

func mustNew(t *testing.T, spec Spec) *Branch {
	t.Helper()
	branch, err := New(spec, pgVersion, testTenant(t))
	if err != nil {
		t.Fatal(err)
	}
	return branch
}

func TestNewAppliesDefaults(t *testing.T) {
	branch := mustNew(t, rootSpec())

	if branch.PgVersion != pgVersion {
		t.Errorf("pg version = %d, want the compute's", branch.PgVersion)
	}
	if branch.Mode.Kind != neon.ModePrimary {
		t.Errorf("mode = %q, want Primary", branch.Mode.Kind)
	}
	if branch.TimelineID.IsZero() {
		t.Error("a new branch must carry the timeline it is about to create")
	}
	if branch.CreatedAt.IsZero() || !branch.UpdatedAt.Equal(branch.CreatedAt) {
		t.Errorf("timestamps = %v, %v", branch.CreatedAt, branch.UpdatedAt)
	}

	// A password is hashed at construction and never held.
	if len(branch.Roles) != 1 || branch.Roles[0].Verifier == "hunter2" {
		t.Fatalf("roles = %+v", branch.Roles)
	}
	if !isVerifier(branch.Roles[0].Verifier) {
		t.Errorf("stored secret is not a verifier: %q", branch.Roles[0].Verifier)
	}
}

// A timeline can only be branched inside its own tenant, and the catalog comes with the timeline,
// so a fork that invented either would be describing something that does not exist.
func TestForkInheritsTenantAndCatalog(t *testing.T) {
	parent := mustNew(t, rootSpec())
	parent.PgVersion = 16

	child, err := parent.Fork(Spec{Name: "feature"})
	if err != nil {
		t.Fatal(err)
	}

	if child.TenantID != parent.TenantID {
		t.Error("a fork must live in its parent's tenant")
	}
	if child.TimelineID == parent.TimelineID {
		t.Error("a fork must get its own timeline")
	}
	if len(child.Roles) != 1 || child.Roles[0] != parent.Roles[0] {
		t.Errorf("roles = %+v, want the parent's", child.Roles)
	}
	if len(child.Databases) != 1 || len(child.Settings) != 1 {
		t.Errorf("catalog = %+v / %+v", child.Databases, child.Settings)
	}
	// Neon's branching code always inherits the ancestor's version.
	if child.PgVersion != 16 {
		t.Errorf("pg version = %d, want the parent's", child.PgVersion)
	}
}

func TestForkRecordsAncestry(t *testing.T) {
	parent := mustNew(t, rootSpec())
	lsn := neon.LSN(0x16B374D848)

	child, err := parent.Fork(Spec{Name: "feature", ParentLSN: &lsn})
	if err != nil {
		t.Fatal(err)
	}
	if child.Parent != "main" {
		t.Errorf("parent = %q", child.Parent)
	}
	if child.ParentTimelineID == nil || *child.ParentTimelineID != parent.TimelineID {
		t.Errorf("parent timeline = %v", child.ParentTimelineID)
	}
	if child.ParentLSN == nil || *child.ParentLSN != lsn {
		t.Errorf("parent lsn = %v", child.ParentLSN)
	}
}

func TestForkAcceptsOverrides(t *testing.T) {
	parent := mustNew(t, rootSpec())

	child, err := parent.Fork(Spec{
		Name:      "feature",
		Roles:     []RoleSpec{{Name: "reader", Password: "other"}},
		Databases: []Database{{Name: "readerdb", Owner: "reader"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(child.Roles) != 1 || child.Roles[0].Name != "reader" {
		t.Errorf("roles = %+v, want the override", child.Roles)
	}
	if len(child.Settings) != 1 {
		t.Error("settings were not inherited when only roles were overridden")
	}
}

// The request is an untagged Rust enum, so the wrong shape is read as the wrong variant rather
// than rejected: naming an ancestor selects branching, omitting one selects bootstrap.
func TestTimelineCreateRequest(t *testing.T) {
	root := mustNew(t, rootSpec())
	request := root.TimelineCreateRequest()
	if request.NewTimelineID != root.TimelineID {
		t.Error("the request does not name the branch's own timeline")
	}
	if request.AncestorTimelineID != nil || request.AncestorStartLSN != nil {
		t.Error("a root branch must bootstrap, not branch")
	}
	if request.PgVersion == nil || *request.PgVersion != pgVersion {
		t.Errorf("pg version = %v", request.PgVersion)
	}

	lsn := neon.LSN(0x16B374D848)
	child, err := root.Fork(Spec{Name: "feature", ParentLSN: &lsn})
	if err != nil {
		t.Fatal(err)
	}
	request = child.TimelineCreateRequest()
	if request.AncestorTimelineID == nil || *request.AncestorTimelineID != root.TimelineID {
		t.Errorf("ancestor = %v", request.AncestorTimelineID)
	}
	if request.AncestorStartLSN == nil || *request.AncestorStartLSN != lsn {
		t.Errorf("ancestor lsn = %v", request.AncestorStartLSN)
	}
}

func TestValidateRejectsUnreachableAndIncoherentBranches(t *testing.T) {
	for _, tc := range []struct {
		name string
		spec Spec
	}{
		{"no roles", Spec{Name: "main"}},
		{"unusable name", Spec{Name: "../escape", Roles: []RoleSpec{{Name: "a", Password: "b"}}}},
		{"role with no secret", Spec{Name: "main", Roles: []RoleSpec{{Name: "a"}}}},
		{"role with no name", Spec{Name: "main", Roles: []RoleSpec{{Password: "b"}}}},
		{"not a verifier", Spec{Name: "main", Roles: []RoleSpec{{Name: "a", Verifier: "hunter2"}}}},
		{
			"database owned by nobody",
			Spec{
				Name:      "main",
				Roles:     []RoleSpec{{Name: "app", Password: "b"}},
				Databases: []Database{{Name: "appdb", Owner: "ghost"}},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := New(tc.spec, pgVersion, testTenant(t)); err == nil {
				t.Error("accepted")
			}
		})
	}

	// The version and the tenant are the caller's to supply; neither has a safe fallback.
	if _, err := New(rootSpec(), 0, testTenant(t)); err == nil {
		t.Error("a branch was created without a postgres version")
	}
	if _, err := New(rootSpec(), pgVersion, neon.TenantID{}); err == nil {
		t.Error("a branch was created without a tenant")
	}
}

func TestApplyPatchesOnlyWhatIsGiven(t *testing.T) {
	branch := mustNew(t, rootSpec())
	before := branch.Roles[0].Verifier

	settings := []Setting{{Name: "max_connections", Value: "50", VarType: "integer"}}
	if err := branch.Apply(Patch{Settings: &settings}); err != nil {
		t.Fatal(err)
	}
	if len(branch.Settings) != 1 || branch.Settings[0].Name != "max_connections" {
		t.Errorf("settings = %+v", branch.Settings)
	}
	if branch.Roles[0].Verifier != before || len(branch.Databases) != 1 {
		t.Error("a patch touched fields it was not given")
	}
	if !branch.UpdatedAt.After(branch.CreatedAt) {
		t.Error("a patch did not advance updated_at")
	}

	empty := []Setting{}
	if err := branch.Apply(Patch{Settings: &empty}); err != nil {
		t.Fatal(err)
	}
	if len(branch.Settings) != 0 {
		t.Error("an empty slice must clear rather than be ignored")
	}
}

// A rejected patch must leave the branch as it was, not half-applied.
func TestApplyIsAllOrNothing(t *testing.T) {
	branch := mustNew(t, rootSpec())
	settings := []Setting{{Name: "work_mem", Value: "128MB", VarType: "string"}}
	roles := []RoleSpec{{Name: "app"}}

	if err := branch.Apply(Patch{Settings: &settings, Roles: &roles}); err == nil {
		t.Fatal("a role with no secret was accepted")
	}
	if branch.Settings[0].Value != "64MB" {
		t.Errorf("settings = %+v, want the original", branch.Settings)
	}
}

func ptr[T any](value T) *T { return &value }

func isVerifier(secret string) bool {
	return len(secret) > 14 && secret[:14] == "SCRAM-SHA-256$"
}
