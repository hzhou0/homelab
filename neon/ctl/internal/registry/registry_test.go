package registry

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

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

	if _, err := store.Branch(ctx, testProjectID, "missing"); !errors.Is(err, ErrNotFound) {
		t.Errorf("Branch on an absent branch = %v, want ErrNotFound", err)
	}
	if err := store.Delete(ctx, testProjectID, "missing"); !errors.Is(err, ErrNotFound) {
		t.Errorf("Delete on an absent branch = %v, want ErrNotFound", err)
	}

	main := mustNew(t, rootSpec())
	if err := store.Put(ctx, main); err != nil {
		t.Fatal(err)
	}

	loaded, err := store.Branch(ctx, testProjectID, "main")
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

	child, err := main.Fork(Spec{Name: "feature", ParentLSN: ptr(neon.LSN(0x16B374D848))}, testProject(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put(ctx, child); err != nil {
		t.Fatal(err)
	}

	branches, err := store.Branches(ctx, testProjectID)
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

	if err := store.Delete(ctx, testProjectID, "feature"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Branch(ctx, testProjectID, "feature"); !errors.Is(err, ErrNotFound) {
		t.Errorf("Branch after Delete = %v, want ErrNotFound", err)
	}
	if _, err := store.Endpoint(ctx, child.EndpointID); !errors.Is(err, ErrNotFound) {
		t.Errorf("Endpoint after Delete = %v, want ErrNotFound", err)
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
	loaded, err := store.Endpoint(ctx, branch.EndpointID)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Mode != branch.Mode {
		t.Errorf("mode = %+v, want %+v", loaded.Mode, branch.Mode)
	}
}

// A branch name reaches a Kubernetes object name, a proxy endpoint id and a path, so anything
// legal in only some of those has to be refused at the door.
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

// Fixed rather than generated, so every call in a test names the same project.
const testProjectID = "pr-test-project-aaaaaaaa"

func testProject(t *testing.T) *Project {
	t.Helper()
	now := time.Now().UTC()
	return &Project{
		ID:        testProjectID,
		Name:      "acme",
		TenantID:  testTenant(t),
		Owner:     "henry",
		Groups:    []string{"platform"},
		CreatedAt: now,
		UpdatedAt: now,
	}
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
	branch, err := New(spec, pgVersion, testProject(t))
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

	child, err := parent.Fork(Spec{Name: "feature"}, testProject(t))
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

	child, err := parent.Fork(Spec{Name: "feature", ParentLSN: &lsn}, testProject(t))
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
	}, testProject(t))
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
	child, err := root.Fork(Spec{Name: "feature", ParentLSN: &lsn}, testProject(t))
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
			if _, err := New(tc.spec, pgVersion, testProject(t)); err == nil {
				t.Error("accepted")
			}
		})
	}

	// The version and the project are the caller's to supply; neither has a safe fallback.
	if _, err := New(rootSpec(), 0, testProject(t)); err == nil {
		t.Error("a branch was created without a postgres version")
	}
	if _, err := New(rootSpec(), pgVersion, nil); err == nil {
		t.Error("a branch was created without a project")
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

// The point of the whole model: a name is unique inside its project, not across the cluster.
func TestSameBranchNameInTwoProjects(t *testing.T) {
	ctx := context.Background()
	store := newStore(t)

	other, err := neon.ParseTenantID("99887766554433221122334455667788")
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewProject("beta", "someone", nil, other)
	if err != nil {
		t.Fatal(err)
	}

	acme := mustNew(t, rootSpec())
	beta, err := New(rootSpec(), pgVersion, second)
	if err != nil {
		t.Fatal(err)
	}
	if acme.EndpointID == beta.EndpointID {
		t.Fatal("two branches were given the same endpoint id")
	}
	for _, branch := range []*Branch{acme, beta} {
		if err := store.Put(ctx, branch); err != nil {
			t.Fatal(err)
		}
	}

	for _, tc := range []struct {
		project string
		want    *Branch
	}{{testProjectID, acme}, {second.ID, beta}} {
		loaded, err := store.Branch(ctx, tc.project, "main")
		if err != nil {
			t.Fatal(err)
		}
		if loaded.EndpointID != tc.want.EndpointID || loaded.TenantID != tc.want.TenantID {
			t.Errorf("%s/main resolved to %+v", tc.project, loaded)
		}
	}

	// Deleting one must not disturb the other's index entry.
	if err := store.Delete(ctx, testProjectID, "main"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Branch(ctx, second.ID, "main"); err != nil {
		t.Errorf("deleting acme/main also removed beta/main: %v", err)
	}
}

func TestEndpointIDIsASingleDNSLabel(t *testing.T) {
	seen := map[string]bool{}
	for range 200 {
		endpoint, err := NewEndpointID()
		if err != nil {
			t.Fatal(err)
		}
		// The proxy reads this out of an SNI name under a wildcard that matches one label, so
		// anything ValidateName rejects would be unreachable.
		if err := ValidateName(endpoint); err != nil {
			t.Fatalf("endpoint id %q is not a legal label: %v", endpoint, err)
		}
		if seen[endpoint] {
			t.Fatalf("endpoint id %q was generated twice", endpoint)
		}
		seen[endpoint] = true
	}
}

func TestProjectPermits(t *testing.T) {
	project := testProject(t)
	for _, tc := range []struct {
		name   string
		user   string
		groups []string
		want   bool
	}{
		{"the owner", "henry", nil, true},
		{"a member of a granted group", "someone", []string{"platform"}, true},
		{"a stranger", "someone", []string{"other"}, false},
		{"nobody", "", nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := project.Permits(tc.user, tc.groups); got != tc.want {
				t.Errorf("Permits(%q, %v) = %v, want %v", tc.user, tc.groups, got, tc.want)
			}
		})
	}
}

// An unowned project is what a claim-less import looks like; it must not fall open.
func TestUnownedProjectPermitsNobody(t *testing.T) {
	project := &Project{Name: "orphan", TenantID: testTenant(t)}
	if project.Permits("", nil) || project.Permits("henry", []string{"platform"}) {
		t.Error("an unowned project admitted somebody")
	}
}

func TestDeleteProjectRefusesWhileBranchesRemain(t *testing.T) {
	ctx := context.Background()
	store := newStore(t)

	project := testProject(t)
	if err := store.CreateProject(ctx, project); err != nil {
		t.Fatal(err)
	}
	branch := mustNew(t, rootSpec())
	if err := store.Put(ctx, branch); err != nil {
		t.Fatal(err)
	}

	if err := store.DeleteProject(ctx, testProjectID); err == nil {
		t.Error("a project with branches was deleted, orphaning its timelines")
	}
	if err := store.Delete(ctx, testProjectID, "main"); err != nil {
		t.Fatal(err)
	}
	if err := store.DeleteProject(ctx, testProjectID); err != nil {
		t.Errorf("an empty project could not be deleted: %v", err)
	}
	if _, err := store.Project(ctx, testProjectID); !errors.Is(err, ErrProjectNotFound) {
		t.Errorf("Project after delete = %v, want ErrProjectNotFound", err)
	}
}

func TestForkRefusesAnotherProject(t *testing.T) {
	parent := mustNew(t, rootSpec())
	other, err := neon.ParseTenantID("99887766554433221122334455667788")
	if err != nil {
		t.Fatal(err)
	}
	stranger, err := NewProject("beta", "someone", nil, other)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := parent.Fork(Spec{Name: "feature"}, stranger); err == nil {
		t.Error("a branch was forked into a project that does not own its tenant")
	}
}

func TestCreateRefusesToOverwrite(t *testing.T) {
	ctx := context.Background()
	store := newStore(t)

	main := mustNew(t, rootSpec())
	if err := store.Create(ctx, main); err != nil {
		t.Fatal(err)
	}

	// A second branch of the same name in the same project, with its own endpoint id.
	again := mustNew(t, rootSpec())
	if again.EndpointID == main.EndpointID {
		t.Fatal("two branches were given the same endpoint id")
	}
	if err := store.Create(ctx, again); !errors.Is(err, ErrNameTaken) {
		t.Errorf("Create with a used name = %v, want ErrNameTaken", err)
	}

	// The refused create must not have left the index pointing at it.
	loaded, err := store.Branch(ctx, testProjectID, "main")
	if err != nil {
		t.Fatal(err)
	}
	if loaded.EndpointID != main.EndpointID {
		t.Errorf("endpoint = %q, want the original %q", loaded.EndpointID, main.EndpointID)
	}
	if _, err := store.Endpoint(ctx, again.EndpointID); !errors.Is(err, ErrNotFound) {
		t.Errorf("the refused branch was written anyway: %v", err)
	}
}

// The case the entropy makes unreachable, checked because the consequence is destroying a branch
// belonging to another project.
func TestCreateRefusesADuplicateEndpointID(t *testing.T) {
	ctx := context.Background()
	store := newStore(t)

	main := mustNew(t, rootSpec())
	if err := store.Create(ctx, main); err != nil {
		t.Fatal(err)
	}

	other, err := neon.ParseTenantID("99887766554433221122334455667788")
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewProject("beta", "someone", nil, other)
	if err != nil {
		t.Fatal(err)
	}
	collider, err := New(rootSpec(), pgVersion, second)
	if err != nil {
		t.Fatal(err)
	}
	collider.EndpointID = main.EndpointID

	if err := store.Create(ctx, collider); !errors.Is(err, ErrEndpointTaken) {
		t.Errorf("Create with a used endpoint id = %v, want ErrEndpointTaken", err)
	}
	loaded, err := store.Endpoint(ctx, main.EndpointID)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.ProjectID != testProjectID {
		t.Errorf("endpoint now resolves to project %q, want the original", loaded.ProjectID)
	}
}

// A project name belongs to its owner, not to the cluster.
func TestSameProjectNameForTwoOwners(t *testing.T) {
	ctx := context.Background()
	store := newStore(t)

	other, err := neon.ParseTenantID("99887766554433221122334455667788")
	if err != nil {
		t.Fatal(err)
	}
	mine, err := NewProject("app", "henry", nil, testTenant(t))
	if err != nil {
		t.Fatal(err)
	}
	theirs, err := NewProject("app", "someone", nil, other)
	if err != nil {
		t.Fatal(err)
	}
	if mine.ID == theirs.ID {
		t.Fatal("two projects were given the same id")
	}
	for _, project := range []*Project{mine, theirs} {
		if err := store.CreateProject(ctx, project); err != nil {
			t.Fatalf("creating %s for %s: %v", project.Name, project.Owner, err)
		}
	}

	// The same owner may not reuse it, and the refusal must not have disturbed the original.
	again, err := NewProject("app", "henry", nil, testTenant(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CreateProject(ctx, again); !errors.Is(err, ErrNameTaken) {
		t.Errorf("the same owner reusing a name = %v, want ErrNameTaken", err)
	}
	if _, err := store.Project(ctx, mine.ID); err != nil {
		t.Errorf("the original was disturbed: %v", err)
	}
	if _, err := store.Project(ctx, again.ID); !errors.Is(err, ErrProjectNotFound) {
		t.Errorf("the refused project was written anyway: %v", err)
	}
}

// Renaming has to move the index entry, or the old name stays unusable forever.
func TestRenameFreesTheOldName(t *testing.T) {
	ctx := context.Background()
	store := newStore(t)

	project, err := NewProject("before", "henry", nil, testTenant(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CreateProject(ctx, project); err != nil {
		t.Fatal(err)
	}

	project.Name = "after"
	if err := store.PutProject(ctx, project); err != nil {
		t.Fatal(err)
	}

	reused, err := NewProject("before", "henry", nil, testTenant(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CreateProject(ctx, reused); err != nil {
		t.Errorf("the vacated name is still taken: %v", err)
	}
	renamed, err := store.Project(ctx, project.ID)
	if err != nil {
		t.Fatal(err)
	}
	if renamed.Name != "after" {
		t.Errorf("name = %q, want after", renamed.Name)
	}
}

func TestABranchRefusesARepeatedName(t *testing.T) {
	project := testProject(t)
	for _, test := range []struct {
		name string
		spec Spec
	}{
		{"role twice", Spec{
			Name:  "main",
			Roles: []RoleSpec{{Name: "app", Password: "a"}, {Name: "app", Password: "b"}},
		}},
		{"database twice", Spec{
			Name:      "main",
			Roles:     []RoleSpec{{Name: "app", Password: "a"}},
			Databases: []Database{{Name: "appdb", Owner: "app"}, {Name: "appdb", Owner: "app"}},
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := New(test.spec, 17, project); err == nil {
				t.Error("a repeated name was accepted")
			}
		})
	}
}
