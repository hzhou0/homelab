package controlplane

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"testing"
	"time"

	"crypto/rand"

	"github.com/hzhou0/homelab/neon/ctl/internal/kube"
	"github.com/hzhou0/homelab/neon/ctl/internal/neon"
	"github.com/hzhou0/homelab/neon/ctl/internal/registry"
	"github.com/hzhou0/homelab/neon/ctl/internal/scram"
	"github.com/hzhou0/homelab/neon/ctl/internal/secret"
)

func form(t *testing.T, server *Server, method, target, user, groups string, values url.Values) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(method, target, strings.NewReader(values.Encode()))
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
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

// Rendering happens into a buffer, so a template that does not exist or does not execute is a 500
// rather than a page that swaps in half-written.
func TestEveryPageRenders(t *testing.T) {
	store := newStore(t)
	seedBranch(t, store)
	server := identityServer(t, store)

	for _, target := range []string{
		"/",
		"/ui/projects",
		"/ui/projects/" + testProjectID,
		"/ui/projects/" + testProjectID + "/branches",
	} {
		response := doAs(t, server, http.MethodGet, target, "tester", "", "")
		if response.Code != http.StatusOK {
			t.Errorf("GET %s = %d, body = %s", target, response.Code, response.Body)
		}
		if got := response.Header().Get("Content-Type"); !strings.HasPrefix(got, "text/html") {
			t.Errorf("GET %s content type = %q", target, got)
		}
	}
}

func TestProjectPageShowsItsBranches(t *testing.T) {
	store := newStore(t)
	seedBranch(t, store)
	server := identityServer(t, store)
	server.opts.EndpointSuffix = "pg.example.net"

	body := doAs(t, server, http.MethodGet, "/ui/projects/"+testProjectID, "tester", "", "").Body.String()
	for _, want := range []string{testProjectName, "main"} {
		if !strings.Contains(body, want) {
			t.Errorf("project page does not mention %q", want)
		}
	}
}

// The page is the same model the API serves, so it hides what the API hides.
func TestThePageOnlyListsProjectsTheCallerHolds(t *testing.T) {
	store := newStore(t)
	server := identityServer(t, store)

	if body := doAs(t, server, http.MethodGet, "/", "mallory", "", "").Body.String(); strings.Contains(body, testProjectName) {
		t.Error("a project listed for somebody who does not hold it")
	}
	if got := doAs(t, server, http.MethodGet, "/ui/projects/"+testProjectID, "mallory", "", "").Code; got != http.StatusNotFound {
		t.Errorf("status = %d, want 404", got)
	}
}

func TestCreatingAProjectFromTheForm(t *testing.T) {
	store := newStore(t)
	server := identityServer(t, store)

	response := form(t, server, http.MethodPost, "/ui/projects", "carol", "platform",
		url.Values{"name": {"widgets"}, "groups": {"platform"}})
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body)
	}
	if !strings.Contains(response.Body.String(), "widgets") {
		t.Error("the new project is missing from the list it answered with")
	}
}

// The form is no wider a door than the API: a group the caller is not in is refused there too.
func TestTheFormCannotGrantAGroupTheCallerIsNotIn(t *testing.T) {
	store := newStore(t)
	server := identityServer(t, store)

	response := form(t, server, http.MethodPost, "/ui/projects", "mallory", "",
		url.Values{"name": {"widgets"}, "groups": {"platform"}})
	if response.Code != http.StatusForbidden {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body)
	}
	if !strings.Contains(response.Body.String(), "groups you belong to") {
		t.Error("the refusal is not rendered where the form can show it")
	}

	projects, err := store.Projects(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(projects) != 1 {
		t.Errorf("projects = %d, want only the seeded one", len(projects))
	}
}

func TestGrantingGroupsFromTheProjectPage(t *testing.T) {
	store := newStore(t)
	server := identityServer(t, store)

	response := form(t, server, http.MethodPatch, "/ui/projects/"+testProjectID, "tester", "platform",
		url.Values{"groups": {"platform"}})
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body)
	}
	project, err := store.Project(context.Background(), testProjectID)
	if err != nil {
		t.Fatal(err)
	}
	if len(project.Groups) != 1 || project.Groups[0] != "platform" {
		t.Errorf("groups = %v, want [platform]", project.Groups)
	}
}

// A group member may use a project but not decide who else may, and the refusal lands beside the
// form rather than replacing the branch list.
func TestAGroupMemberCannotRegrantFromThePage(t *testing.T) {
	store := newStore(t)
	project, err := store.Project(context.Background(), testProjectID)
	if err != nil {
		t.Fatal(err)
	}
	project.Groups = []string{"platform"}
	if err := store.PutProject(context.Background(), project); err != nil {
		t.Fatal(err)
	}
	server := identityServer(t, store)

	response := form(t, server, http.MethodPatch, "/ui/projects/"+testProjectID, "carol", "platform",
		url.Values{"groups": {}})
	if response.Code != http.StatusForbidden {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body)
	}
	after, err := store.Project(context.Background(), testProjectID)
	if err != nil {
		t.Fatal(err)
	}
	if len(after.Groups) != 1 {
		t.Errorf("groups = %v, want the grant left alone", after.Groups)
	}
}

func TestBranchActionsFromThePage(t *testing.T) {
	store := newStore(t)
	seedBranch(t, store)
	runtime := newFakeRuntime()
	server := newTestServer(t, newFakeStorcon(t), store, runtime, nil)

	response := form(t, server, http.MethodPost,
		"/ui/projects/"+testProjectID+"/branches/main/start", "tester", "", nil)
	if response.Code != http.StatusOK {
		t.Fatalf("start = %d, body = %s", response.Code, response.Body)
	}
	if _, err := runtime.Get(context.Background(), testEndpointID); err != nil {
		t.Fatalf("starting from the page did not create a compute: %v", err)
	}

	response = form(t, server, http.MethodDelete,
		"/ui/projects/"+testProjectID+"/branches/main", "tester", "", nil)
	if response.Code != http.StatusOK {
		t.Fatalf("delete = %d, body = %s", response.Code, response.Body)
	}
	if _, err := store.Branch(context.Background(), testProjectID, "main"); err == nil {
		t.Error("the branch survived a delete from the page")
	}
	if !strings.Contains(response.Body.String(), "no branches") {
		t.Error("the list it answered with still shows the deleted branch")
	}
}

// Vendored rather than fetched, so the page works on a network that can reach nothing but this.
func TestAssetsAreServedFromTheBinary(t *testing.T) {
	server := identityServer(t, newStore(t))
	for _, asset := range []string{"htmx.min.js", "style.css"} {
		response := doAs(t, server, http.MethodGet, "/ui/assets/"+asset, "tester", "", "")
		if response.Code != http.StatusOK || response.Body.Len() == 0 {
			t.Errorf("GET %s = %d, %d bytes", asset, response.Code, response.Body.Len())
		}
	}
}

// The form is the only way most people will ever create a branch, and a branch with no role is one
// the registry refuses and nobody could connect to.
func TestCreatingARootBranchFromTheForm(t *testing.T) {
	store := newStore(t)
	server := identityServer(t, store)
	server.opts.EndpointSuffix = "pg.example.net"

	response := form(t, server, http.MethodPost, "/ui/projects/"+testProjectID+"/branches", "tester", "",
		url.Values{"name": {"trunk"}, "role": {"app"}, "database": {"appdb"}, "pg_version": {"17"}})
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body)
	}

	branch, err := store.Branch(context.Background(), testProjectID, "trunk")
	if err != nil {
		t.Fatal(err)
	}
	if len(branch.Roles) != 1 || branch.Roles[0].Name != "app" {
		t.Fatalf("roles = %v, want the one the form named", branch.Roles)
	}
	if !scram.IsVerifier(branch.Roles[0].Verifier) {
		t.Error("the generated password was not turned into a verifier")
	}

	// The password exists nowhere else, so the answer to the request that set it is the only place
	// it can ever be read.
	body := response.Body.String()
	if !strings.Contains(body, "postgresql://app:") || !strings.Contains(body, "@ep-") {
		t.Errorf("the connection string is not in the answer: %s", body)
	}
}

func TestAForkInheritsRatherThanBeingGivenARole(t *testing.T) {
	store := newStore(t)
	seedBranch(t, store)
	server := identityServer(t, store)

	response := form(t, server, http.MethodPost, "/ui/projects/"+testProjectID+"/branches", "tester", "",
		url.Values{"name": {"child"}, "parent": {"main"}, "role": {"intruder"}})
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body)
	}
	branch, err := store.Branch(context.Background(), testProjectID, "child")
	if err != nil {
		t.Fatal(err)
	}
	if len(branch.Roles) != 1 || branch.Roles[0].Name != "app" {
		t.Errorf("roles = %v, want the ancestor's", branch.Roles)
	}
}

func TestThePageShowsWhereToConnectAndAsWhom(t *testing.T) {
	store := newStore(t)
	seedBranch(t, store)
	server := identityServer(t, store)
	server.opts.EndpointSuffix = "pg.example.net"

	body := doAs(t, server, http.MethodGet, "/ui/projects/"+testProjectID+"/branches/main", "tester", "", "").Body.String()
	for _, want := range []string{
		"app",
		"appdb",
		testEndpointID + ".pg.example.net",
		"postgresql://app@" + testEndpointID + ".pg.example.net/appdb?sslmode=require",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("the branch does not show %q", want)
		}
	}
	if strings.Contains(body, "SCRAM-SHA-256") {
		t.Error("the page prints the role's verifier")
	}
}

func TestReplacingARolePassword(t *testing.T) {
	store := newStore(t)
	branch := seedBranch(t, store)
	server := identityServer(t, store)

	response := form(t, server, http.MethodPost,
		"/ui/projects/"+testProjectID+"/branches/main/roles/app/password", "tester", "", nil)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body)
	}
	if !strings.Contains(response.Body.String(), "postgresql://app:") {
		t.Error("the new password is not in the answer that set it")
	}

	after, err := store.Branch(context.Background(), testProjectID, "main")
	if err != nil {
		t.Fatal(err)
	}
	if after.Roles[0].Verifier == branch.Roles[0].Verifier {
		t.Error("the verifier did not change")
	}
}

// A refusal the page cannot render into the section it was asked to replace would otherwise reach
// the browser as JSON, which htmx swaps into the document as text.
func TestARefusedPageAnswersWithAPageAndAnHtmxRequestWithARedirect(t *testing.T) {
	server := identityServer(t, newStore(t))

	response := doAs(t, server, http.MethodGet, "/ui/projects/pr-absent-project-aaaaaaa", "tester", "", "")
	if response.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", response.Code)
	}
	if got := response.Header().Get("Content-Type"); !strings.HasPrefix(got, "text/html") {
		t.Errorf("content type = %q, want html", got)
	}
	if body := response.Body.String(); !strings.Contains(body, "<!doctype html>") {
		t.Errorf("a refusal answered with something that is not a page: %s", body)
	}

	request := httptest.NewRequest(http.MethodDelete, "/ui/projects/pr-absent-project-aaaaaaa", nil)
	request.Header.Set("Remote-User", "tester")
	request.Header.Set("HX-Request", "true")
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, request)
	if got := recorder.Header().Get("HX-Redirect"); got != "/" {
		t.Errorf("HX-Redirect = %q, want /", got)
	}
	if recorder.Body.Len() != 0 {
		t.Errorf("the refusal carries a body htmx would swap: %s", recorder.Body)
	}
}

func TestAddingARoleAndADatabaseToABranch(t *testing.T) {
	store := newStore(t)
	seedBranch(t, store)
	server := identityServer(t, store)
	server.opts.EndpointSuffix = "pg.example.net"

	response := form(t, server, http.MethodPost, "/ui/projects/"+testProjectID+"/branches/main/roles",
		"tester", "", url.Values{"role": {"reporting"}})
	if response.Code != http.StatusOK {
		t.Fatalf("adding a role = %d, body = %s", response.Code, response.Body)
	}
	if !strings.Contains(response.Body.String(), "postgresql://reporting:") {
		t.Error("the password of the new role is not in the answer that set it")
	}

	response = form(t, server, http.MethodPost, "/ui/projects/"+testProjectID+"/branches/main/databases",
		"tester", "", url.Values{"database": {"analytics"}, "owner": {"reporting"}})
	if response.Code != http.StatusOK {
		t.Fatalf("adding a database = %d, body = %s", response.Code, response.Body)
	}

	branch, err := store.Branch(context.Background(), testProjectID, "main")
	if err != nil {
		t.Fatal(err)
	}
	if len(branch.Roles) != 2 || len(branch.Databases) != 2 {
		t.Fatalf("roles = %v, databases = %v, want the seeded pair plus the added one", branch.Roles, branch.Databases)
	}
	// The role that was already there keeps the password nobody can read back.
	if branch.Roles[0].Verifier != "SCRAM-SHA-256$4096:c2FsdA==$c3RvcmVk:c2VydmVy" {
		t.Error("adding a role disturbed the credentials of the one beside it")
	}

	body := doAs(t, server, http.MethodGet, "/ui/projects/"+testProjectID+"/branches/main", "tester", "", "").Body.String()
	for _, want := range []string{"app", "reporting", "appdb", "analytics"} {
		if !strings.Contains(body, want) {
			t.Errorf("the branch page does not offer %q", want)
		}
	}
}

func TestABranchRefusesADatabaseNobodyOnItOwns(t *testing.T) {
	store := newStore(t)
	seedBranch(t, store)
	server := identityServer(t, store)

	response := form(t, server, http.MethodPost, "/ui/projects/"+testProjectID+"/branches/main/databases",
		"tester", "", url.Values{"database": {"analytics"}, "owner": {"nobody"}})
	if response.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", response.Code)
	}
	if !strings.Contains(response.Body.String(), "not a role on this branch") {
		t.Errorf("the refusal is not rendered where the form can show it: %s", response.Body)
	}
	branch, err := store.Branch(context.Background(), testProjectID, "main")
	if err != nil {
		t.Fatal(err)
	}
	if len(branch.Databases) != 1 {
		t.Errorf("databases = %v, want the refusal to have changed nothing", branch.Databases)
	}
}

func TestANameCannotBeAddedTwice(t *testing.T) {
	store := newStore(t)
	seedBranch(t, store)
	server := identityServer(t, store)

	response := form(t, server, http.MethodPost, "/ui/projects/"+testProjectID+"/branches/main/roles",
		"tester", "", url.Values{"role": {"app"}})
	if response.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", response.Code)
	}
	branch, err := store.Branch(context.Background(), testProjectID, "main")
	if err != nil {
		t.Fatal(err)
	}
	if len(branch.Roles) != 1 {
		t.Errorf("roles = %v, want the duplicate refused", branch.Roles)
	}
}

// A removal recorded here alone would leave the object in Postgres, so the compute is woken and
// told to drop it in the same action.
func TestDeletingADatabaseTellsTheComputeToDropIt(t *testing.T) {
	store := newStore(t)
	seedBranch(t, store)
	server, computes := liveServer(t, store)

	response := form(t, server, http.MethodDelete,
		"/ui/projects/"+testProjectID+"/branches/main/databases/appdb", "tester", "", nil)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body)
	}

	branch, err := store.Branch(context.Background(), testProjectID, "main")
	if err != nil {
		t.Fatal(err)
	}
	if len(branch.Databases) != 0 {
		t.Errorf("databases = %v, want the deleted one gone", branch.Databases)
	}

	specs := computes.specs()
	if len(specs) == 0 {
		t.Fatal("the compute was never told anything")
	}
	last := specs[len(specs)-1]
	if len(last.DeltaOperations) != 1 {
		t.Fatalf("delta operations = %v, want the one drop", last.DeltaOperations)
	}
	if last.DeltaOperations[0].Action != neon.DeleteDatabase || last.DeltaOperations[0].Name != "appdb" {
		t.Errorf("delta = %+v, want a delete_db for appdb", last.DeltaOperations[0])
	}
	// The catalog it is reconciled against must no longer name the database, or it would be
	// created again beside the instruction to drop it.
	for _, database := range last.Cluster.Databases {
		if database.Name == "appdb" {
			t.Error("the spec still lists the database it asks the compute to drop")
		}
	}
}

func TestDeletingARole(t *testing.T) {
	store := newStore(t)
	seedBranch(t, store)
	server, computes := liveServer(t, store)

	// A role some database is owned by cannot go: the branch it would leave behind is one the
	// registry refuses, and refusing here means nothing was dropped either.
	response := form(t, server, http.MethodDelete,
		"/ui/projects/"+testProjectID+"/branches/main/roles/app", "tester", "", nil)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400, body = %s", response.Code, response.Body)
	}
	for _, spec := range computes.specs() {
		if len(spec.DeltaOperations) > 0 {
			t.Fatal("a refused delete still asked the compute to drop something")
		}
	}

	if added := form(t, server, http.MethodPost, "/ui/projects/"+testProjectID+"/branches/main/roles",
		"tester", "", url.Values{"role": {"reporting"}}); added.Code != http.StatusOK {
		t.Fatalf("adding a role to delete = %d, body = %s", added.Code, added.Body)
	}
	response = form(t, server, http.MethodDelete,
		"/ui/projects/"+testProjectID+"/branches/main/roles/reporting", "tester", "", nil)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body)
	}

	branch, err := store.Branch(context.Background(), testProjectID, "main")
	if err != nil {
		t.Fatal(err)
	}
	if len(branch.Roles) != 1 || branch.Roles[0].Name != "app" {
		t.Errorf("roles = %v, want only the one that owns a database", branch.Roles)
	}
	specs := computes.specs()
	last := specs[len(specs)-1]
	if len(last.DeltaOperations) != 1 || last.DeltaOperations[0].Action != neon.DeleteRole {
		t.Errorf("delta operations = %+v, want one delete_role", last.DeltaOperations)
	}
}

// Each page swaps its own shape into its own section. An action answering with the other page's
// fragment lands a branch list inside the branch, or the reverse.
func TestAnActionAnswersInTheShapeOfThePageThatAskedForIt(t *testing.T) {
	store := newStore(t)
	seedBranch(t, store)
	server := identityServer(t, store)

	catalog := form(t, server, http.MethodPost, "/ui/projects/"+testProjectID+"/branches/main/roles",
		"tester", "", url.Values{"role": {"reporting"}}).Body.String()
	if !strings.Contains(catalog, `id="branch"`) || strings.Contains(catalog, `id="branches"`) {
		t.Error("a catalog change did not answer with the branch")
	}

	list := form(t, server, http.MethodPost, "/ui/projects/"+testProjectID+"/branches/main/start",
		"tester", "", nil).Body.String()
	if !strings.Contains(list, `id="branches"`) {
		t.Error("starting from the list did not answer with the list")
	}

	page := form(t, server, http.MethodPost, "/ui/projects/"+testProjectID+"/branches/main/stop",
		"tester", "", url.Values{"view": {"branch"}}).Body.String()
	if !strings.Contains(page, `id="branch"`) || strings.Contains(page, `id="branches"`) {
		t.Error("stopping from the branch page did not answer with the branch")
	}
}

func TestABranchThatIsNotThereSendsThePageBackToTheProject(t *testing.T) {
	store := newStore(t)
	server := identityServer(t, store)

	request := httptest.NewRequest(http.MethodGet, "/ui/projects/"+testProjectID+"/branches/absent", nil)
	request.Header.Set("Remote-User", "tester")
	request.Header.Set("HX-Request", "true")
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, request)
	if got := recorder.Header().Get("HX-Redirect"); got != "/ui/projects/"+testProjectID {
		t.Errorf("HX-Redirect = %q, want the project page", got)
	}
}

// The page is the catalog. Everything Postgres holds is shown and manageable, including what was
// made with SQL — except what belongs to Postgres and to compute_ctl, which is not a branch's.
func TestTheBranchPageShowsWhatPostgresHas(t *testing.T) {
	store := newStore(t)
	seedBranch(t, store)
	server, computes := liveServer(t, store)
	computes.catalog = neon.CatalogObjects{
		Roles: []neon.Role{{Name: "app"}, {Name: "analytics"}, {Name: "cloud_admin"}, {Name: "pg_monitor"}},
		Databases: []neon.Database{
			{Name: "appdb", Owner: "app"}, {Name: "reports", Owner: "analytics"},
			{Name: "postgres", Owner: "cloud_admin"}, {Name: "template1", Owner: "cloud_admin"},
		},
	}

	body := doAs(t, server, http.MethodGet, "/ui/projects/"+testProjectID+"/branches/main", "tester", "", "").Body.String()
	for _, want := range []string{"analytics", "reports", `/databases/reports"`, `/roles/analytics"`} {
		if !strings.Contains(body, want) {
			t.Errorf("the branch page does not offer %q", want)
		}
	}
	for _, unwanted := range []string{"cloud_admin", "pg_monitor", "template1", "/databases/postgres\""} {
		if strings.Contains(body, unwanted) {
			t.Errorf("the branch page shows %q, which is not the branch's to manage", unwanted)
		}
	}

	// What the compute holds is what gets recorded, so the proxy can authenticate a role this
	// service did not create.
	branch, err := store.Branch(context.Background(), testProjectID, "main")
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, role := range branch.Roles {
		names = append(names, role.Name)
	}
	if !slices.Equal(names, []string{"analytics", "app"}) {
		t.Errorf("recorded roles = %v", names)
	}
}

// A compute that stopped cleanly was read on the way out, so its names are known and a connection
// string can still be offered. One that was never read is not known at all, and saying nothing is
// the only honest answer.
func TestADownBranchShowsOnlyWhatWasSnapshotted(t *testing.T) {
	for _, tc := range []struct {
		name     string
		snapshot *registry.LastSeen
		want     []string
		unwanted []string
	}{
		{
			name:     "stopped cleanly",
			snapshot: &registry.LastSeen{At: time.Now().UTC(), Roles: []string{"app"}, Databases: []string{"appdb"}},
			want:     []string{"postgresql://app@", "not running"},
			unwanted: []string{"was not shut down cleanly"},
		},
		{
			name:     "never read",
			want:     []string{"was not shut down cleanly"},
			unwanted: []string{"postgresql://app@", "<h2>connect</h2>"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := newStore(t)
			branch := seedBranch(t, store)
			branch.LastSeen = tc.snapshot
			if err := store.Put(context.Background(), branch); err != nil {
				t.Fatal(err)
			}
			runtime := newFakeRuntime()
			seedCompute(t, runtime, false)
			server := newTestServer(t, newFakeStorcon(t), store, runtime, newFakeCompute(t))
			server.identity = IdentityOptions{UserHeader: "Remote-User", GroupsHeader: "Remote-Groups", Admin: "root"}

			body := doAs(t, server, http.MethodGet, "/ui/projects/"+testProjectID+"/branches/main", "tester", "", "").Body.String()
			for _, want := range tc.want {
				if !strings.Contains(body, want) {
					t.Errorf("the page does not say %q", want)
				}
			}
			for _, unwanted := range tc.unwanted {
				if strings.Contains(body, unwanted) {
					t.Errorf("the page says %q", unwanted)
				}
			}
		})
	}
}

// A drop the compute refuses is not undone here, because undoing it would be a guess. The compute
// still holds the database, so the read that renders the answer records it back.
func TestARefusedDropIsCorrectedByTheRead(t *testing.T) {
	store := newStore(t)
	seedBranch(t, store)
	server, computes := liveServer(t, store)
	computes.configureFail = true

	response := form(t, server, http.MethodDelete,
		"/ui/projects/"+testProjectID+"/branches/main/databases/appdb", "tester", "", nil)
	if response.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502, body = %s", response.Code, response.Body)
	}

	branch, err := store.Branch(context.Background(), testProjectID, "main")
	if err != nil {
		t.Fatal(err)
	}
	if len(branch.Databases) != 1 || branch.Databases[0].Name != "appdb" {
		t.Errorf("databases = %v, want what the compute still has", branch.Databases)
	}
}

// A branch nothing is known about cannot be changed against a guess about what it holds.
func TestAnUnknownBranchRefusesAChange(t *testing.T) {
	store := newStore(t)
	seedBranch(t, store)
	runtime := newFakeRuntime()
	seedCompute(t, runtime, false)
	server := newTestServer(t, newFakeStorcon(t), store, runtime, newFakeCompute(t))
	server.identity = IdentityOptions{UserHeader: "Remote-User", GroupsHeader: "Remote-Groups", Admin: "root"}

	response := form(t, server, http.MethodPost, "/ui/projects/"+testProjectID+"/branches/main/roles",
		"tester", "", url.Values{"role": {"reporting"}})
	if response.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409, body = %s", response.Code, response.Body)
	}
	if runtime.ensures != 0 {
		t.Error("a branch nothing is known about was started to take a change")
	}
}

// A page with nothing to ask still has to hand somebody the string that starts the branch, and the
// names it needs are the ones the compute had when it was stopped — including the ones nobody
// recorded here.
func TestStoppingABranchKeepsTheNamesItsConnectStringsNeed(t *testing.T) {
	store := newStore(t)
	seedBranch(t, store)
	computes := newFakeCompute(t)
	runtime := newFakeRuntime()
	seedCompute(t, runtime, true)
	server := newTestServer(t, newFakeStorcon(t), store, runtime, computes)
	server.identity = IdentityOptions{UserHeader: "Remote-User", GroupsHeader: "Remote-Groups", Admin: "root"}
	server.opts.EndpointSuffix = "pg.example.net"
	computes.catalog = neon.CatalogObjects{
		Roles:     []neon.Role{{Name: "app"}},
		Databases: []neon.Database{{Name: "appdb", Owner: "app"}, {Name: "analytics", Owner: "app"}},
	}

	if stopped := form(t, server, http.MethodPost, "/ui/projects/"+testProjectID+"/branches/main/stop",
		"tester", "", nil); stopped.Code != http.StatusOK {
		t.Fatalf("stop = %d, body = %s", stopped.Code, stopped.Body)
	}

	branch, err := store.Branch(context.Background(), testProjectID, "main")
	if err != nil {
		t.Fatal(err)
	}
	if branch.LastSeen == nil {
		t.Fatal("stopping the branch kept nothing")
	}
	if len(branch.LastSeen.Databases) != 2 {
		t.Errorf("databases = %v, want both, including the one made outside this service", branch.LastSeen.Databases)
	}

	body := doAs(t, server, http.MethodGet, "/ui/projects/"+testProjectID+"/branches/main", "tester", "", "").Body.String()
	if !strings.Contains(body, "analytics") {
		t.Error("the connect strings do not offer a database the branch actually has")
	}
	if !strings.Contains(body, "when the database was last running") {
		t.Error("the page does not say the names are from the last time the branch ran")
	}

	// The catalog itself is not shown: nothing can be asked, so nothing is claimed.
	if strings.Contains(body, "reset password") || strings.Contains(body, `aria-label="add a role"`) {
		t.Error("a branch that cannot be asked still offered its catalog for editing")
	}
}

func passwordServer(t *testing.T, store *registry.Store) *Server {
	t.Helper()
	key := make([]byte, secret.KeySize)
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}
	box, err := secret.New(key)
	if err != nil {
		t.Fatal(err)
	}
	server := identityServer(t, store)
	server.secrets = box
	server.opts.EndpointSuffix = "pg.example.net"
	return server
}

func TestAKeptPasswordCanBeShownAgain(t *testing.T) {
	store := newStore(t)
	server := passwordServer(t, store)

	created := form(t, server, http.MethodPost, "/ui/projects/"+testProjectID+"/branches", "tester", "",
		url.Values{"name": {"trunk"}, "role": {"app"}, "database": {"appdb"}, "pg_version": {"17"}})
	if created.Code != http.StatusOK {
		t.Fatalf("create = %d, body = %s", created.Code, created.Body)
	}
	password := passwordFrom(t, created.Body.String())

	// The registry keeps it sealed, never as the thing itself.
	branch, err := store.Branch(context.Background(), testProjectID, "trunk")
	if err != nil {
		t.Fatal(err)
	}
	if branch.Roles[0].Secret == "" {
		t.Fatal("nothing was kept")
	}
	if strings.Contains(branch.Roles[0].Secret, password) {
		t.Error("the password is stored as itself")
	}

	api := doAs(t, server, http.MethodGet,
		"/api/projects/"+testProjectID+"/branches/trunk/roles/app/password", "tester", "", "")
	if api.Code != http.StatusOK || !strings.Contains(api.Body.String(), password) {
		t.Errorf("api reveal = %d, body = %s", api.Code, api.Body)
	}
}

// Keeping no password is a configuration, not a failure, and it answers differently from a role
// whose password simply was never kept.
func TestRevealingRefusesWhenNoPasswordsAreKept(t *testing.T) {
	store := newStore(t)
	seedBranch(t, store)
	server := identityServer(t, store)

	response := doAs(t, server, http.MethodGet,
		"/api/projects/"+testProjectID+"/branches/main/roles/app/password", "tester", "", "")
	if response.Code != http.StatusPreconditionFailed {
		t.Errorf("status = %d, want 412", response.Code)
	}

	kept := passwordServer(t, store)
	response = doAs(t, kept, http.MethodGet,
		"/api/projects/"+testProjectID+"/branches/main/roles/app/password", "tester", "", "")
	if response.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404 for a role seeded with only a verifier", response.Code)
	}
}

// Roles are replaced wholesale on every change, so the one hazard is a change to one role quietly
// discarding what was kept for the others.
func TestChangingOneRoleKeepsTheOthersPasswords(t *testing.T) {
	store := newStore(t)
	server := passwordServer(t, store)

	created := form(t, server, http.MethodPost, "/ui/projects/"+testProjectID+"/branches", "tester", "",
		url.Values{"name": {"trunk"}, "role": {"app"}, "database": {"appdb"}, "pg_version": {"17"}})
	if created.Code != http.StatusOK {
		t.Fatalf("create = %d, body = %s", created.Code, created.Body)
	}
	password := passwordFrom(t, created.Body.String())

	for _, action := range []struct {
		name   string
		method string
		target string
		values url.Values
	}{
		{"adding a role", http.MethodPost, "/ui/projects/" + testProjectID + "/branches/trunk/roles",
			url.Values{"role": {"reporting"}}},
		{"resetting another role", http.MethodPost,
			"/ui/projects/" + testProjectID + "/branches/trunk/roles/reporting/password", nil},
		{"adding a database", http.MethodPost,
			"/ui/projects/" + testProjectID + "/branches/trunk/databases",
			url.Values{"database": {"other"}, "owner": {"app"}}},
	} {
		if response := form(t, server, action.method, action.target, "tester", "", action.values); response.Code != http.StatusOK {
			t.Fatalf("%s = %d, body = %s", action.name, response.Code, response.Body)
		}
		shown := doAs(t, server, http.MethodGet,
			"/api/projects/"+testProjectID+"/branches/trunk/roles/app/password", "tester", "", "")
		if !strings.Contains(shown.Body.String(), password) {
			t.Fatalf("%s discarded the password kept for a role beside it", action.name)
		}
	}
}

func passwordFrom(t *testing.T, body string) string {
	t.Helper()
	_, rest, ok := strings.Cut(body, "postgresql://app:")
	if !ok {
		t.Fatalf("no connection string carrying a password in %s", body)
	}
	password, _, ok := strings.Cut(rest, "@")
	if !ok {
		t.Fatal("a connection string with no host")
	}
	return password
}

// A verifier supplied alongside a password is the one that takes effect, so keeping the password
// beside it would hand back a string that does not authenticate.
func TestAPasswordIsNotKeptWhenAVerifierOverrulesIt(t *testing.T) {
	store := newStore(t)
	server := passwordServer(t, store)

	response := do(t, server, http.MethodPost, "/api/projects/"+testProjectID+"/branches", `{
		"name":"main",
		"roles":[{"name":"app","password":"hunter2","verifier":"SCRAM-SHA-256$4096:c2FsdA==$c3RvcmVk:c2VydmVy"}],
		"databases":[{"name":"appdb","owner":"app"}]}`)
	if response.Code != http.StatusCreated {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body)
	}
	branch, err := store.Branch(context.Background(), testProjectID, "main")
	if err != nil {
		t.Fatal(err)
	}
	if branch.Roles[0].Secret != "" {
		t.Error("a password was kept for a role whose verifier came from somewhere else")
	}
}

// The spec is compute_ctl's format, not ours, and a name it does not read is a drop that silently
// does not happen. Asserted against the bytes, because decoding with the types that encoded them
// would accept any spelling.
func TestADropIsSpelledTheWayComputeCtlReadsIt(t *testing.T) {
	store := newStore(t)
	seedBranch(t, store)
	computes := newFakeCompute(t)
	runtime := newFakeRuntime()
	seedCompute(t, runtime, true)
	server := newTestServer(t, newFakeStorcon(t), store, runtime, computes)
	server.identity = IdentityOptions{UserHeader: "Remote-User", GroupsHeader: "Remote-Groups", Admin: "root"}

	response := form(t, server, http.MethodDelete,
		"/ui/projects/"+testProjectID+"/branches/main/databases/appdb", "tester", "", nil)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body)
	}

	body := computes.lastBody()
	for _, want := range []string{`"delta_operations"`, `"action":"delete_db"`, `"name":"appdb"`} {
		if !strings.Contains(body, want) {
			t.Errorf("the spec does not carry %s: %s", want, body)
		}
	}
}

// Nothing is dropped unless somebody asked, so a spec rendered for any other reason must not carry
// the field at all rather than an empty one.
func TestAnOrdinarySpecCarriesNoDeltas(t *testing.T) {
	store := newStore(t)
	seedBranch(t, store)
	computes := newFakeCompute(t)
	runtime := newFakeRuntime()
	seedCompute(t, runtime, true)
	server := newTestServer(t, newFakeStorcon(t), store, runtime, computes)
	server.identity = IdentityOptions{UserHeader: "Remote-User", GroupsHeader: "Remote-Groups", Admin: "root"}

	if added := form(t, server, http.MethodPost, "/ui/projects/"+testProjectID+"/branches/main/roles",
		"tester", "", url.Values{"role": {"reporting"}}); added.Code != http.StatusOK {
		t.Fatalf("adding a role = %d, body = %s", added.Code, added.Body)
	}
	if body := computes.lastBody(); strings.Contains(body, "delta_operations") {
		t.Errorf("a spec that drops nothing carries the field: %s", body)
	}
}

// Nothing here reaches Postgres except through a compute, so a branch that is asleep is woken to
// take a change rather than the change being recorded against a compute that will never see it.
func TestAChangeStartsASleepingBranch(t *testing.T) {
	store := newStore(t)
	branch := seedBranch(t, store)
	branch.LastSeen = &registry.LastSeen{At: time.Now().UTC(), Roles: []string{"app"}, Databases: []string{"appdb"}}
	if err := store.Put(context.Background(), branch); err != nil {
		t.Fatal(err)
	}
	computes := newFakeCompute(t)
	primeCatalog(computes, []registry.Branch{*branch})
	runtime := newFakeRuntime()
	seedCompute(t, runtime, false)
	server := newTestServer(t, newFakeStorcon(t), store, runtime, computes)
	server.identity = IdentityOptions{UserHeader: "Remote-User", GroupsHeader: "Remote-Groups", Admin: "root"}

	response := form(t, server, http.MethodPost, "/ui/projects/"+testProjectID+"/branches/main/databases",
		"tester", "", url.Values{"database": {"reports"}, "owner": {"app"}})
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body)
	}
	if runtime.ensures == 0 {
		t.Error("the branch was not started to apply the change")
	}
	if len(computes.specs()) == 0 {
		t.Error("no compute was asked to apply the change")
	}
}

// A spec that fails may still have applied part of itself, so undoing the change blindly would
// record something Postgres does not have. What the compute reports is what gets recorded.
func TestARefusedChangeIsRecordedAsTheComputeHasIt(t *testing.T) {
	for _, tc := range []struct {
		name    string
		catalog neon.CatalogObjects
		method  string
		target  string
		values  url.Values
		roles   []string
		dbs     []string
	}{
		{
			name:    "nothing applied",
			catalog: neon.CatalogObjects{Roles: []neon.Role{{Name: "app"}}, Databases: []neon.Database{{Name: "appdb", Owner: "app"}}},
			method:  http.MethodPost,
			target:  "/roles",
			values:  url.Values{"role": {"reporting"}},
			roles:   []string{"app"},
			dbs:     []string{"appdb"},
		},
		{
			name: "the role was created before the failure",
			catalog: neon.CatalogObjects{
				Roles:     []neon.Role{{Name: "app"}, {Name: "reporting"}},
				Databases: []neon.Database{{Name: "appdb", Owner: "app"}},
			},
			method: http.MethodPost,
			target: "/roles",
			values: url.Values{"role": {"reporting"}},
			roles:  []string{"app", "reporting"},
			dbs:    []string{"appdb"},
		},
		{
			name:    "the drop happened before the failure",
			catalog: neon.CatalogObjects{Roles: []neon.Role{{Name: "app"}}},
			method:  http.MethodDelete,
			target:  "/databases/appdb",
			roles:   []string{"app"},
			dbs:     nil,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := newStore(t)
			seedBranch(t, store)
			computes := newFakeCompute(t)
			computes.catalog = tc.catalog
			computes.configureFail = true
			runtime := newFakeRuntime()
			seedCompute(t, runtime, true)
			server := newTestServer(t, newFakeStorcon(t), store, runtime, computes)
			server.identity = IdentityOptions{UserHeader: "Remote-User", GroupsHeader: "Remote-Groups", Admin: "root"}

			response := form(t, server, tc.method,
				"/ui/projects/"+testProjectID+"/branches/main"+tc.target, "tester", "", tc.values)
			if response.Code != http.StatusBadGateway {
				t.Fatalf("status = %d, want 502", response.Code)
			}

			after, err := store.Branch(context.Background(), testProjectID, "main")
			if err != nil {
				t.Fatal(err)
			}
			var roles, dbs []string
			for _, role := range after.Roles {
				roles = append(roles, role.Name)
			}
			for _, database := range after.Databases {
				dbs = append(dbs, database.Name)
			}
			if !slices.Equal(roles, tc.roles) {
				t.Errorf("roles = %v, want %v", roles, tc.roles)
			}
			if !slices.Equal(dbs, tc.dbs) {
				t.Errorf("databases = %v, want %v", dbs, tc.dbs)
			}
		})
	}
}

// A password change that did not land would leave the record holding a verifier Postgres does not
// have, and the proxy would then refuse a password that still works. The catalog carries the
// verifier, so the record takes it from there rather than from whichever version looks likelier.
func TestARefusedPasswordChangeTakesTheVerifierFromTheCompute(t *testing.T) {
	for _, live := range []string{"SCRAM-SHA-256$4096:c2FsdA==$c3RvcmVk:c2VydmVy", "SCRAM-SHA-256$4096:bmV3$bmV3:bmV3"} {
		t.Run(live, func(t *testing.T) {
			store := newStore(t)
			seedBranch(t, store)
			computes := newFakeCompute(t)
			computes.catalog = neon.CatalogObjects{
				Roles:     []neon.Role{{Name: "app", EncryptedPassword: &live}},
				Databases: []neon.Database{{Name: "appdb", Owner: "app"}},
			}
			computes.configureFail = true
			runtime := newFakeRuntime()
			seedCompute(t, runtime, true)
			server := newTestServer(t, newFakeStorcon(t), store, runtime, computes)
			server.identity = IdentityOptions{UserHeader: "Remote-User", GroupsHeader: "Remote-Groups", Admin: "root"}

			response := form(t, server, http.MethodPost,
				"/ui/projects/"+testProjectID+"/branches/main/roles/app/password", "tester", "", nil)
			if response.Code != http.StatusBadGateway {
				t.Fatalf("status = %d, want 502", response.Code)
			}

			after, err := store.Branch(context.Background(), testProjectID, "main")
			if err != nil {
				t.Fatal(err)
			}
			if after.Roles[0].Verifier != live {
				t.Errorf("verifier = %q, want the one the compute reports", after.Roles[0].Verifier)
			}
		})
	}
}

// A branch nothing has confirmed shows nothing and takes no changes, so creating one from the page
// starts it rather than leaving something to go and start.
func TestCreatingABranchStartsIt(t *testing.T) {
	store := newStore(t)
	computes := newFakeCompute(t)
	runtime := newFakeRuntime()
	server := newTestServer(t, newFakeStorcon(t), store, runtime, computes)
	server.identity = IdentityOptions{UserHeader: "Remote-User", GroupsHeader: "Remote-Groups", Admin: "root"}

	response := form(t, server, http.MethodPost, "/ui/projects/"+testProjectID+"/branches", "tester", "",
		url.Values{"name": {"trunk"}, "role": {"app"}, "database": {"appdb"}, "pg_version": {"17"}})
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body)
	}

	instances, err := runtime.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(instances) != 1 || !instances[0].Running() {
		t.Errorf("the new branch was not started: %v", instances)
	}
}

// A fragment URL is linkable, bookmarkable and refreshable. Answering one with a bare fragment
// hands somebody a page with no stylesheet and no navigation.
func TestAFragmentURLAnswersWithAPageWhenItIsNotHtmx(t *testing.T) {
	store := newStore(t)
	seedBranch(t, store)
	server := identityServer(t, store)

	for _, target := range []string{
		"/ui/projects",
		"/ui/projects/" + testProjectID + "/branches",
		"/ui/projects/" + testProjectID + "/branches/main/detail",
	} {
		response := doAs(t, server, http.MethodGet, target, "tester", "", "")
		if response.Code != http.StatusOK {
			t.Fatalf("GET %s = %d", target, response.Code)
		}
		if !strings.Contains(response.Body.String(), `href="/ui/assets/style.css"`) {
			t.Errorf("GET %s answered with a fragment rather than a page", target)
		}

		request := httptest.NewRequest(http.MethodGet, target, nil)
		request.Header.Set("Remote-User", "tester")
		request.Header.Set("HX-Request", "true")
		recorder := httptest.NewRecorder()
		server.Handler().ServeHTTP(recorder, request)
		if strings.Contains(recorder.Body.String(), `href="/ui/assets/style.css"`) {
			t.Errorf("GET %s answered htmx with a whole page", target)
		}
	}
}

// Starting a branch is what discards its snapshot, so between the button and a compute that can be
// read there is a window with neither. Reporting that as unknown tells somebody to start a branch
// that is already starting.
func TestAStartingBranchIsNotReportedAsUnknown(t *testing.T) {
	store := newStore(t)
	seedBranch(t, store)
	runtime := newFakeRuntime()
	runtime.add(kube.Instance{
		Binding:    kube.Binding{ID: testEndpointID, TenantID: mustTenant(t), TimelineID: mustTimeline(t)},
		ControlURL: "http://compute-" + testEndpointID + ".neon:3080",
		PgAddress:  "compute-" + testEndpointID + ".neon:55433",
		Replicas:   1,
	})
	server := newTestServer(t, newFakeStorcon(t), store, runtime, newFakeCompute(t))
	server.identity = IdentityOptions{UserHeader: "Remote-User", GroupsHeader: "Remote-Groups", Admin: "root"}

	body := doAs(t, server, http.MethodGet, "/ui/projects/"+testProjectID+"/branches/main", "tester", "", "").Body.String()
	if strings.Contains(body, "was not shut down cleanly") {
		t.Error("a branch that is coming up is reported as one nothing is known about")
	}
	if !strings.Contains(body, "The database is starting") {
		t.Error("the page does not say the branch is starting")
	}
}

// Stopping a branch takes its snapshot, so the answer to the stop is the first thing that can show
// a connection string built from one. Rendering it from the copy the request started with reports
// a branch nothing is known about, one poll before the snapshot turns up.
func TestStoppingABranchAnswersWithItsSnapshot(t *testing.T) {
	store := newStore(t)
	seedBranch(t, store)
	server, _ := liveServer(t, store)

	response := form(t, server, http.MethodPost,
		"/ui/projects/"+testProjectID+"/branches/main/stop", "tester", "", url.Values{"view": {"branch"}})
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body)
	}

	body := response.Body.String()
	if strings.Contains(body, "was not shut down cleanly") {
		t.Error("stopping a branch answered as though nothing were known about it")
	}
	if !strings.Contains(body, "postgresql://app@") {
		t.Error("the answer does not carry the connection string the snapshot was taken for")
	}
}

// The password rides in the connection string as a mask. Nothing renders it into the page: the
// string carries a placeholder and the browser fetches the real one only when somebody asks.
func TestTheConnectionStringMasksAPasswordThatCanBeShown(t *testing.T) {
	for _, keyed := range []bool{true, false} {
		name := "no password kept"
		if keyed {
			name = "password kept"
		}
		t.Run(name, func(t *testing.T) {
			store := newStore(t)
			branch := seedBranch(t, store)
			branch.Roles[0].Secret = "sealed"
			if err := store.Put(context.Background(), branch); err != nil {
				t.Fatal(err)
			}
			server, _ := liveServer(t, store)
			if keyed {
				key := make([]byte, secret.KeySize)
				if _, err := rand.Read(key); err != nil {
					t.Fatal(err)
				}
				box, err := secret.New(key)
				if err != nil {
					t.Fatal(err)
				}
				server.secrets = box
			}
			server.opts.EndpointSuffix = "pg.example.net"

			body := doAs(t, server, http.MethodGet, "/ui/projects/"+testProjectID+"/branches/main", "tester", "", "").Body.String()
			masked := "postgresql://app:***@ep-test-branch-aaaaaaaa.pg.example.net/appdb?sslmode=require"
			if got := strings.Contains(body, masked); got != keyed {
				t.Errorf("connection string masked = %v, want %v", got, keyed)
			}
			if got := strings.Contains(body, "data-reveal="); got != keyed {
				t.Errorf("string offers to reveal = %v, want %v", got, keyed)
			}
			if got := strings.Contains(body, "Passwords are shown once"); got == keyed {
				t.Errorf("page says passwords are shown once = %v, want %v", got, !keyed)
			}
			// The mask is a placeholder, not a credential, and must not arrive percent-encoded.
			if strings.Contains(body, "%2A") {
				t.Error("the mask was escaped as though it were a password")
			}
			if strings.Contains(body, "sealed") {
				t.Error("a stored password reached the page")
			}
		})
	}
}

// A parent is picked from the branches that exist. The list lives outside the section an action
// replaces, so it is swapped alongside it or it names branches that have since gone.
func TestTheParentPickerFollowsTheBranchList(t *testing.T) {
	store := newStore(t)
	seedBranch(t, store)
	server := identityServer(t, store)

	page := doAs(t, server, http.MethodGet, "/ui/projects/"+testProjectID, "tester", "", "").Body.String()
	if !strings.Contains(page, `<select id="pick-parent"`) {
		t.Error("the fork form does not offer the branches to pick from")
	}
	if !strings.Contains(page, "<option>main</option>") {
		t.Error("the parent picker does not list the branch that exists")
	}

	created := form(t, server, http.MethodPost, "/ui/projects/"+testProjectID+"/branches", "tester", "",
		url.Values{"name": {"trunk"}, "role": {"app"}, "database": {"appdb"}, "pg_version": {"17"}})
	if created.Code != http.StatusOK {
		t.Fatalf("create = %d, body = %s", created.Code, created.Body)
	}
	body := created.Body.String()
	if !strings.Contains(body, `id="pick-parent"`) || !strings.Contains(body, `hx-swap-oob="true"`) {
		t.Fatal("creating a branch did not refresh the parent picker")
	}
	if !strings.Contains(body, "<option>trunk</option>") {
		t.Error("the refreshed picker does not offer the branch that was just made")
	}
}

// Size comes from storage rather than from the compute, so it is a fact about the branch and is
// there whether or not one is running. Uptime is the compute's and goes with it.
func TestABranchReportsItsSizeWithoutACompute(t *testing.T) {
	store := newStore(t)
	seedBranch(t, store)
	storcon := newFakeStorcon(t)
	storcon.logicalSize = 42 * 1024 * 1024
	runtime := newFakeRuntime()
	server := newTestServer(t, storcon, store, runtime, newFakeCompute(t))
	server.identity = IdentityOptions{UserHeader: "Remote-User", GroupsHeader: "Remote-Groups", Admin: "root"}

	// No compute at all: the branch page cannot say what it holds, but it can say how much.
	page := doAs(t, server, http.MethodGet, "/ui/projects/"+testProjectID+"/branches/main", "tester", "", "").Body.String()
	if !strings.Contains(page, "42.0 MiB") {
		t.Error("a branch with no compute does not report its size")
	}
	if !strings.Contains(page, "<dt>uptime</dt>") {
		t.Error("the compute facts do not offer uptime")
	}

	// And the listing carries it, for the same reason.
	list := doAs(t, server, http.MethodGet, "/ui/projects/"+testProjectID, "tester", "", "").Body.String()
	if !strings.Contains(list, "42.0 MiB") {
		t.Error("the branch list does not report size")
	}

	api := doAs(t, server, http.MethodGet, "/api/projects/"+testProjectID+"/branches/main", "tester", "", "")
	if !strings.Contains(api.Body.String(), `"size_bytes":44040192`) {
		t.Errorf("the api does not carry the size: %s", api.Body)
	}
}

// A size the pageserver is still working out is reported as approximate rather than as fact.
func TestAnApproximateSizeIsMarked(t *testing.T) {
	store := newStore(t)
	seedBranch(t, store)
	storcon := newFakeStorcon(t)
	storcon.logicalSize = 1536
	storcon.sizeAccurate = false
	server := newTestServer(t, storcon, store, newFakeRuntime(), newFakeCompute(t))
	server.identity = IdentityOptions{UserHeader: "Remote-User", GroupsHeader: "Remote-Groups", Admin: "root"}

	page := doAs(t, server, http.MethodGet, "/ui/projects/"+testProjectID+"/branches/main", "tester", "", "").Body.String()
	if !strings.Contains(page, "about 1.5 KiB") {
		t.Error("an approximate size is reported as though it were exact")
	}
}

// The roles table masks a password the same way the connection string does. Neither renders one
// into the page: the mask is all the browser holds until somebody asks for the real thing.
func TestTheRolesTableMasksThePassword(t *testing.T) {
	store := newStore(t)
	branch := seedBranch(t, store)
	branch.Roles[0].Secret = "sealed"
	if err := store.Put(context.Background(), branch); err != nil {
		t.Fatal(err)
	}
	server, _ := liveServer(t, store)
	key := make([]byte, secret.KeySize)
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}
	box, err := secret.New(key)
	if err != nil {
		t.Fatal(err)
	}
	server.secrets = box

	body := doAs(t, server, http.MethodGet, "/ui/projects/"+testProjectID+"/branches/main", "tester", "", "").Body.String()
	if !strings.Contains(body, `<code class="mask"`) {
		t.Error("the roles table does not mask the password")
	}
	if !strings.Contains(body, `data-role="app"`) {
		t.Error("the mask does not say which role it would reveal")
	}
	if strings.Contains(body, "sealed") {
		t.Error("a stored password reached the page")
	}

	// With no key there is nothing to unmask, so the column offers only the reset.
	server.secrets = nil
	bare := doAs(t, server, http.MethodGet, "/ui/projects/"+testProjectID+"/branches/main", "tester", "", "").Body.String()
	if strings.Contains(bare, `<code class="mask"`) {
		t.Error("a deployment that keeps no passwords still offered to show one")
	}
	if !strings.Contains(bare, ">reset</button>") {
		t.Error("the reset went missing with the mask")
	}
}
