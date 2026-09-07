package controlplane

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"crypto/rand"

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
	computes := newFakeCompute(t)
	runtime := newFakeRuntime()
	server := newTestServer(t, newFakeStorcon(t), store, runtime, computes)
	server.identity = IdentityOptions{UserHeader: "Remote-User", GroupsHeader: "Remote-Groups", Admin: "root"}

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
	computes := newFakeCompute(t)
	server := newTestServer(t, newFakeStorcon(t), store, newFakeRuntime(), computes)
	server.identity = IdentityOptions{UserHeader: "Remote-User", GroupsHeader: "Remote-Groups", Admin: "root"}

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

// The page reads from two places that can disagree, and a page that only ever showed one of them
// would be confidently wrong about what a person can connect to.
func TestTheBranchPageShowsWhatPostgresHasAndWhatIsOnlyRecorded(t *testing.T) {
	store := newStore(t)
	seedBranch(t, store)
	computes := newFakeCompute(t)
	runtime := newFakeRuntime()
	seedCompute(t, runtime, true)
	server := newTestServer(t, newFakeStorcon(t), store, runtime, computes)
	server.identity = IdentityOptions{UserHeader: "Remote-User", GroupsHeader: "Remote-Groups", Admin: "root"}

	// app and appdb are recorded and live; analytics was made in SQL; reporting is recorded but
	// has not reached the compute.
	computes.catalog = neon.CatalogObjects{
		Roles:     []neon.Role{{Name: "app"}},
		Databases: []neon.Database{{Name: "appdb", Owner: "app"}, {Name: "analytics", Owner: "app"}},
	}
	if added := form(t, server, http.MethodPost, "/ui/projects/"+testProjectID+"/branches/main/roles",
		"tester", "", url.Values{"role": {"reporting"}}); added.Code != http.StatusOK {
		t.Fatalf("adding a role = %d, body = %s", added.Code, added.Body)
	}

	body := doAs(t, server, http.MethodGet, "/ui/projects/"+testProjectID+"/branches/main", "tester", "", "").Body.String()
	for _, want := range []string{"analytics", "not made here", "reporting", "pending", "live"} {
		if !strings.Contains(body, want) {
			t.Errorf("the branch page does not say %q", want)
		}
	}

	// Nothing made outside this service is offered an action, because dropping what it did not
	// create is not this page's to do.
	if strings.Contains(body, `/databases/analytics"`) {
		t.Error("a database this service did not create was given a delete button")
	}
}

// Not knowing is its own answer: a suspended compute cannot be asked, and saying "pending" would
// claim something the page has no way to know.
func TestASuspendedBranchIsNotReportedAsPending(t *testing.T) {
	store := newStore(t)
	seedBranch(t, store)
	server := identityServer(t, store)

	body := doAs(t, server, http.MethodGet, "/ui/projects/"+testProjectID+"/branches/main", "tester", "", "").Body.String()
	if strings.Contains(body, "pending") {
		t.Error("a branch whose compute was never asked reports its roles as pending")
	}
	if !strings.Contains(body, "not running") {
		t.Error("the page does not say why it is showing only what is recorded")
	}
}

// A drop the compute refuses must leave the record alone: a name removed here and still there is
// the divergence the page exists to not have.
func TestARefusedDropChangesNothing(t *testing.T) {
	store := newStore(t)
	seedBranch(t, store)
	computes := newFakeCompute(t)
	computes.configureFail = true
	runtime := newFakeRuntime()
	seedCompute(t, runtime, true)
	server := newTestServer(t, newFakeStorcon(t), store, runtime, computes)
	server.identity = IdentityOptions{UserHeader: "Remote-User", GroupsHeader: "Remote-Groups", Admin: "root"}

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
		t.Errorf("databases = %v, want the refused drop rolled back", branch.Databases)
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
	if !strings.Contains(body, "when it was stopped") {
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

	shown := doAs(t, server, http.MethodGet,
		"/ui/projects/"+testProjectID+"/branches/trunk/roles/app/password", "tester", "", "")
	if shown.Code != http.StatusOK {
		t.Fatalf("reveal = %d, body = %s", shown.Code, shown.Body)
	}
	if !strings.Contains(shown.Body.String(), password) {
		t.Error("the password that was kept was not the one shown back")
	}

	// And the same answer by hand, which is the only claim the page makes about the API.
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
			"/ui/projects/"+testProjectID+"/branches/trunk/roles/app/password", "tester", "", "")
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
