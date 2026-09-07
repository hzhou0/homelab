package controlplane

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/hzhou0/homelab/neon/ctl/internal/controlplane/ui"
	"github.com/hzhou0/homelab/neon/ctl/internal/kube"
	"github.com/hzhou0/homelab/neon/ctl/internal/neon"
	"github.com/hzhou0/homelab/neon/ctl/internal/registry"
)

// page carries everything any template may ask for. One struct rather than one per view, because
// a fragment is rendered from the same handler as the page it belongs to.
type page struct {
	Title      string
	Identity   *Identity
	PgVersions []int

	Projects []projectView
	Project  *projectView
	Branches []branchRow
	Branch   *branchRow
	MayGrant bool

	// Notice is the outcome of the action just taken. It renders outside every polled section, so
	// a password shown once is not swept away by the next refresh.
	Notice notice

	// Error is the section's own failure — the list could not be read — and replaces it.
	Error      string
	GrantError string

	Message string
}

type notice struct {
	Error  string
	Secret *credential
}

// credential is a password on its way to somebody, carried apart from the branch so that a page
// which merely renders a branch cannot print one by accident.
type credential struct {
	Branch string
	Role   string
	URI    string

	// Whether it can be asked for again. When it cannot, saying so is the difference between a
	// person copying it now and discovering later that nothing can print it.
	Kept bool
}

// branchRow is the branch as the page needs it: the API's view plus the one thing a person came
// for, which is the line they can paste into a client.
type branchRow struct {
	branchView
	Host string

	Connect connectOptions
	Catalog catalogState

	// How long this compute has been up. What the branch holds is a branch fact and rides on the
	// view, so it is there for a listing too.
	Uptime string

	// Set while the compute is coming up, when there is nothing to say yet but there will be.
	Starting bool

	// Set when the branch has neither a compute to ask nor a snapshot to fall back on, so nothing
	// about it can be stated — not even a connection string, whose names may since have gone.
	Unknown bool

	// Whether a password can be asked for a second time here at all.
	Passwords bool
}

// connectOptions is what the connect strings can be built from. The names come from the compute
// while it is up and from the snapshot taken as it stopped while it is down, because a string that
// names a database is what wakes it.
type connectOptions struct {
	Roles     []string
	Databases []string
	URI       string

	// Set when the names are from a compute that has since stopped, which is a thing to say rather
	// than to hide: one of them may have been dropped since.
	Since *time.Time
}

// catalogState is what the compute has. Postgres is the only thing that knows what exists, so it
// is read on every render and nothing is shown that was not just confirmed there.
type catalogState struct {
	// False when the compute could not be asked, which is the difference between knowing a name is
	// absent and having no way to find out.
	Known bool

	Roles     []catalogEntry
	Databases []catalogEntry
}

type catalogEntry struct {
	Name  string
	Owner string

	verifier string
}

// Postgres reserves the pg_ prefix for its own roles and refuses to create anything using it; the
// other two are made by compute_ctl for itself and are not a branch's to manage.
func reservedRole(name string) bool {
	return strings.HasPrefix(name, "pg_") || name == "cloud_admin" || name == "neon_superuser"
}

// compute_ctl's own connection string names postgres, so offering a way to drop it would be a way
// to break the compute.
func reservedDatabase(name string) bool {
	return name == "postgres" || name == "template0" || name == "template1"
}

func (s *Server) readCatalog(ctx context.Context, branch *registry.Branch, instance *kube.Instance) catalogState {
	if instance == nil || !instance.Running() {
		return catalogState{}
	}
	live, err := s.liveCatalog(ctx, instance)
	if err != nil {
		s.log.Error("reading a compute catalog", "branch", branch.Name, "error", err)
		return catalogState{}
	}

	state := catalogState{Known: true}
	for _, role := range live.Roles {
		if reservedRole(role.Name) {
			continue
		}
		entry := catalogEntry{Name: role.Name}
		if role.EncryptedPassword != nil {
			entry.verifier = *role.EncryptedPassword
		}
		state.Roles = append(state.Roles, entry)
	}
	for _, database := range live.Databases {
		if !reservedDatabase(database.Name) {
			state.Databases = append(state.Databases, catalogEntry{Name: database.Name, Owner: database.Owner})
		}
	}
	// pg_authid and pg_database answer in whatever order they please, and a table that reorders
	// itself under the poll is unreadable.
	byName := func(a, b catalogEntry) int { return strings.Compare(a.Name, b.Name) }
	slices.SortFunc(state.Roles, byName)
	slices.SortFunc(state.Databases, byName)

	s.adopt(ctx, branch, state)
	return state
}

// adopt records what the compute has. A role made with SQL authenticates through the proxy like any
// other once it is here, and it has to be here rather than read live: the proxy resolves a verifier
// before it wakes a compute, so there is a moment when the registry is the only place one exists.
// A sealed password survives only for a name whose verifier has not moved.
func (s *Server) adopt(ctx context.Context, branch *registry.Branch, state catalogState) {
	sealed := map[registry.Role]string{}
	for _, role := range branch.Roles {
		if role.Secret != "" {
			sealed[registry.Role{Name: role.Name, Verifier: role.Verifier}] = role.Secret
		}
	}

	updated := *branch
	updated.Roles = nil
	updated.Databases = nil
	for _, entry := range state.Roles {
		role := registry.Role{Name: entry.Name, Verifier: entry.verifier}
		role.Secret = sealed[role]
		updated.Roles = append(updated.Roles, role)
	}
	for _, entry := range state.Databases {
		updated.Databases = append(updated.Databases, registry.Database{Name: entry.Name, Owner: entry.Owner})
	}

	if slices.Equal(updated.Roles, branch.Roles) && slices.Equal(updated.Databases, branch.Databases) {
		return
	}
	if err := updated.Validate(); err != nil {
		s.log.Warn("cannot record what the compute has", "branch", branch.Name, "error", err)
		return
	}
	if err := s.registry.Put(ctx, &updated); err != nil {
		s.log.Error("recording what the compute has", "branch", branch.Name, "error", err)
		return
	}
	*branch = updated
}

func (s *Server) liveCatalog(ctx context.Context, instance *kube.Instance) (*neon.CatalogObjects, error) {
	client, err := s.computeClient(instance)
	if err != nil {
		return nil, err
	}
	return client.Catalog(ctx)
}

func (s *Server) branchRow(ctx context.Context, branch *registry.Branch) branchRow {
	return branchRow{branchView: s.view(ctx, branch), Host: s.endpointHost(branch)}
}

// What exists is the compute's to say; what it had when it stopped is the next best thing; what is
// recorded here is all there is for a branch that has never run.
func (s *Server) connectOptions(branch *registry.Branch, host string, catalog catalogState, masked bool) connectOptions {
	options := connectOptions{}
	switch {
	case catalog.Known:
		for _, entry := range catalog.Roles {
			options.Roles = append(options.Roles, entry.Name)
		}
		for _, entry := range catalog.Databases {
			options.Databases = append(options.Databases, entry.Name)
		}
	case branch.LastSeen != nil:
		options.Roles = branch.LastSeen.Roles
		options.Databases = branch.LastSeen.Databases
		options.Since = &branch.LastSeen.At
	}

	if len(options.Databases) == 0 || len(options.Roles) == 0 {
		return options
	}
	// The owner is the role that can do anything in a database, so it is the pairing worth
	// offering first; a database nothing here owns falls back to naming a role that exists.
	role := options.Roles[0]
	for _, database := range branch.Databases {
		if database.Name == options.Databases[0] && slices.Contains(options.Roles, database.Owner) {
			role = database.Owner
			break
		}
	}
	if masked {
		options.URI = maskedConnectionURI(host, role, options.Databases[0])
	} else {
		options.URI = connectionURI(host, role, options.Databases[0], "")
	}
	return options
}

// Coarse on purpose: the difference between 3h11m and 3h12m of uptime is not worth reading.
func since(start time.Time) string {
	elapsed := time.Since(start)
	switch {
	case elapsed < time.Minute:
		return fmt.Sprintf("%ds", int(elapsed.Seconds()))
	case elapsed < time.Hour:
		return fmt.Sprintf("%dm", int(elapsed.Minutes()))
	case elapsed < 24*time.Hour:
		return fmt.Sprintf("%dh %dm", int(elapsed.Hours()), int(elapsed.Minutes())%60)
	}
	return fmt.Sprintf("%dd %dh", int(elapsed.Hours())/24, int(elapsed.Hours())%24)
}

func (s *Server) endpointHost(branch *registry.Branch) string {
	if s.opts.EndpointSuffix == "" {
		return branch.EndpointID
	}
	return branch.EndpointID + "." + s.opts.EndpointSuffix
}

// passwordMask stands in the connection string for a password the browser has not asked for yet.
const passwordMask = "***"

// A mask is a placeholder rather than a credential, so it is spliced in rather than encoded:
// url.UserPassword renders *** as %2A%2A%2A, which is what somebody copying the line would get.
func maskedConnectionURI(host, role, database string) string {
	plain := connectionURI(host, role, database, "")
	user := url.User(role).String()
	return strings.Replace(plain, user+"@", user+":"+passwordMask+"@", 1)
}

// The proxy takes the endpoint out of the SNI name and demands TLS, so both the host and the mode
// are part of what a client has to be told.
func connectionURI(host, role, database, password string) string {
	uri := url.URL{Scheme: "postgresql", Host: host, RawQuery: "sslmode=require"}
	if password == "" {
		uri.User = url.User(role)
	} else {
		uri.User = url.UserPassword(role, password)
	}
	if database != "" {
		uri.Path = "/" + database
	}
	return uri.String()
}

// The browser selects the first option, and an unasked-for create takes the newest version, so
// the form has to offer them in that order to agree with itself.
func newestFirst(versions []int) []int {
	ordered := slices.Clone(versions)
	slices.Reverse(ordered)
	return ordered
}

// seal is what turns a password into something that can be shown again. Without a key the password
// is dropped here, which is the whole of the feature being off.
func (s *Server) seal(role, password string) (string, error) {
	if s.secrets == nil {
		return "", nil
	}
	return s.secrets.Seal(password, role)
}

// reveal answers only for a role whose password was kept: one set before a key was configured, or
// supplied as a verifier, has nothing to open.
func (s *Server) reveal(role *registry.Role) (string, bool) {
	if s.secrets == nil || role.Secret == "" {
		return "", false
	}
	password, err := s.secrets.Open(role.Secret, role.Name)
	if err != nil {
		s.log.Error("opening a stored password", "role", role.Name, "error", err)
		return "", false
	}
	return password, true
}

func newPassword() (string, error) {
	raw := make([]byte, 18)
	if _, err := rand.Read(raw); err != nil {
		return "", fmt.Errorf("generating a password: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(raw), nil
}

// A template is rendered whole before anything is written, so a failure mid-render cannot leave a
// half-written page that htmx would swap in as if it were the answer.
func (s *Server) render(w http.ResponseWriter, status int, name string, data *page) {
	var body bytes.Buffer
	if err := ui.Render(&body, name, data); err != nil {
		s.log.Error("rendering page", "template", name, "error", err)
		http.Error(w, "cannot render page", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(status)
	_, _ = body.WriteTo(w)
}

// A page cannot answer with the API's JSON: htmx would swap it into the document as text. Every
// refusal here means the caller can no longer act on what they are looking at, so htmx is sent back.
func (s *Server) uiRefusal(w http.ResponseWriter, r *http.Request) func(int, string) {
	return s.uiRefusalTo(w, r, "/")
}

// The location is the nearest page that still exists, since what the caller was looking at does not.
func (s *Server) uiRefusalTo(w http.ResponseWriter, r *http.Request, location string) func(int, string) {
	return func(status int, message string) {
		if r.Header.Get("HX-Request") == "true" {
			w.Header().Set("HX-Redirect", location)
			w.WriteHeader(status)
			return
		}
		s.render(w, status, "error-page", &page{Title: "neon", Message: message})
	}
}

func (s *Server) uiCaller(w http.ResponseWriter, r *http.Request) *Identity {
	return s.identityOr(r, s.uiRefusal(w, r))
}

func (s *Server) uiScope(w http.ResponseWriter, r *http.Request) (*Identity, *registry.Project) {
	refuse := s.uiRefusal(w, r)
	identity := s.identityOr(r, refuse)
	if identity == nil {
		return nil, nil
	}
	project := s.projectOr(r, identity, refuse)
	if project == nil {
		return nil, nil
	}
	return identity, project
}

// A fragment is only a fragment to htmx. Reached any other way — a bookmark, a refresh, a link
// somebody was sent — it has to arrive as a page, or it renders with no stylesheet and no way out.
func shell(r *http.Request, fragment, page string) string {
	if r.Header.Get("HX-Request") == "" {
		return page
	}
	return fragment
}

func (s *Server) handleUIProjects(w http.ResponseWriter, r *http.Request) {
	identity := s.uiCaller(w, r)
	if identity == nil {
		return
	}
	s.renderProjects(w, r, identity, "projects-page", http.StatusOK, notice{})
}

func (s *Server) handleUIProjectsFragment(w http.ResponseWriter, r *http.Request) {
	identity := s.uiCaller(w, r)
	if identity == nil {
		return
	}
	s.renderProjects(w, r, identity, shell(r, "projects", "projects-page"), http.StatusOK, notice{})
}

func (s *Server) renderProjects(w http.ResponseWriter, r *http.Request, identity *Identity, template string, status int, note notice) {
	data := &page{Title: "neon", Identity: identity, Notice: note}

	projects, err := s.registry.Projects(r.Context())
	if err != nil {
		s.log.Error("listing projects", "error", err)
		data.Error = "registry unavailable"
		s.render(w, http.StatusServiceUnavailable, template, data)
		return
	}
	for i := range projects {
		if identity.permits(&projects[i]) {
			data.Projects = append(data.Projects, s.projectView(r, &projects[i]))
		}
	}
	s.render(w, status, template, data)
}

func (s *Server) handleUICreateProject(w http.ResponseWriter, r *http.Request) {
	identity := s.uiCaller(w, r)
	if identity == nil {
		return
	}
	refuse := func(status int, message string) {
		s.renderProjects(w, r, identity, "projects-update", status, notice{Error: message})
	}

	groups := splitGroups(r.PostFormValue("groups"))
	if !identity.mayGrant(groups) {
		refuse(http.StatusForbidden, "you may only grant groups you belong to")
		return
	}

	tenant, err := neon.NewTenantID()
	if err != nil {
		s.log.Error("minting a tenant", "error", err)
		refuse(http.StatusInternalServerError, "cannot mint a tenant")
		return
	}
	project, err := registry.NewProject(r.PostFormValue("name"), identity.User, groups, tenant)
	if err != nil {
		refuse(http.StatusBadRequest, err.Error())
		return
	}
	if err := s.storcon.CreateTenant(r.Context(), tenant); err != nil {
		s.log.Error("creating tenant", "project", project.Name, "error", err)
		refuse(http.StatusBadGateway, "cannot create tenant")
		return
	}
	if err := s.registry.CreateProject(r.Context(), project); err != nil {
		if cleanup := s.storcon.DeleteTenant(r.Context(), tenant); cleanup != nil {
			s.log.Error("deleting the tenant of a failed project", "tenant", tenant, "error", cleanup)
		}
		if errors.Is(err, registry.ErrNameTaken) {
			refuse(http.StatusConflict, "you already have a project with that name")
			return
		}
		s.log.Error("recording project", "project", project.Name, "error", err)
		refuse(http.StatusServiceUnavailable, "registry unavailable")
		return
	}
	s.renderProjects(w, r, identity, "projects-update", http.StatusOK, notice{})
}

func (s *Server) handleUIDeleteProject(w http.ResponseWriter, r *http.Request) {
	identity, project := s.uiScope(w, r)
	if project == nil {
		return
	}
	refuse := func(status int, message string) {
		s.renderProjects(w, r, identity, "projects-update", status, notice{Error: message})
	}

	branches, err := s.registry.Branches(r.Context(), project.ID)
	if err != nil {
		s.log.Error("listing branches", "project", project.ID, "error", err)
		refuse(http.StatusServiceUnavailable, "registry unavailable")
		return
	}
	if len(branches) > 0 {
		refuse(http.StatusConflict, "delete the project's branches first")
		return
	}
	if err := s.registry.DeleteProject(r.Context(), project.ID); err != nil {
		s.log.Error("removing project", "project", project.ID, "error", err)
		refuse(http.StatusServiceUnavailable, "registry unavailable")
		return
	}
	if err := s.storcon.DeleteTenant(r.Context(), project.TenantID); err != nil {
		s.log.Error("deleting tenant", "project", project.ID, "tenant", project.TenantID, "error", err)
	}
	s.renderProjects(w, r, identity, "projects-update", http.StatusOK, notice{})
}

func (s *Server) handleUIProject(w http.ResponseWriter, r *http.Request) {
	identity, project := s.uiScope(w, r)
	if project == nil {
		return
	}
	s.renderBranches(w, r, identity, project, "project-page", http.StatusOK, notice{})
}

func (s *Server) handleUIBranches(w http.ResponseWriter, r *http.Request) {
	identity, project := s.uiScope(w, r)
	if project == nil {
		return
	}
	s.renderBranches(w, r, identity, project, shell(r, "branches", "project-page"), http.StatusOK, notice{})
}

func (s *Server) renderBranches(w http.ResponseWriter, r *http.Request, identity *Identity, project *registry.Project, template string, status int, note notice) {
	view := s.projectView(r, project)
	data := &page{
		Title:      project.Name,
		Identity:   identity,
		PgVersions: newestFirst(s.computes.PgVersions()),
		Project:    &view,
		MayGrant:   identity.Admin || identity.User == project.Owner,
		Notice:     note,
	}

	branches, err := s.registry.Branches(r.Context(), project.ID)
	if err != nil {
		s.log.Error("listing branches", "project", project.ID, "error", err)
		data.Error = "registry unavailable"
		s.render(w, http.StatusServiceUnavailable, template, data)
		return
	}
	for i := range branches {
		data.Branches = append(data.Branches, s.branchRow(r.Context(), &branches[i]))
	}
	s.render(w, status, template, data)
}

func (s *Server) handleUIBranchPage(w http.ResponseWriter, r *http.Request) {
	identity, project, branch := s.uiBranch(w, r)
	if branch == nil {
		return
	}
	s.renderBranch(w, r, identity, project, branch, "branch-page", http.StatusOK, notice{})
}

func (s *Server) handleUIBranchFragment(w http.ResponseWriter, r *http.Request) {
	identity, project, branch := s.uiBranch(w, r)
	if branch == nil {
		return
	}
	s.renderBranch(w, r, identity, project, branch, shell(r, "branch", "branch-page"), http.StatusOK, notice{})
}

func (s *Server) renderBranch(w http.ResponseWriter, r *http.Request, identity *Identity, project *registry.Project, branch *registry.Branch, template string, status int, note notice) {
	view := s.projectView(r, project)
	row := s.branchRow(r.Context(), branch)
	instance, err := s.computes.Get(r.Context(), branch.EndpointID)
	if err != nil {
		instance = nil
	}
	row.Catalog = s.readCatalog(r.Context(), branch, instance)
	row.Passwords = s.secrets != nil
	row.Connect = s.connectOptions(branch, row.Host, row.Catalog, row.Passwords)
	if started := row.Compute.StartedAt; started != nil {
		row.Uptime = since(*started)
	}
	// A compute on its way up has no catalog to read and no snapshot either, because starting one
	// is what discards it. That is a named state rather than an absence of one.
	row.Starting = instance != nil && instance.Replicas > 0 && !instance.Ready
	row.Unknown = !row.Catalog.Known && !row.Starting && branch.LastSeen == nil
	s.render(w, status, template, &page{
		Title:    branch.Name,
		Identity: identity,
		Project:  &view,
		Branch:   &row,
		Notice:   note,
	})
}

// Only the grant section is re-rendered, so a refusal lands beside the form that caused it rather
// than replacing the branch list with an error about something else.
func (s *Server) handleUIGrant(w http.ResponseWriter, r *http.Request) {
	identity, project := s.uiScope(w, r)
	if project == nil {
		return
	}
	view := s.projectView(r, project)
	data := &page{
		Title:    project.Name,
		Identity: identity,
		Project:  &view,
		MayGrant: identity.Admin || identity.User == project.Owner,
	}
	refuse := func(status int, message string) {
		data.GrantError = message
		s.render(w, status, "grant", data)
	}

	if !data.MayGrant {
		refuse(http.StatusForbidden, "only the owner may change who else may reach this project")
		return
	}
	groups := splitGroups(r.PostFormValue("groups"))
	if !identity.mayGrant(groups) {
		refuse(http.StatusForbidden, "you may only grant groups you belong to")
		return
	}

	project.Groups = groups
	if err := s.registry.PutProject(r.Context(), project); err != nil {
		s.log.Error("recording project", "project", project.ID, "error", err)
		refuse(http.StatusServiceUnavailable, "registry unavailable")
		return
	}
	view.Groups = groups
	s.render(w, http.StatusOK, "grant", data)
}

// A root branch is created with the role that owns it, since one with no role is unreachable. A
// fork is not: naming a role here would replace the catalog it inherits.
func (s *Server) handleUICreateBranch(w http.ResponseWriter, r *http.Request) {
	identity, project := s.uiScope(w, r)
	if project == nil {
		return
	}
	refuse := func(status int, message string) {
		s.renderBranches(w, r, identity, project, "branches-update", status, notice{Error: message})
	}

	// Always started: a branch nothing has confirmed shows nothing and takes no changes, so
	// creating one and leaving it down would only produce something to go and start.
	request := createBranchRequest{
		Name:   strings.TrimSpace(r.PostFormValue("name")),
		Parent: strings.TrimSpace(r.PostFormValue("parent")),
		Start:  true,
	}
	if version, err := strconv.Atoi(r.PostFormValue("pg_version")); err == nil {
		request.PgVersion = version
	}

	role := strings.TrimSpace(r.PostFormValue("role"))
	database := strings.TrimSpace(r.PostFormValue("database"))
	password := r.PostFormValue("password")
	if request.Parent == "" && role != "" {
		if password == "" {
			generated, err := newPassword()
			if err != nil {
				s.log.Error("generating a password", "branch", request.Name, "error", err)
				refuse(http.StatusInternalServerError, "cannot generate a password")
				return
			}
			password = generated
		}
		request.Roles = []roleRequest{{Name: role, Password: password}}
		if database != "" {
			request.Databases = []registry.Database{{Name: database, Owner: role}}
		}
	}

	branch, err := s.createBranch(r.Context(), project, request)
	if err != nil {
		s.log.Error("creating branch", "branch", request.Name, "error", err)
		refuse(statusOf(err), err.Error())
		return
	}

	note := notice{}
	if len(request.Roles) > 0 {
		note.Secret = &credential{
			Branch: branch.Name,
			Role:   role,
			URI:    connectionURI(s.endpointHost(branch), role, database, password),
			Kept:   s.secrets != nil,
		}
	}
	if _, err := s.ensureRunning(r.Context(), branch); err != nil {
		s.log.Error("starting new branch", "branch", branch.Name, "error", err)
		note.Error = "the branch exists but its compute did not start: " + err.Error()
	}
	s.renderBranches(w, r, identity, project, "branches-update", http.StatusOK, note)
}

// Starting and stopping are offered on the branch's page and in the list of them, and each wants
// its own shape of answer back. Everything else here belongs to one page and answers in its shape.
func (s *Server) answerBranchAction(w http.ResponseWriter, r *http.Request, identity *Identity, project *registry.Project, branch *registry.Branch, status int, note notice) {
	if r.FormValue("view") == "branch" {
		s.renderBranch(w, r, identity, project, branch, "branch-update", status, note)
		return
	}
	s.renderBranches(w, r, identity, project, "branches-update", status, note)
}

// uiBranch resolves what every action below needs: who is asking, which project, which branch.
func (s *Server) uiBranch(w http.ResponseWriter, r *http.Request) (*Identity, *registry.Project, *registry.Branch) {
	identity, project := s.uiScope(w, r)
	if project == nil {
		return nil, nil, nil
	}
	branch, err := s.registry.Branch(r.Context(), project.ID, r.PathValue("name"))
	if err != nil {
		s.uiRefusalTo(w, r, "/ui/projects/"+project.ID)(http.StatusNotFound, err.Error())
		return nil, nil, nil
	}
	return identity, project, branch
}

// A branch with neither a compute to ask nor a snapshot to fall back on is one nothing is known
// about, and a change made against a guess is how the two ends stop agreeing. Starting it is what
// makes it knowable, and that is a thing a person does deliberately.
func (s *Server) unknownState(ctx context.Context, branch *registry.Branch) bool {
	if branch.LastSeen != nil {
		return false
	}
	instance, err := s.computes.Get(ctx, branch.EndpointID)
	return err != nil || !instance.Running()
}

// The page reports what the branch is, never what it was asked to be: a change that did not reach
// a compute is not shown as having been made.
func (s *Server) patchBranch(w http.ResponseWriter, r *http.Request, identity *Identity, project *registry.Project, branch *registry.Branch, patch registry.Patch, note notice, deltas ...neon.DeltaOp) {
	if s.unknownState(r.Context(), branch) {
		s.renderBranch(w, r, identity, project, branch, "branch-update", http.StatusConflict,
			notice{Error: "nothing is known about this branch; start it before changing anything"})
		return
	}
	if err := s.applyCatalog(r.Context(), branch, patch, deltas...); err != nil {
		s.log.Error("applying a catalog change", "branch", branch.Name, "error", err)
		s.renderBranch(w, r, identity, project, branch, "branch-update", statusOf(err), notice{Error: err.Error()})
		return
	}
	s.renderBranch(w, r, identity, project, branch, "branch-update", http.StatusOK, note)
}

// Roles are replaced wholesale, so the ones that are staying have to be restated — by verifier,
// since the password behind each is not kept.
func roleSpecsOf(branch *registry.Branch) []registry.RoleSpec {
	specs := make([]registry.RoleSpec, 0, len(branch.Roles))
	for _, role := range branch.Roles {
		specs = append(specs, registry.RoleSpec{Name: role.Name, Verifier: role.Verifier, Secret: role.Secret})
	}
	return specs
}

// A password is worth printing against the database the role owns, which is the one Postgres will
// let it do anything in. A role that owns none is offered no database rather than a guessed one.
func ownedDatabase(branch *registry.Branch, role string) string {
	for _, database := range branch.Databases {
		if database.Owner == role {
			return database.Name
		}
	}
	return ""
}

func (s *Server) handleUIAddRole(w http.ResponseWriter, r *http.Request) {
	identity, project, branch := s.uiBranch(w, r)
	if branch == nil {
		return
	}
	name := strings.TrimSpace(r.PostFormValue("role"))
	password := r.PostFormValue("password")
	if password == "" {
		generated, err := newPassword()
		if err != nil {
			s.log.Error("generating a password", "branch", branch.Name, "error", err)
			s.renderBranch(w, r, identity, project, branch, "branch-update", http.StatusInternalServerError,
				notice{Error: "cannot generate a password"})
			return
		}
		password = generated
	}

	sealed, err := s.seal(name, password)
	if err != nil {
		s.log.Error("sealing a password", "branch", branch.Name, "error", err)
		s.renderBranch(w, r, identity, project, branch, "branch-update", http.StatusInternalServerError,
			notice{Error: "cannot store the password"})
		return
	}
	specs := append(roleSpecsOf(branch), registry.RoleSpec{Name: name, Password: password, Secret: sealed})
	note := notice{Secret: &credential{
		Branch: branch.Name,
		Role:   name,
		URI:    connectionURI(s.endpointHost(branch), name, ownedDatabase(branch, name), password),
		Kept:   s.secrets != nil,
	}}
	s.patchBranch(w, r, identity, project, branch, registry.Patch{Roles: &specs}, note)
}

// The owner is named rather than inferred: which role owns a database decides what every other role
// may do in it, and it cannot be changed afterwards from here.
func (s *Server) handleUIAddDatabase(w http.ResponseWriter, r *http.Request) {
	identity, project, branch := s.uiBranch(w, r)
	if branch == nil {
		return
	}
	databases := append(slices.Clone(branch.Databases), registry.Database{
		Name:  strings.TrimSpace(r.PostFormValue("database")),
		Owner: strings.TrimSpace(r.PostFormValue("owner")),
	})
	s.patchBranch(w, r, identity, project, branch, registry.Patch{Databases: &databases}, notice{})
}

// A removal has to be named, because a list of what should exist cannot say that something should
// stop existing. The registry refuses the removals that would break the branch — the last role, or
// a role some database is owned by — so neither is checked again here.
func (s *Server) handleUIDeleteRole(w http.ResponseWriter, r *http.Request) {
	identity, project, branch := s.uiBranch(w, r)
	if branch == nil {
		return
	}
	role := r.PathValue("role")
	specs := slices.DeleteFunc(roleSpecsOf(branch), func(spec registry.RoleSpec) bool { return spec.Name == role })
	s.patchBranch(w, r, identity, project, branch, registry.Patch{Roles: &specs}, notice{},
		neon.DeltaOp{Action: neon.DeleteRole, Name: role})
}

func (s *Server) handleUIDeleteDatabase(w http.ResponseWriter, r *http.Request) {
	identity, project, branch := s.uiBranch(w, r)
	if branch == nil {
		return
	}
	name := r.PathValue("database")
	databases := slices.DeleteFunc(slices.Clone(branch.Databases), func(database registry.Database) bool {
		return database.Name == name
	})
	s.patchBranch(w, r, identity, project, branch, registry.Patch{Databases: &databases}, notice{},
		neon.DeltaOp{Action: neon.DeleteDatabase, Name: name})
}

func (s *Server) handleUIResetPassword(w http.ResponseWriter, r *http.Request) {
	identity, project, branch := s.uiBranch(w, r)
	if branch == nil {
		return
	}
	role := r.PathValue("role")
	password, err := newPassword()
	if err != nil {
		s.log.Error("generating a password", "branch", branch.Name, "error", err)
		s.renderBranch(w, r, identity, project, branch, "branch-update", http.StatusInternalServerError,
			notice{Error: "cannot generate a password"})
		return
	}

	sealed, err := s.seal(role, password)
	if err != nil {
		s.log.Error("sealing a password", "branch", branch.Name, "error", err)
		s.renderBranch(w, r, identity, project, branch, "branch-update", http.StatusInternalServerError,
			notice{Error: "cannot store the password"})
		return
	}

	specs := roleSpecsOf(branch)
	found := false
	for i := range specs {
		if specs[i].Name == role {
			specs[i] = registry.RoleSpec{Name: role, Password: password, Secret: sealed}
			found = true
		}
	}
	if !found {
		s.renderBranch(w, r, identity, project, branch, "branch-update", http.StatusNotFound,
			notice{Error: "no role " + role + " on " + branch.Name})
		return
	}

	note := notice{Secret: &credential{
		Branch: branch.Name,
		Role:   role,
		URI:    connectionURI(s.endpointHost(branch), role, ownedDatabase(branch, role), password),
		Kept:   s.secrets != nil,
	}}
	s.patchBranch(w, r, identity, project, branch, registry.Patch{Roles: &specs}, note)
}

func (s *Server) handleUIStartBranch(w http.ResponseWriter, r *http.Request) {
	s.branchAction(w, r, func(branch *registry.Branch) error {
		_, err := s.ensureRunning(r.Context(), branch)
		return withStatus(http.StatusServiceUnavailable, err)
	})
}

func (s *Server) handleUIStopBranch(w http.ResponseWriter, r *http.Request) {
	s.branchAction(w, r, func(branch *registry.Branch) error {
		return s.stopBranch(r.Context(), branch)
	})
}

func (s *Server) handleUIDeleteBranch(w http.ResponseWriter, r *http.Request) {
	s.branchAction(w, r, func(branch *registry.Branch) error {
		return s.deleteBranch(r.Context(), branch)
	})
}

// Every action on a branch answers with the whole list, since any of them can change what another
// row shows.
func (s *Server) branchAction(w http.ResponseWriter, r *http.Request, act func(*registry.Branch) error) {
	identity, project, branch := s.uiBranch(w, r)
	if branch == nil {
		return
	}
	err := act(branch)
	// Suspending writes the snapshot against the copy of the branch it resolved for itself, so the
	// one held here is behind the moment the action returns. The answer is rendered from a re-read
	// or it reports a branch nothing is known about, one poll before the snapshot appears.
	if fresh, reread := s.registry.Branch(r.Context(), project.ID, branch.Name); reread == nil {
		branch = fresh
	}
	if err != nil {
		s.log.Error("branch action", "branch", branch.Name, "error", err)
		s.answerBranchAction(w, r, identity, project, branch, statusOf(err), notice{Error: err.Error()})
		return
	}
	s.answerBranchAction(w, r, identity, project, branch, http.StatusOK, notice{})
}
