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

	// Whether a password can be asked for a second time here at all.
	Passwords bool
}

// connectOptions is what the connect strings can be built from. The names come from the compute
// while it is up and from what it had when it was last stopped while it is down, because a string
// that names a database is what wakes it.
type connectOptions struct {
	Roles     []string
	Databases []string
	URI       string

	// Set when the names are from a compute that has since stopped, which is a thing to say rather
	// than to hide: one of them may have been dropped since.
	Since *time.Time
}

// catalogState is the registry and Postgres laid over each other. What this service records is
// what may authenticate; what Postgres holds is what exists, and the two are allowed to differ.
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

	known    bool
	Recorded bool
	Live     bool
}

func (e catalogEntry) Managed() bool { return e.Recorded }
func (e catalogEntry) Pending() bool { return e.known && e.Recorded && !e.Live }
func (e catalogEntry) Foreign() bool { return e.Live && !e.Recorded }

// A branch is read from two places that can disagree, and saying so is the whole point: a name the
// compute does not have is one nothing can use yet, and one only the compute has was not made here.
func (s *Server) catalogState(ctx context.Context, branch *registry.Branch, instance *kube.Instance) catalogState {
	state := catalogState{}
	for _, role := range branch.Roles {
		state.Roles = append(state.Roles, catalogEntry{Name: role.Name, Recorded: true})
	}
	for _, database := range branch.Databases {
		state.Databases = append(state.Databases, catalogEntry{Name: database.Name, Owner: database.Owner, Recorded: true})
	}
	if instance == nil || !instance.Running() {
		return state
	}

	live, err := s.liveCatalog(ctx, instance)
	if err != nil {
		s.log.Error("reading a compute catalog", "branch", branch.Name, "error", err)
		return state
	}
	state.Known = true

	roles := map[string]bool{}
	for _, role := range live.Roles {
		roles[role.Name] = true
	}
	for i := range state.Roles {
		state.Roles[i].known = true
		state.Roles[i].Live = roles[state.Roles[i].Name]
		delete(roles, state.Roles[i].Name)
	}
	for _, role := range live.Roles {
		if roles[role.Name] {
			state.Roles = append(state.Roles, catalogEntry{Name: role.Name, known: true, Live: true})
		}
	}

	databases := map[string]string{}
	for _, database := range live.Databases {
		databases[database.Name] = database.Owner
	}
	for i := range state.Databases {
		owner, live := databases[state.Databases[i].Name]
		state.Databases[i].known = true
		state.Databases[i].Live = live
		// The owner Postgres reports is the one deciding what a role may do there, so it is the one
		// worth showing when the two disagree.
		if live && owner != "" {
			state.Databases[i].Owner = owner
		}
		delete(databases, state.Databases[i].Name)
	}
	for _, database := range live.Databases {
		if owner, ok := databases[database.Name]; ok {
			state.Databases = append(state.Databases, catalogEntry{Name: database.Name, Owner: owner, known: true, Live: true})
		}
	}
	return state
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
func (s *Server) connectOptions(branch *registry.Branch, host string, catalog catalogState) connectOptions {
	options := connectOptions{}
	switch {
	case catalog.Known:
		for _, entry := range catalog.Roles {
			if entry.Live {
				options.Roles = append(options.Roles, entry.Name)
			}
		}
		for _, entry := range catalog.Databases {
			if entry.Live {
				options.Databases = append(options.Databases, entry.Name)
			}
		}
	case branch.LastSeen != nil:
		options.Roles = branch.LastSeen.Roles
		options.Databases = branch.LastSeen.Databases
		options.Since = &branch.LastSeen.At
	default:
		for _, role := range branch.Roles {
			options.Roles = append(options.Roles, role.Name)
		}
		for _, database := range branch.Databases {
			options.Databases = append(options.Databases, database.Name)
		}
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
	options.URI = connectionURI(host, role, options.Databases[0], "")
	return options
}

func (s *Server) endpointHost(branch *registry.Branch) string {
	if s.opts.EndpointSuffix == "" {
		return branch.EndpointID
	}
	return branch.EndpointID + "." + s.opts.EndpointSuffix
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
	s.renderProjects(w, r, identity, "projects", http.StatusOK, notice{})
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
	s.renderBranches(w, r, identity, project, "branches", http.StatusOK, notice{})
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
	s.renderBranch(w, r, identity, project, branch, "branch", http.StatusOK, notice{})
}

func (s *Server) renderBranch(w http.ResponseWriter, r *http.Request, identity *Identity, project *registry.Project, branch *registry.Branch, template string, status int, note notice) {
	view := s.projectView(r, project)
	row := s.branchRow(r.Context(), branch)
	instance, err := s.computes.Get(r.Context(), branch.EndpointID)
	if err != nil {
		instance = nil
	}
	row.Catalog = s.catalogState(r.Context(), branch, instance)
	row.Connect = s.connectOptions(branch, row.Host, row.Catalog)
	row.Passwords = s.secrets != nil
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

	request := createBranchRequest{
		Name:   strings.TrimSpace(r.PostFormValue("name")),
		Parent: strings.TrimSpace(r.PostFormValue("parent")),
		Start:  r.PostFormValue("start") != "",
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
	if request.Start {
		if _, err := s.ensureRunning(r.Context(), branch); err != nil {
			s.log.Error("starting new branch", "branch", branch.Name, "error", err)
			note.Error = "the branch exists but its compute did not start: " + err.Error()
		}
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

// Every change to a catalog is the same three steps, and the third can fail on its own: the branch
// is already changed, and only the compute is behind.
func (s *Server) patchBranch(w http.ResponseWriter, r *http.Request, identity *Identity, project *registry.Project, branch *registry.Branch, patch registry.Patch, note notice) {
	if err := branch.Apply(patch); err != nil {
		s.renderBranch(w, r, identity, project, branch, "branch-update", http.StatusBadRequest, notice{Error: err.Error()})
		return
	}
	if err := s.registry.Put(r.Context(), branch); err != nil {
		s.log.Error("recording branch", "branch", branch.Name, "error", err)
		s.renderBranch(w, r, identity, project, branch, "branch-update", http.StatusServiceUnavailable, notice{Error: "registry unavailable"})
		return
	}
	if err := s.reconfigureBranch(r.Context(), branch); err != nil {
		s.log.Error("applying branch change to a running compute", "branch", branch.Name, "error", err)
		note.Error = "the compute has not taken the change yet; restart the branch to apply it"
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

// Dropping is the compute's to do, and it is asked to do it before the catalog forgets the name:
// a removal recorded here alone would leave the object in Postgres, holding its data, reachable by
// nothing and listed nowhere. The registry refuses the removals that would break the branch — the
// last role, or a role some database is owned by — so neither is checked again here.
func (s *Server) handleUIDeleteRole(w http.ResponseWriter, r *http.Request) {
	identity, project, branch := s.uiBranch(w, r)
	if branch == nil {
		return
	}
	role := r.PathValue("role")
	specs := slices.DeleteFunc(roleSpecsOf(branch), func(spec registry.RoleSpec) bool { return spec.Name == role })
	s.dropFrom(w, r, identity, project, branch, registry.Patch{Roles: &specs},
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
	s.dropFrom(w, r, identity, project, branch, registry.Patch{Databases: &databases},
		neon.DeltaOp{Action: neon.DeleteDatabase, Name: name})
}

// A drop is the one change that is not a statement of what should exist, so it cannot be left for
// the compute to pick up later: it happens now, against a running compute, or it does not happen.
// The record is put back if the compute refuses, because a removal recorded here and not performed
// there is exactly the divergence this page exists to not have.
func (s *Server) dropFrom(w http.ResponseWriter, r *http.Request, identity *Identity, project *registry.Project, branch *registry.Branch, patch registry.Patch, delta neon.DeltaOp) {
	refuse := func(status int, message string) {
		s.renderBranch(w, r, identity, project, branch, "branch-update", status, notice{Error: message})
	}

	recorded := *branch
	if err := branch.Apply(patch); err != nil {
		refuse(http.StatusBadRequest, err.Error())
		return
	}
	if err := s.registry.Put(r.Context(), branch); err != nil {
		s.log.Error("recording branch", "branch", branch.Name, "error", err)
		refuse(http.StatusServiceUnavailable, "registry unavailable")
		return
	}

	if err := s.dropFromCatalog(r.Context(), branch, delta); err != nil {
		s.log.Error("dropping from a compute", "branch", branch.Name, "name", delta.Name, "error", err)
		*branch = recorded
		if restore := s.registry.Put(r.Context(), branch); restore != nil {
			s.log.Error("restoring a branch whose drop failed", "branch", branch.Name, "error", restore)
		}
		refuse(http.StatusBadGateway, "the compute did not drop "+delta.Name+", so nothing was removed: "+err.Error())
		return
	}
	s.renderBranch(w, r, identity, project, branch, "branch-update", http.StatusOK, notice{})
}

// A password that was kept can be shown again; one that was not is gone, and the difference is
// worth saying out loud rather than answering both with an empty string.
func (s *Server) handleUIRevealPassword(w http.ResponseWriter, r *http.Request) {
	identity, project, branch := s.uiBranch(w, r)
	if branch == nil {
		return
	}
	role, found := branch.Role(r.PathValue("role"))
	if !found {
		s.renderBranch(w, r, identity, project, branch, "branch-update", http.StatusNotFound,
			notice{Error: "no role " + r.PathValue("role") + " on " + branch.Name})
		return
	}
	// Answers carrying a password are not something a cache anywhere between here and the browser
	// should be keeping.
	w.Header().Set("Cache-Control", "no-store")
	password, ok := s.reveal(&role)
	if !ok {
		message := "this deployment keeps no passwords, so " + role.Name + "'s cannot be shown again"
		if s.secrets != nil {
			message = role.Name + "'s password was not kept; reset it to be given a new one"
		}
		s.renderBranch(w, r, identity, project, branch, "branch-update", http.StatusNotFound,
			notice{Error: message})
		return
	}

	database := r.FormValue("database")
	if database == "" {
		database = ownedDatabase(branch, role.Name)
	}
	s.renderBranch(w, r, identity, project, branch, "branch-update", http.StatusOK, notice{
		Secret: &credential{
			Branch: branch.Name,
			Role:   role.Name,
			URI:    connectionURI(s.endpointHost(branch), role.Name, database, password),
			Kept:   true,
		},
	})
}

// The password is replaced rather than read back: only a verifier is kept, so the one moment it can
// be shown is the moment it is set.
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
	if err := act(branch); err != nil {
		s.log.Error("branch action", "branch", branch.Name, "error", err)
		s.answerBranchAction(w, r, identity, project, branch, statusOf(err), notice{Error: err.Error()})
		return
	}
	s.answerBranchAction(w, r, identity, project, branch, http.StatusOK, notice{})
}
