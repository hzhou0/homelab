package controlplane

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/hzhou0/homelab/neon/ctl/internal/neon"
	"github.com/hzhou0/homelab/neon/ctl/internal/registry"
)

type createProjectRequest struct {
	Name   string   `json:"name"`
	Groups []string `json:"groups,omitempty"`
	// Only an admin may name someone else, so that creating a project cannot be used to hand one
	// to a person who did not ask for it.
	Owner string `json:"owner,omitempty"`
}

type projectView struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	TenantID  string    `json:"tenant_id"`
	Owner     string    `json:"owner"`
	Groups    []string  `json:"groups"`
	Branches  int       `json:"branches"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

func (s *Server) projectView(r *http.Request, project *registry.Project) projectView {
	view := projectView{
		ID:        project.ID,
		Name:      project.Name,
		TenantID:  project.TenantID.String(),
		Owner:     project.Owner,
		Groups:    project.Groups,
		CreatedAt: project.CreatedAt,
		UpdatedAt: project.UpdatedAt,
	}
	if view.Groups == nil {
		view.Groups = []string{}
	}
	if branches, err := s.registry.Branches(r.Context(), project.ID); err == nil {
		view.Branches = len(branches)
	}
	return view
}

// Listing is filtered rather than refused: a caller sees the projects they hold, which for most
// people is a short list and for nobody is an error.
func (s *Server) handleListProjects(w http.ResponseWriter, r *http.Request) {
	identity := s.caller(w, r)
	if identity == nil {
		return
	}
	projects, err := s.registry.Projects(r.Context())
	if err != nil {
		s.log.Error("listing projects", "error", err)
		writeError(w, http.StatusServiceUnavailable, "registry unavailable")
		return
	}
	views := make([]projectView, 0, len(projects))
	for i := range projects {
		if identity.permits(&projects[i]) {
			views = append(views, s.projectView(r, &projects[i]))
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"projects": views})
}

func (s *Server) handleGetProject(w http.ResponseWriter, r *http.Request) {
	_, project := s.scope(w, r)
	if project == nil {
		return
	}
	writeJSON(w, http.StatusOK, s.projectView(r, project))
}

// The tenant is minted here and never asked for. It is an implementation detail of the project,
// and letting a caller choose it would let them collide with somebody else's storage.
func (s *Server) handleCreateProject(w http.ResponseWriter, r *http.Request) {
	identity := s.caller(w, r)
	if identity == nil {
		return
	}
	var request createProjectRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		writeError(w, http.StatusBadRequest, "malformed request body")
		return
	}

	owner := identity.User
	if request.Owner != "" && request.Owner != identity.User {
		if !identity.Admin {
			writeError(w, http.StatusForbidden, "only an admin may create a project for someone else")
			return
		}
		owner = request.Owner
	}
	if !identity.mayGrant(request.Groups) {
		writeError(w, http.StatusForbidden, "you may only grant groups you belong to")
		return
	}

	tenant, err := neon.NewTenantID()
	if err != nil {
		s.log.Error("minting a tenant", "error", err)
		writeError(w, http.StatusInternalServerError, "cannot mint a tenant")
		return
	}
	project, err := registry.NewProject(request.Name, owner, request.Groups, tenant)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	// The tenant exists before the project is recorded, so a failure here leaves a tenant the
	// controller can still enumerate rather than a project pointing at nothing.
	if err := s.storcon.CreateTenant(r.Context(), tenant); err != nil {
		s.log.Error("creating tenant", "project", project.Name, "error", err)
		writeError(w, http.StatusBadGateway, "cannot create tenant")
		return
	}
	if err := s.registry.CreateProject(r.Context(), project); err != nil {
		if cleanup := s.storcon.DeleteTenant(r.Context(), tenant); cleanup != nil {
			s.log.Error("deleting the tenant of a failed project", "tenant", tenant, "error", cleanup)
		}
		if errors.Is(err, registry.ErrNameTaken) {
			writeError(w, http.StatusConflict, "you already have a project with that name")
			return
		}
		s.log.Error("recording project", "project", project.Name, "error", err)
		writeError(w, http.StatusServiceUnavailable, "registry unavailable")
		return
	}
	writeJSON(w, http.StatusCreated, s.projectView(r, project))
}

type patchProjectRequest struct {
	Groups *[]string `json:"groups,omitempty"`
	Owner  *string   `json:"owner,omitempty"`
}

// Re-granting is an act of ownership, so a group member who can use the project cannot change who
// else may. They can see it, which is why this refuses rather than pretending it is absent.
func (s *Server) handlePatchProject(w http.ResponseWriter, r *http.Request) {
	identity, project := s.scope(w, r)
	if project == nil {
		return
	}
	if !identity.Admin && identity.User != project.Owner {
		writeError(w, http.StatusForbidden, "only the owner may change who else may reach this project")
		return
	}

	var request patchProjectRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		writeError(w, http.StatusBadRequest, "malformed request body")
		return
	}

	if request.Owner != nil && *request.Owner != project.Owner {
		// Nothing here can check that an identifier belongs to anybody, and handing a project to a
		// mistyped one leaves it reachable by no one but the administrator.
		if !identity.Admin {
			writeError(w, http.StatusForbidden, "only an admin may hand a project to someone else")
			return
		}
		project.Owner = *request.Owner
	}
	if request.Groups != nil {
		if !identity.mayGrant(*request.Groups) {
			writeError(w, http.StatusForbidden, "you may only grant groups you belong to")
			return
		}
		project.Groups = *request.Groups
	}

	project.UpdatedAt = time.Now().UTC()
	if err := s.registry.PutProject(r.Context(), project); err != nil {
		s.log.Error("recording project", "project", project.Name, "error", err)
		writeError(w, http.StatusServiceUnavailable, "registry unavailable")
		return
	}
	writeJSON(w, http.StatusOK, s.projectView(r, project))
}

func (s *Server) handleDeleteProject(w http.ResponseWriter, r *http.Request) {
	_, project := s.scope(w, r)
	if project == nil {
		return
	}

	branches, err := s.registry.Branches(r.Context(), project.ID)
	if err != nil {
		s.log.Error("listing branches", "project", project.Name, "error", err)
		writeError(w, http.StatusServiceUnavailable, "registry unavailable")
		return
	}
	if len(branches) > 0 {
		writeError(w, http.StatusConflict, "delete the project's branches first")
		return
	}

	if err := s.registry.DeleteProject(r.Context(), project.ID); err != nil {
		s.log.Error("removing project", "project", project.Name, "error", err)
		writeError(w, http.StatusServiceUnavailable, "registry unavailable")
		return
	}
	if err := s.storcon.DeleteTenant(r.Context(), project.TenantID); err != nil {
		// The project is already gone, so this is reported rather than retried: what is left is a
		// tenant with no timelines, which costs nothing until someone reaps it.
		s.log.Error("deleting tenant", "project", project.Name, "tenant", project.TenantID, "error", err)
	}
	w.WriteHeader(http.StatusNoContent)
}
