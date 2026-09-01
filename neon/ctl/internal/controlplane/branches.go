package controlplane

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/hzhou0/homelab/neon/ctl/internal/kube"
	"github.com/hzhou0/homelab/neon/ctl/internal/neon"
	"github.com/hzhou0/homelab/neon/ctl/internal/registry"
)

// The types here are the HTTP surface only. Every rule about what a branch is lives on the branch.

type roleRequest struct {
	Name     string `json:"name"`
	Password string `json:"password,omitempty"`
	Verifier string `json:"verifier,omitempty"`
}

type createBranchRequest struct {
	Name string `json:"name"`

	Parent    string  `json:"parent,omitempty"`
	ParentLSN *string `json:"parent_lsn,omitempty"`

	TenantID  string `json:"tenant_id,omitempty"`
	PgVersion int    `json:"pg_version,omitempty"`
	Mode      string `json:"mode,omitempty"`

	Roles     []roleRequest       `json:"roles,omitempty"`
	Databases []registry.Database `json:"databases,omitempty"`
	Settings  []registry.Setting  `json:"settings,omitempty"`

	Start bool `json:"start,omitempty"`
}

func (r createBranchRequest) spec() (registry.Spec, error) {
	mode, err := neon.ParseComputeMode(r.Mode)
	if err != nil {
		return registry.Spec{}, err
	}

	spec := registry.Spec{
		Name:      r.Name,
		Mode:      mode,
		Roles:     roleSpecs(r.Roles),
		Databases: r.Databases,
		Settings:  r.Settings,
	}
	if r.ParentLSN != nil {
		lsn, err := neon.ParseLSN(*r.ParentLSN)
		if err != nil {
			return registry.Spec{}, err
		}
		spec.ParentLSN = &lsn
	}
	return spec, nil
}

type patchBranchRequest struct {
	Roles     *[]roleRequest       `json:"roles,omitempty"`
	Databases *[]registry.Database `json:"databases,omitempty"`
	Settings  *[]registry.Setting  `json:"settings,omitempty"`
}

func (r patchBranchRequest) patch() registry.Patch {
	patch := registry.Patch{Databases: r.Databases, Settings: r.Settings}
	if r.Roles != nil {
		specs := roleSpecs(*r.Roles)
		patch.Roles = &specs
	}
	return patch
}

// Only ever asked for a root branch, since a fork inherits its ancestor's. Unasked takes the
// newest image present, so the set of images is the whole statement of what can run.
func (s *Server) resolvePgVersion(requested int) (int, error) {
	available := s.computes.PgVersions()
	if len(available) == 0 {
		return 0, errors.New("this deployment has no compute images")
	}
	if requested == 0 {
		return available[len(available)-1], nil
	}
	for _, version := range available {
		if version == requested {
			return version, nil
		}
	}
	return 0, fmt.Errorf("no compute image for postgres %d; this deployment can run %v", requested, available)
}

func roleSpecs(requested []roleRequest) []registry.RoleSpec {
	specs := make([]registry.RoleSpec, 0, len(requested))
	for _, role := range requested {
		specs = append(specs, registry.RoleSpec{Name: role.Name, Password: role.Password, Verifier: role.Verifier})
	}
	return specs
}

type computeView struct {
	Status     string     `json:"status"`
	Replicas   int32      `json:"replicas"`
	LastActive *time.Time `json:"last_active,omitempty"`
	Error      string     `json:"error,omitempty"`
}

type branchView struct {
	Name       string  `json:"name"`
	TenantID   string  `json:"tenant_id"`
	TimelineID string  `json:"timeline_id"`
	Parent     string  `json:"parent,omitempty"`
	ParentLSN  *string `json:"parent_lsn,omitempty"`

	PgVersion int    `json:"pg_version"`
	Mode      string `json:"mode"`

	Roles     []string            `json:"roles"`
	Databases []registry.Database `json:"databases"`
	Settings  []registry.Setting  `json:"settings"`

	Compute computeView `json:"compute"`

	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

func (s *Server) handleListBranches(w http.ResponseWriter, r *http.Request) {
	branches, err := s.registry.List(r.Context())
	if err != nil {
		s.log.Error("listing branches", "error", err)
		writeError(w, http.StatusServiceUnavailable, "registry unavailable")
		return
	}
	views := make([]branchView, 0, len(branches))
	for i := range branches {
		views = append(views, s.view(r.Context(), &branches[i]))
	}
	writeJSON(w, http.StatusOK, map[string]any{"branches": views})
}

func (s *Server) handleGetBranch(w http.ResponseWriter, r *http.Request) {
	branch := s.namedBranch(w, r)
	if branch == nil {
		return
	}
	writeJSON(w, http.StatusOK, s.view(r.Context(), branch))
}

func (s *Server) handleCreateBranch(w http.ResponseWriter, r *http.Request) {
	var request createBranchRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		writeError(w, http.StatusBadRequest, "malformed request body")
		return
	}

	branch, err := s.createBranch(r.Context(), request)
	if err != nil {
		s.log.Error("creating branch", "branch", request.Name, "error", err)
		writeError(w, statusOf(err), err.Error())
		return
	}

	if request.Start {
		if _, err := s.ensureRunning(r.Context(), branch); err != nil {
			s.log.Error("starting new branch", "branch", branch.Name, "error", err)
		}
	}
	writeJSON(w, http.StatusCreated, s.view(r.Context(), branch))
}

// The branch is fully built and validated before the first controller call, so a rejected request
// cannot leave a tenant behind.
func (s *Server) createBranch(ctx context.Context, request createBranchRequest) (*registry.Branch, error) {
	spec, err := request.spec()
	if err != nil {
		return nil, withStatus(http.StatusBadRequest, err)
	}

	if _, err := s.registry.Get(ctx, spec.Name); err == nil {
		return nil, withStatus(http.StatusConflict, fmt.Errorf("branch %q already exists", spec.Name))
	} else if !errors.Is(err, registry.ErrNotFound) && !errors.Is(err, registry.ErrInvalidName) {
		return nil, withStatus(http.StatusServiceUnavailable, err)
	}

	branch, tenantIsNew, err := s.buildBranch(ctx, request, spec)
	if err != nil {
		return nil, err
	}

	if tenantIsNew {
		if err := s.storcon.CreateTenant(ctx, branch.TenantID); err != nil {
			return nil, withStatus(http.StatusBadGateway, fmt.Errorf("creating tenant: %w", err))
		}
	}
	if err := s.storcon.CreateTimeline(ctx, branch.TenantID, branch.TimelineCreateRequest()); err != nil {
		if tenantIsNew {
			if cleanup := s.storcon.DeleteTenant(ctx, branch.TenantID); cleanup != nil {
				s.log.Error("deleting the tenant of a failed branch", "tenant", branch.TenantID, "error", cleanup)
			}
		}
		return nil, withStatus(http.StatusBadGateway, fmt.Errorf("creating timeline: %w", err))
	}
	if err := s.registry.Put(ctx, branch); err != nil {
		// The timeline outlives the failed registry write. It stays enumerable from the
		// controller, so nothing is lost but the name.
		return nil, withStatus(http.StatusServiceUnavailable, fmt.Errorf("recording branch: %w", err))
	}
	return branch, nil
}

// buildBranch answers one question: which tenant. A fresh id is minted locally so that the branch
// can be validated before anything is created.
func (s *Server) buildBranch(ctx context.Context, request createBranchRequest, spec registry.Spec) (*registry.Branch, bool, error) {
	if request.Parent != "" {
		parent, err := s.registry.Get(ctx, request.Parent)
		if err != nil {
			if isNotFound(err) || errors.Is(err, registry.ErrInvalidName) {
				return nil, false, withStatus(http.StatusBadRequest, fmt.Errorf("parent branch %q does not exist", request.Parent))
			}
			return nil, false, withStatus(http.StatusServiceUnavailable, err)
		}
		branch, err := parent.Fork(spec)
		return branch, false, withStatus(http.StatusBadRequest, err)
	}

	var (
		tenant neon.TenantID
		err    error
	)
	tenantIsNew := request.TenantID == ""
	if tenantIsNew {
		tenant, err = neon.NewTenantID()
	} else {
		tenant, err = neon.ParseTenantID(request.TenantID)
	}
	if err != nil {
		return nil, false, withStatus(http.StatusBadRequest, err)
	}

	pgVersion, err := s.resolvePgVersion(request.PgVersion)
	if err != nil {
		return nil, false, withStatus(http.StatusBadRequest, err)
	}
	branch, err := registry.New(spec, pgVersion, tenant)
	return branch, tenantIsNew, withStatus(http.StatusBadRequest, err)
}

func (s *Server) handlePatchBranch(w http.ResponseWriter, r *http.Request) {
	branch := s.namedBranch(w, r)
	if branch == nil {
		return
	}
	var request patchBranchRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		writeError(w, http.StatusBadRequest, "malformed request body")
		return
	}
	if err := branch.Apply(request.patch()); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := s.registry.Put(r.Context(), branch); err != nil {
		s.log.Error("recording branch", "branch", branch.Name, "error", err)
		writeError(w, http.StatusServiceUnavailable, "registry unavailable")
		return
	}

	// A running compute keeps its old catalog until it is told; the spec endpoint alone would not
	// reach it until its next restart.
	if instance, err := s.computes.Get(r.Context(), branch.Name); err == nil && instance.Running() {
		spec, err := s.renderSpec(r.Context(), instance)
		if err == nil {
			var client *neon.ComputeCtl
			if client, err = s.computeClient(instance); err == nil {
				err = client.Configure(r.Context(), spec)
			}
		}
		if err != nil {
			s.log.Error("applying branch change to a running compute", "branch", branch.Name, "error", err)
		}
	}

	writeJSON(w, http.StatusOK, s.view(r.Context(), branch))
}

func (s *Server) handleDeleteBranch(w http.ResponseWriter, r *http.Request) {
	branch := s.namedBranch(w, r)
	if branch == nil {
		return
	}
	name := branch.Name

	if err := s.computes.Delete(r.Context(), name); err != nil {
		s.log.Error("deleting compute", "branch", name, "error", err)
		writeError(w, http.StatusServiceUnavailable, "cannot remove compute")
		return
	}
	if err := s.storcon.DeleteTimeline(r.Context(), branch.TenantID, branch.TimelineID); err != nil {
		s.log.Error("deleting timeline", "branch", name, "error", err)
		writeError(w, http.StatusBadGateway, "cannot remove timeline")
		return
	}
	if err := s.registry.Delete(r.Context(), name); err != nil && !errors.Is(err, registry.ErrNotFound) {
		s.log.Error("removing branch from registry", "branch", name, "error", err)
		writeError(w, http.StatusServiceUnavailable, "registry unavailable")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleStartBranch(w http.ResponseWriter, r *http.Request) {
	branch := s.namedBranch(w, r)
	if branch == nil {
		return
	}
	if _, err := s.ensureRunning(r.Context(), branch); err != nil {
		s.log.Error("starting branch", "branch", branch.Name, "error", err)
		writeError(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, s.view(r.Context(), branch))
}

func (s *Server) handleStopBranch(w http.ResponseWriter, r *http.Request) {
	branch := s.namedBranch(w, r)
	if branch == nil {
		return
	}
	instance, err := s.computes.Get(r.Context(), branch.Name)
	if errors.Is(err, kube.ErrNotFound) {
		writeJSON(w, http.StatusOK, s.view(r.Context(), branch))
		return
	}
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "cannot resolve compute")
		return
	}
	if err := s.suspend(r.Context(), instance); err != nil {
		s.log.Error("stopping branch", "branch", branch.Name, "error", err)
		writeError(w, http.StatusServiceUnavailable, "cannot stop compute")
		return
	}
	writeJSON(w, http.StatusOK, s.view(r.Context(), branch))
}

func (s *Server) namedBranch(w http.ResponseWriter, r *http.Request) *registry.Branch {
	branch, err := s.registry.Get(r.Context(), r.PathValue("name"))
	switch {
	case err == nil:
		return branch
	case isNotFound(err):
		writeError(w, http.StatusNotFound, err.Error())
	case errors.Is(err, registry.ErrInvalidName):
		writeError(w, http.StatusBadRequest, err.Error())
	default:
		s.log.Error("reading branch", "error", err)
		writeError(w, http.StatusServiceUnavailable, "registry unavailable")
	}
	return nil
}

// view renders live state rather than anything tracked: what is running comes from the runtime
// and from compute_ctl's own status, so nothing here can drift from the cluster.
func (s *Server) view(ctx context.Context, branch *registry.Branch) branchView {
	view := branchView{
		Name:       branch.Name,
		TenantID:   branch.TenantID.String(),
		TimelineID: branch.TimelineID.String(),
		Parent:     branch.Parent,
		PgVersion:  branch.PgVersion,
		Mode:       string(branch.Mode.Kind),
		Roles:      make([]string, 0, len(branch.Roles)),
		Databases:  branch.Databases,
		Settings:   branch.Settings,
		CreatedAt:  branch.CreatedAt,
		UpdatedAt:  branch.UpdatedAt,
		Compute:    computeView{Status: "absent"},
	}
	if branch.ParentLSN != nil {
		lsn := branch.ParentLSN.String()
		view.ParentLSN = &lsn
	}
	for _, role := range branch.Roles {
		view.Roles = append(view.Roles, role.Name)
	}

	instance, err := s.computes.Get(ctx, branch.Name)
	if err != nil {
		return view
	}
	view.Compute.Replicas = instance.Replicas
	switch {
	case instance.Replicas == 0:
		view.Compute.Status = "suspended"
	case !instance.Ready:
		view.Compute.Status = "starting"
	default:
		view.Compute.Status = "running"
	}

	if instance.Running() {
		client, err := s.computeClient(instance)
		if err != nil {
			return view
		}
		status, err := client.Status(ctx)
		if err == nil {
			view.Compute.Status = string(status.Status)
			view.Compute.LastActive = status.LastActive
			if status.Error != nil {
				view.Compute.Error = *status.Error
			}
		}
	}
	return view
}
