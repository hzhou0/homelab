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

	PgVersion int    `json:"pg_version,omitempty"`
	Mode      string `json:"mode,omitempty"`

	Roles     []roleRequest       `json:"roles,omitempty"`
	Databases []registry.Database `json:"databases,omitempty"`
	Settings  []registry.Setting  `json:"settings,omitempty"`

	Start bool `json:"start,omitempty"`
}

// Every path from a request to a role passes through here, so sealing is asked for by the type
// rather than remembered at each caller.
type sealer func(role, password string) (string, error)

func (r createBranchRequest) spec(seal sealer) (registry.Spec, error) {
	mode, err := neon.ParseComputeMode(r.Mode)
	if err != nil {
		return registry.Spec{}, err
	}
	roles, err := roleSpecs(r.Roles, seal)
	if err != nil {
		return registry.Spec{}, err
	}

	spec := registry.Spec{
		Name:      r.Name,
		Mode:      mode,
		Roles:     roles,
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

func (r patchBranchRequest) patch(seal sealer) (registry.Patch, error) {
	patch := registry.Patch{Databases: r.Databases, Settings: r.Settings}
	if r.Roles != nil {
		specs, err := roleSpecs(*r.Roles, seal)
		if err != nil {
			return registry.Patch{}, err
		}
		patch.Roles = &specs
	}
	return patch, nil
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

func roleSpecs(requested []roleRequest, seal sealer) ([]registry.RoleSpec, error) {
	specs := make([]registry.RoleSpec, 0, len(requested))
	for _, role := range requested {
		spec := registry.RoleSpec{Name: role.Name, Password: role.Password, Verifier: role.Verifier}
		// A verifier wins over a password, so sealing one that will not be used would keep a
		// password the role does not have.
		if role.Password != "" && role.Verifier == "" {
			sealed, err := seal(role.Name, role.Password)
			if err != nil {
				return nil, err
			}
			spec.Secret = sealed
		}
		specs = append(specs, spec)
	}
	return specs, nil
}

type computeView struct {
	Status     string     `json:"status"`
	Replicas   int32      `json:"replicas"`
	LastActive *time.Time `json:"last_active,omitempty"`
	Error      string     `json:"error,omitempty"`
}

type branchView struct {
	Name       string  `json:"name"`
	ProjectID  string  `json:"project_id"`
	EndpointID string  `json:"endpoint_id"`
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
	_, project := s.scope(w, r)
	if project == nil {
		return
	}
	branches, err := s.registry.Branches(r.Context(), project.ID)
	if err != nil {
		s.log.Error("listing branches", "project", project.ID, "error", err)
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
	_, project := s.scope(w, r)
	if project == nil {
		return
	}
	var request createBranchRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		writeError(w, http.StatusBadRequest, "malformed request body")
		return
	}

	branch, err := s.createBranch(r.Context(), project, request)
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
// cannot leave a timeline behind. The tenant is the project's and already exists.
func (s *Server) createBranch(ctx context.Context, project *registry.Project, request createBranchRequest) (*registry.Branch, error) {
	spec, err := request.spec(s.seal)
	if err != nil {
		return nil, withStatus(http.StatusBadRequest, err)
	}

	branch, err := s.buildBranch(ctx, project, request, spec)
	if err != nil {
		return nil, err
	}

	if err := s.storcon.CreateTimeline(ctx, branch.TenantID, branch.TimelineCreateRequest()); err != nil {
		return nil, withStatus(http.StatusBadGateway, fmt.Errorf("creating timeline: %w", err))
	}
	// Create rather than Put: the name is claimed in the same transaction it is checked in, so two
	// callers racing for it cannot both believe they won.
	if err := s.registry.Create(ctx, branch); err != nil {
		if cleanup := s.storcon.DeleteTimeline(ctx, branch.TenantID, branch.TimelineID); cleanup != nil {
			s.log.Error("deleting the timeline of a failed branch", "timeline", branch.TimelineID, "error", cleanup)
		}
		if errors.Is(err, registry.ErrNameTaken) {
			return nil, withStatus(http.StatusConflict, err)
		}
		return nil, withStatus(http.StatusServiceUnavailable, fmt.Errorf("recording branch: %w", err))
	}
	return branch, nil
}

// buildBranch answers one question: root or fork. Both take their tenant from the project, so a
// branch can never be created outside the isolation boundary its project owns.
func (s *Server) buildBranch(ctx context.Context, project *registry.Project, request createBranchRequest, spec registry.Spec) (*registry.Branch, error) {
	if request.Parent != "" {
		parent, err := s.registry.Branch(ctx, project.ID, request.Parent)
		if err != nil {
			if isNotFound(err) || errors.Is(err, registry.ErrInvalidName) {
				return nil, withStatus(http.StatusBadRequest, fmt.Errorf("parent branch %q does not exist", request.Parent))
			}
			return nil, withStatus(http.StatusServiceUnavailable, err)
		}
		branch, err := parent.Fork(spec, project)
		return branch, withStatus(http.StatusBadRequest, err)
	}

	pgVersion, err := s.resolvePgVersion(request.PgVersion)
	if err != nil {
		return nil, withStatus(http.StatusBadRequest, err)
	}
	branch, err := registry.New(spec, pgVersion, project)
	return branch, withStatus(http.StatusBadRequest, err)
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
	patch, err := request.patch(s.seal)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := branch.Apply(patch); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := s.registry.Put(r.Context(), branch); err != nil {
		s.log.Error("recording branch", "branch", branch.Name, "error", err)
		writeError(w, http.StatusServiceUnavailable, "registry unavailable")
		return
	}

	if err := s.reconfigureBranch(r.Context(), branch); err != nil {
		s.log.Error("applying branch change to a running compute", "branch", branch.Name, "error", err)
	}

	writeJSON(w, http.StatusOK, s.view(r.Context(), branch))
}

// A running compute keeps its old catalog until it is told; the spec endpoint alone would not
// reach it until its next restart.
func (s *Server) reconfigureBranch(ctx context.Context, branch *registry.Branch) error {
	instance, err := s.computes.Get(ctx, branch.EndpointID)
	if err != nil || !instance.Running() {
		return nil
	}
	return s.configure(ctx, instance)
}

// A drop is performed by the compute and by nothing else, so the branch is woken for it. Recording
// the removal without it would leave the object in Postgres holding data that nothing can reach.
func (s *Server) dropFromCatalog(ctx context.Context, branch *registry.Branch, delta neon.DeltaOp) error {
	instance, err := s.ensureRunning(ctx, branch)
	if err != nil {
		return withStatus(http.StatusServiceUnavailable, fmt.Errorf("waking the branch to drop %s: %w", delta.Name, err))
	}
	return s.configure(ctx, instance, delta)
}

func (s *Server) configure(ctx context.Context, instance *kube.Instance, deltas ...neon.DeltaOp) error {
	spec, err := s.renderSpec(ctx, instance)
	if err != nil {
		return err
	}
	spec.DeltaOperations = deltas
	client, err := s.computeClient(instance)
	if err != nil {
		return err
	}
	return client.Configure(ctx, spec)
}

type revealedPassword struct {
	Role     string `json:"role"`
	Password string `json:"password"`
}

// Mirrors what the page does, because everything the page does is a request somebody could have
// made by hand. A deployment that keeps no passwords refuses rather than pretending to have none.
func (s *Server) handleRevealPassword(w http.ResponseWriter, r *http.Request) {
	branch := s.namedBranch(w, r)
	if branch == nil {
		return
	}
	if s.secrets == nil {
		writeError(w, http.StatusPreconditionFailed, "this deployment keeps no passwords")
		return
	}
	role, found := branch.Role(r.PathValue("role"))
	if !found {
		writeError(w, http.StatusNotFound, "role not found")
		return
	}
	password, ok := s.reveal(&role)
	if !ok {
		writeError(w, http.StatusNotFound, "no password was kept for this role")
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, http.StatusOK, revealedPassword{Role: role.Name, Password: password})
}

func (s *Server) handleDeleteBranch(w http.ResponseWriter, r *http.Request) {
	branch := s.namedBranch(w, r)
	if branch == nil {
		return
	}
	if err := s.deleteBranch(r.Context(), branch); err != nil {
		s.log.Error("deleting branch", "branch", branch.Name, "error", err)
		writeError(w, statusOf(err), err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// The compute goes first: a timeline deleted under a running compute would leave it serving pages
// that no longer have a backing store.
func (s *Server) deleteBranch(ctx context.Context, branch *registry.Branch) error {
	if err := s.computes.Delete(ctx, branch.EndpointID); err != nil {
		return withStatus(http.StatusServiceUnavailable, fmt.Errorf("removing compute: %w", err))
	}
	if err := s.storcon.DeleteTimeline(ctx, branch.TenantID, branch.TimelineID); err != nil {
		return withStatus(http.StatusBadGateway, fmt.Errorf("removing timeline: %w", err))
	}
	if err := s.registry.Delete(ctx, branch.ProjectID, branch.Name); err != nil && !errors.Is(err, registry.ErrNotFound) {
		return withStatus(http.StatusServiceUnavailable, fmt.Errorf("removing branch: %w", err))
	}
	return nil
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
	if err := s.stopBranch(r.Context(), branch); err != nil {
		s.log.Error("stopping branch", "branch", branch.Name, "error", err)
		writeError(w, statusOf(err), err.Error())
		return
	}
	writeJSON(w, http.StatusOK, s.view(r.Context(), branch))
}

// A branch with no compute is already stopped, which is not an error to ask for again.
func (s *Server) stopBranch(ctx context.Context, branch *registry.Branch) error {
	instance, err := s.computes.Get(ctx, branch.EndpointID)
	if errors.Is(err, kube.ErrNotFound) {
		return nil
	}
	if err != nil {
		return withStatus(http.StatusServiceUnavailable, fmt.Errorf("resolving compute: %w", err))
	}
	if err := s.suspend(ctx, instance); err != nil {
		return withStatus(http.StatusServiceUnavailable, fmt.Errorf("stopping compute: %w", err))
	}
	return nil
}

// Every branch route resolves through its project, so a caller who may not see the project never
// reaches the branch — including to learn whether it exists.
func (s *Server) namedBranch(w http.ResponseWriter, r *http.Request) *registry.Branch {
	_, project := s.scope(w, r)
	if project == nil {
		return nil
	}
	branch, err := s.registry.Branch(r.Context(), project.ID, r.PathValue("name"))
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
		ProjectID:  branch.ProjectID,
		EndpointID: branch.EndpointID,
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

	instance, err := s.computes.Get(ctx, branch.EndpointID)
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
