package controlplane

import (
	"context"
	"errors"
	"net/http"

	"github.com/hzhou0/homelab/neon/ctl/internal/registry"
)

// Authentication happens at the proxy, before the wake, so a bad password never costs a cold start.

// The proxy has no branch concept: an endpoint it cannot resolve, for whatever reason, must read
// as absent rather than as an outage, or it retries a name that will never exist.
func (s *Server) endpointBranch(ctx context.Context, w http.ResponseWriter, endpoint string) *registry.Branch {
	branch, err := s.registry.Get(ctx, endpoint)
	switch {
	case err == nil:
		return branch
	case isNotFound(err), errors.Is(err, registry.ErrInvalidName):
		writeProxyError(w, http.StatusNotFound, reasonEndpointNotFound, "endpoint not found")
	default:
		s.log.Error("reading branch for the proxy", "endpoint", endpoint, "error", err)
		writeProxyError(w, http.StatusServiceUnavailable, "", "registry unavailable")
	}
	return nil
}

type endpointAccessControl struct {
	RoleSecret string `json:"role_secret"`
}

// A password changed inside Postgres rather than through this API leaves proxy authentication
// failing while a direct connection still works.
func (s *Server) handleEndpointAccessControl(w http.ResponseWriter, r *http.Request) {
	endpoint := r.URL.Query().Get("endpointish")
	roleName := r.URL.Query().Get("role")

	branch := s.endpointBranch(r.Context(), w, endpoint)
	if branch == nil {
		return
	}

	role, found := branch.Role(roleName)
	if !found {
		writeProxyError(w, http.StatusNotFound, reasonRoleNotFound, "role not found")
		return
	}
	writeJSON(w, http.StatusOK, endpointAccessControl{RoleSecret: role.Verifier})
}

type metricsAuxInfo struct {
	EndpointID string `json:"endpoint_id"`
	ProjectID  string `json:"project_id"`
	BranchID   string `json:"branch_id"`
	ComputeID  string `json:"compute_id"`
}

type wakeCompute struct {
	Address string `json:"address"`
	// A null server name tells the proxy to reach the compute without TLS, which is what the
	// fenced namespace makes safe.
	ServerName *string        `json:"server_name"`
	Aux        metricsAuxInfo `json:"aux"`
}

func (s *Server) handleWakeCompute(w http.ResponseWriter, r *http.Request) {
	endpoint := r.URL.Query().Get("endpointish")

	branch := s.endpointBranch(r.Context(), w, endpoint)
	if branch == nil {
		return
	}

	instance, err := s.ensureRunning(r.Context(), branch)
	if err != nil {
		s.log.Error("waking compute", "endpoint", endpoint, "error", err)
		writeProxyError(w, http.StatusServiceUnavailable, "", "compute did not start")
		return
	}

	writeJSON(w, http.StatusOK, wakeCompute{
		Address: instance.PgAddress,
		Aux: metricsAuxInfo{
			EndpointID: branch.Name,
			ProjectID:  branch.TenantID.String(),
			BranchID:   branch.TimelineID.String(),
			ComputeID:  instance.ID,
		},
	})
}

type endpointJWKS struct {
	JWKS []struct{} `json:"jwks"`
}

func (s *Server) handleEndpointJWKS(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, endpointJWKS{JWKS: []struct{}{}})
}
