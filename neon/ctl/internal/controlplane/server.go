// Package controlplane is the join between the storage layer, which knows tenants and timelines
// but not computes, and the computes, which know only their own id.
package controlplane

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/hzhou0/homelab/neon/ctl/internal/kube"
	"github.com/hzhou0/homelab/neon/ctl/internal/neon"
	"github.com/hzhou0/homelab/neon/ctl/internal/registry"
	"golang.org/x/sync/singleflight"
)

// proxyBasePath is our own route prefix, not something the proxy dictates: the chart points the
// proxy's --auth-endpoint at it.
const proxyBasePath = "/proxy/v1"

type Options struct {
	// WakeTimeout bounds how long a connection waits for a suspended compute. A cold start is a
	// basebackup, so this scales with working-set size.
	WakeTimeout time.Duration

	// SuspendTimeout is reported to compute_ctl and drives its idle metrics; the decision to
	// scale to zero is made here. Zero never suspends.
	SuspendTimeout time.Duration
}

type Server struct {
	storcon  *neon.StorageController
	registry *registry.Store
	computes kube.Runtime
	log      *slog.Logger
	opts     Options

	key           *neon.SigningKey
	storageKey    *neon.StorageKey
	storageAuth   *neon.StorageVerifier
	computeClient func(instance *kube.Instance) (*neon.ComputeCtl, error)
	waking        singleflight.Group
}

func New(storcon *neon.StorageController, store *registry.Store, computes kube.Runtime, key *neon.SigningKey, storageKey *neon.StorageKey, log *slog.Logger, opts Options) *Server {
	server := &Server{
		storcon:    storcon,
		registry:   store,
		computes:   computes,
		log:        log,
		key:        key,
		storageKey: storageKey,
		opts:       opts,
	}
	if storageKey != nil {
		server.storageAuth = storageKey.Verifier()
	}
	server.computeClient = func(instance *kube.Instance) (*neon.ComputeCtl, error) {
		return neon.NewComputeCtl(instance.ControlURL, instance.ID, key, nil)
	}
	return server
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("PUT /notify-attach", s.handleNotifyAttach)
	mux.HandleFunc("PUT /notify-safekeepers", s.handleNotifySafekeepers)
	mux.HandleFunc("GET /compute/api/v2/computes/{compute_id}/spec", s.handleComputeSpec)

	base := proxyBasePath
	mux.HandleFunc("GET "+base+"/get_endpoint_access_control", s.handleEndpointAccessControl)
	mux.HandleFunc("GET "+base+"/wake_compute", s.handleWakeCompute)
	mux.HandleFunc("GET "+base+"/endpoints/{endpoint}/jwks", s.handleEndpointJWKS)

	mux.HandleFunc("GET /api/branches", s.handleListBranches)
	mux.HandleFunc("POST /api/branches", s.handleCreateBranch)
	mux.HandleFunc("GET /api/branches/{name}", s.handleGetBranch)
	mux.HandleFunc("PATCH /api/branches/{name}", s.handlePatchBranch)
	mux.HandleFunc("DELETE /api/branches/{name}", s.handleDeleteBranch)
	mux.HandleFunc("POST /api/branches/{name}/start", s.handleStartBranch)
	mux.HandleFunc("POST /api/branches/{name}/stop", s.handleStopBranch)

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /readyz", s.handleReady)

	return mux
}

// handleReady probes the storage controller, because a control plane that cannot resolve
// placement can only serve stale specs, which is the one failure a compute cannot recover from.
func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	if err := s.storcon.Ping(r.Context()); err != nil {
		s.log.Warn("readiness probe failed", "error", err)
		writeError(w, http.StatusServiceUnavailable, "storage controller unreachable")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

type apiError struct {
	Error string `json:"error"`
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, apiError{Error: message})
}

// Creating a branch spans the registry, the controller and Kubernetes, and which of them refused
// decides the status. Carrying it on the error keeps that decision where the failure happened.
type statusError struct {
	status int
	err    error
}

func (e statusError) Error() string { return e.err.Error() }

func withStatus(code int, err error) error {
	if err == nil {
		return nil
	}
	return statusError{status: code, err: err}
}

func statusOf(err error) int {
	var carried statusError
	if errors.As(err, &carried) {
		return carried.status
	}
	return http.StatusInternalServerError
}

// The proxy reads the reason to tell a missing secret from an outage, so a plain status is not
// enough.
type errorReason struct {
	Reason string `json:"reason"`
}

type userFacingMessage struct {
	Message string `json:"message"`
}

type errorDetails struct {
	ErrorInfo         *errorReason       `json:"error_info"`
	UserFacingMessage *userFacingMessage `json:"user_facing_message"`
}

type controlPlaneError struct {
	Error  string `json:"error"`
	Status struct {
		Details errorDetails `json:"details"`
	} `json:"status"`
}

const (
	reasonEndpointNotFound = "ENDPOINT_NOT_FOUND"
	reasonRoleNotFound     = "ROLE_NOT_FOUND"
)

func writeProxyError(w http.ResponseWriter, status int, reason, message string) {
	body := controlPlaneError{Error: message}
	body.Status.Details = errorDetails{
		ErrorInfo:         &errorReason{Reason: reason},
		UserFacingMessage: &userFacingMessage{Message: message},
	}
	writeJSON(w, status, body)
}

const readinessPoll = time.Second

// A cold start is slow enough that a burst of connections arrives while it is in progress, so they
// share one attempt rather than each racing an update of the same object.
func (s *Server) ensureRunning(ctx context.Context, branch *registry.Branch) (*kube.Instance, error) {
	instance, err, _ := s.waking.Do(branch.Name, func() (any, error) {
		return s.wake(ctx, branch)
	})
	if err != nil {
		return nil, err
	}
	return instance.(*kube.Instance), nil
}

func (s *Server) wake(ctx context.Context, branch *registry.Branch) (*kube.Instance, error) {
	instance, err := s.computes.Ensure(ctx, kube.Binding{
		ID:         branch.Name,
		TenantID:   branch.TenantID,
		TimelineID: branch.TimelineID,
		Mode:       branch.Mode,
	}, branch.PgVersion)
	if err != nil {
		return nil, err
	}
	if instance.Running() {
		return instance, nil
	}

	deadline := time.Now().Add(s.opts.WakeTimeout)
	for {
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("compute %s did not become ready within %s", branch.Name, s.opts.WakeTimeout)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(readinessPoll):
		}

		instance, err = s.computes.Get(ctx, branch.Name)
		if err != nil {
			return nil, err
		}
		if instance.Running() {
			return instance, nil
		}
	}
}

// suspend asks the compute to shut down before removing it. A crash would be safe — the WAL is on
// the safekeepers — but a clean shutdown restarts faster and reports the LSN it stopped at.
func (s *Server) suspend(ctx context.Context, instance *kube.Instance) error {
	if instance.Running() {
		client, err := s.computeClient(instance)
		if err == nil {
			_, err = client.Terminate(ctx, neon.TerminateFast)
		}
		if err != nil {
			s.log.Warn("compute did not terminate cleanly, scaling down anyway",
				"compute", instance.ID, "error", err)
		}
	}
	return s.computes.Scale(ctx, instance.ID, 0)
}

func isNotFound(err error) bool {
	return errors.Is(err, registry.ErrNotFound) || errors.Is(err, kube.ErrNotFound)
}

// RunSuspender scales idle computes to zero. compute_ctl reports its own last activity, so idle
// detection is a poll rather than anything we instrument, and nothing here has to be persisted.
func (s *Server) RunSuspender(ctx context.Context) {
	if s.opts.SuspendTimeout <= 0 {
		return
	}
	// Polling faster than the threshold only bounds how long past it a compute lingers.
	ticker := time.NewTicker(s.opts.SuspendTimeout / 4)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.suspendIdle(ctx)
		}
	}
}

func (s *Server) suspendIdle(ctx context.Context) {
	instances, err := s.computes.List(ctx)
	if err != nil {
		s.log.Error("listing computes to suspend", "error", err)
		return
	}

	for i := range instances {
		instance := &instances[i]
		if !instance.Running() {
			continue
		}

		client, err := s.computeClient(instance)
		if err != nil {
			s.log.Error("signing a token for a compute", "compute", instance.ID, "error", err)
			continue
		}
		status, err := client.Status(ctx)
		if err != nil {
			s.log.Warn("cannot read compute status", "compute", instance.ID, "error", err)
			continue
		}

		// Only a compute that reached Running has meaningful activity: one still starting has no
		// last_active and must not be taken down mid-basebackup.
		if status.Status != neon.ComputeRunning {
			continue
		}
		idleSince := status.StartTime
		if status.LastActive != nil {
			idleSince = *status.LastActive
		}
		if time.Since(idleSince) < s.opts.SuspendTimeout {
			continue
		}

		s.log.Info("suspending idle compute", "compute", instance.ID, "idle_since", idleSince)
		if err := s.suspend(ctx, instance); err != nil {
			s.log.Error("suspending compute", "compute", instance.ID, "error", err)
		}
	}
}
