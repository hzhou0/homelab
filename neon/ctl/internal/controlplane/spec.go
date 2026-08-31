package controlplane

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"

	"github.com/hzhou0/homelab/neon/ctl/internal/kube"
	"github.com/hzhou0/homelab/neon/ctl/internal/neon"
	"github.com/hzhou0/homelab/neon/ctl/internal/registry"
)

// Placement is always read live. A cached answer that only notifications update would, on the
// recovery path, hand a broken compute the same broken answer forever.
func (s *Server) renderSpec(ctx context.Context, instance *kube.Instance) (*neon.ComputeSpec, error) {
	placement, err := s.storcon.ResolvePlacement(ctx, instance.TenantID, instance.TimelineID)
	if err != nil {
		return nil, fmt.Errorf("resolving placement for %s: %w", instance.ID, err)
	}

	// The branch name is the compute id, the endpoint id and the cluster name: Neon keys metrics
	// and logs by all three, and here they are one thing.
	tenant := instance.TenantID
	timeline := instance.TimelineID
	name := instance.ID

	spec := &neon.ComputeSpec{
		FormatVersion:          1.0,
		TenantID:               &tenant,
		TimelineID:             &timeline,
		PageserverConnstring:   &placement.PageserverConnstring,
		ShardStripeSize:        placement.ShardStripeSize,
		SafekeeperConnstrings:  placement.SafekeeperConnstrings,
		Mode:                   instance.Mode,
		BranchID:               &name,
		EndpointID:             &name,
		ReconfigureConcurrency: 1,
		SuspendTimeoutSeconds:  int64(s.opts.SuspendTimeout.Seconds()),
		Cluster: neon.Cluster{
			ClusterID: &name,
			Name:      &name,
			Roles:     []neon.Role{},
			Databases: []neon.Database{},
		},
	}
	if s.storageKey != nil {
		token, err := s.storageKey.Token(neon.StorageClaims{TenantID: &tenant, Scope: neon.ScopeTenant})
		if err != nil {
			return nil, fmt.Errorf("signing a storage token for %s: %w", instance.ID, err)
		}
		spec.StorageAuthToken = &token
	}
	if placement.SafekeepersGeneration != 0 {
		generation := placement.SafekeepersGeneration
		spec.SafekeepersGeneration = &generation
	}

	branch, err := s.registry.Get(ctx, instance.ID)
	if err != nil {
		// The catalog lives in the timeline, so a spec with none mutates nothing and the compute
		// still boots. This is what keeps a registry outage off the recovery path.
		if errors.Is(err, registry.ErrNotFound) {
			s.log.Warn("serving a spec for a compute with no registry entry", "compute", instance.ID)
		} else {
			s.log.Error("registry unavailable, serving a spec without catalog contents",
				"compute", instance.ID, "error", err)
		}
		spec.SkipPgCatalogUpdates = true
		return spec, nil
	}

	for _, role := range branch.Roles {
		verifier := role.Verifier
		spec.Cluster.Roles = append(spec.Cluster.Roles, neon.Role{
			Name:              role.Name,
			EncryptedPassword: &verifier,
		})
	}
	for _, database := range branch.Databases {
		spec.Cluster.Databases = append(spec.Cluster.Databases, neon.Database{
			Name:  database.Name,
			Owner: database.Owner,
		})
	}
	spec.Cluster.Settings = settingsFor(instance, branch.Settings)
	return spec, nil
}

// A compute needs more than the branch's own settings to run at all: without a port it listens on
// 5432 while compute_ctl dials the one the pod template declares, and marks the compute failed.
// The tenant, timeline, pageserver and safekeeper GUCs are not here — they ride the spec's own
// fields, and naming them twice would let the two disagree.
func settingsFor(instance *kube.Instance, branch []registry.Setting) []neon.GenericOption {
	settings := []neon.GenericOption{
		{Name: "listen_addresses", Value: ptr("0.0.0.0"), VarType: "string"},
		{Name: "shared_preload_libraries", Value: ptr("neon"), VarType: "string"},
		// Durability belongs to the safekeeper quorum; a compute's own disk is scratch.
		{Name: "fsync", Value: ptr("off"), VarType: "bool"},
		{Name: "wal_level", Value: ptr("logical"), VarType: "enum"},
		{Name: "wal_log_hints", Value: ptr("on"), VarType: "bool"},
		{Name: "hot_standby", Value: ptr("on"), VarType: "bool"},
		{Name: "max_wal_senders", Value: ptr("10"), VarType: "integer"},
		{Name: "max_replication_slots", Value: ptr("10"), VarType: "integer"},
		{Name: "max_connections", Value: ptr("100"), VarType: "integer"},
		// walproposer is the standby, and it must never time out: the safekeepers are the WAL.
		{Name: "synchronous_standby_names", Value: ptr("walproposer"), VarType: "string"},
		{Name: "wal_sender_timeout", Value: ptr("0"), VarType: "integer"},
		// Roles are provisioned from SCRAM verifiers, which md5 encryption would reject.
		{Name: "password_encryption", Value: ptr("scram-sha-256"), VarType: "enum"},
	}
	if _, port, err := net.SplitHostPort(instance.PgAddress); err == nil {
		settings = append(settings, neon.GenericOption{Name: "port", Value: &port, VarType: "integer"})
	}

	for _, setting := range branch {
		value := setting.Value
		varType := setting.VarType
		if varType == "" {
			varType = "string"
		}
		replaced := false
		for i := range settings {
			if settings[i].Name == setting.Name {
				settings[i] = neon.GenericOption{Name: setting.Name, Value: &value, VarType: varType}
				replaced = true
				break
			}
		}
		if !replaced {
			settings = append(settings, neon.GenericOption{Name: setting.Name, Value: &value, VarType: varType})
		}
	}
	return settings
}

func ptr[T any](value T) *T { return &value }

func (s *Server) handleComputeSpec(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("compute_id")

	instance, err := s.computes.Get(r.Context(), id)
	if errors.Is(err, kube.ErrNotFound) {
		writeError(w, http.StatusNotFound, fmt.Sprintf("no compute named %q", id))
		return
	}
	if err != nil {
		s.log.Error("looking up compute for spec", "compute", id, "error", err)
		writeError(w, http.StatusServiceUnavailable, "cannot resolve compute")
		return
	}

	if instance.TimelineID.IsZero() {
		writeJSON(w, http.StatusOK, neon.ControlPlaneConfigResponse{
			Status:           neon.StatusEmpty,
			ComputeCtlConfig: s.key.ComputeCtlConfig(),
		})
		return
	}

	spec, err := s.renderSpec(r.Context(), instance)
	if err != nil {
		// Retryable: compute_ctl backs off on 503 and gives up on 500.
		s.log.Error("rendering spec", "compute", id, "error", err)
		writeError(w, http.StatusServiceUnavailable, "cannot resolve storage placement")
		return
	}

	writeJSON(w, http.StatusOK, neon.ControlPlaneConfigResponse{
		Spec:             spec,
		Status:           neon.StatusAttached,
		ComputeCtlConfig: s.key.ComputeCtlConfig(),
	})
}
