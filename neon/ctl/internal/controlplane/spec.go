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
		return nil, withStatus(http.StatusServiceUnavailable, fmt.Errorf("resolving placement for %s: %w", instance.ID, err))
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
		SuspendTimeoutSeconds:  -1,
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

	// A spec is read once, at startup, and nothing revisits it. Serving one without the catalog
	// would leave a branch running that no role can log in to, so it is refused instead.
	branch, err := s.registry.Endpoint(ctx, instance.ID)
	if err != nil {
		if errors.Is(err, registry.ErrNotFound) {
			return nil, withStatus(http.StatusNotFound, fmt.Errorf("no branch owns compute %s", instance.ID))
		}
		return nil, withStatus(http.StatusServiceUnavailable, fmt.Errorf("reading the catalog of %s: %w", instance.ID, err))
	}

	for _, role := range branch.Roles {
		// A role with no verifier must travel as null. Upstream reads any non-SCRAM string as an
		// md5 hash, so an empty one becomes PASSWORD 'md5' — and the ALTER carrying it grants LOGIN.
		entry := neon.Role{Name: role.Name}
		if role.Verifier != "" {
			verifier := role.Verifier
			entry.EncryptedPassword = &verifier
		}
		spec.Cluster.Roles = append(spec.Cluster.Roles, entry)
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

// Without a port here Postgres listens on 5432 while compute_ctl dials what the pod template
// declares. The storage GUCs are absent deliberately: they ride the spec's own fields.
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
		// compute_ctl retries a 503 three times a tenth of a second apart and then exits, so the
		// pod restart is the real backoff. Any other status it gives up on at once.
		s.log.Error("rendering spec", "compute", id, "error", err)
		writeError(w, statusOf(err), err.Error())
		return
	}

	writeJSON(w, http.StatusOK, neon.ControlPlaneConfigResponse{
		Spec:             spec,
		Status:           neon.StatusAttached,
		ComputeCtlConfig: s.key.ComputeCtlConfig(),
	})
}
