package controlplane

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"

	"github.com/hzhou0/homelab/neon/ctl/internal/kube"
	"github.com/hzhou0/homelab/neon/ctl/internal/neon"
)

// The one inbound path that changes what a running compute is told. A rejected token answers 403
// deliberately: the controller never retries that, and it never would succeed.
func (s *Server) authorized(w http.ResponseWriter, r *http.Request) bool {
	if s.storageAuth == nil {
		return true
	}
	token, found := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !found {
		writeError(w, http.StatusForbidden, "a bearer token is required")
		return false
	}
	claims, err := s.storageAuth.Verify(token)
	if err != nil {
		s.log.Warn("rejected a notification", "error", err)
		writeError(w, http.StatusForbidden, "token does not verify")
		return false
	}
	// Admin only. Every compute holds a tenant-scoped token, and one of those must not be able to
	// repoint the storage of every other compute.
	if claims.Scope != neon.ScopeAdmin {
		s.log.Warn("rejected a notification", "scope", claims.Scope)
		writeError(w, http.StatusForbidden, "token is not admin-scoped")
		return false
	}
	return true
}

// The controller never retries 400, 401 or 403, so a transient failure returned as one leaves a
// tenant mis-notified until something else kicks a reconcile. Only an unparseable body earns one.
func (s *Server) handleNotifyAttach(w http.ResponseWriter, r *http.Request) {
	if !s.authorized(w, r) {
		return
	}
	var request neon.NotifyAttachRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		s.log.Error("malformed notify-attach", "error", err)
		writeError(w, http.StatusBadRequest, "malformed request body")
		return
	}

	log := s.log.With("hook", "notify-attach", "tenant", request.TenantID.String())

	fresh, err := s.attachIsVisible(r.Context(), request)
	if err != nil {
		log.Error("resolving attachment", "error", err)
		writeError(w, http.StatusServiceUnavailable, "cannot resolve attachment")
		return
	}
	if !fresh {
		// Our read of the controller predates the attachment it is telling us about. Pushing now
		// would send a stale address and report success, and nothing would correct it.
		log.Info("attachment not yet visible, asking for a retry")
		writeError(w, http.StatusServiceUnavailable, "attachment not yet visible")
		return
	}

	instances, err := s.computes.ListByTenant(r.Context(), request.TenantID)
	if err != nil {
		log.Error("listing computes", "error", err)
		writeError(w, http.StatusServiceUnavailable, "cannot list computes")
		return
	}

	s.reconfigure(r.Context(), w, log, instances)
}

func (s *Server) handleNotifySafekeepers(w http.ResponseWriter, r *http.Request) {
	if !s.authorized(w, r) {
		return
	}
	var request neon.NotifySafekeepersRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		s.log.Error("malformed notify-safekeepers", "error", err)
		writeError(w, http.StatusBadRequest, "malformed request body")
		return
	}

	log := s.log.With("hook", "notify-safekeepers",
		"tenant", request.TenantID.String(),
		"timeline", request.TimelineID.String(),
		"generation", request.Generation)

	located, err := s.storcon.LocateTimeline(r.Context(), request.TenantID, request.TimelineID)
	if err != nil {
		log.Error("locating timeline", "error", err)
		writeError(w, http.StatusServiceUnavailable, "cannot resolve safekeepers")
		return
	}
	if located.Generation < request.Generation {
		// The generation must never regress on a compute: walproposer uses it to decide which
		// membership configuration is newer.
		log.Info("membership not yet visible, asking for a retry", "visible_generation", located.Generation)
		writeError(w, http.StatusServiceUnavailable, "membership not yet visible")
		return
	}

	instances, err := s.computes.ListByTimeline(r.Context(), request.TenantID, request.TimelineID)
	if err != nil {
		log.Error("listing computes", "error", err)
		writeError(w, http.StatusServiceUnavailable, "cannot list computes")
		return
	}

	s.reconfigure(r.Context(), w, log, instances)
}

// Notifications carry node ids and addresses are resolved live, so the two must agree before a
// push can mean anything.
func (s *Server) attachIsVisible(ctx context.Context, request neon.NotifyAttachRequest) (bool, error) {
	located, err := s.storcon.LocateTenant(ctx, request.TenantID)
	if err != nil {
		return false, err
	}
	if len(located.Shards) != len(request.Shards) {
		return false, nil
	}
	visible := make(map[int]neon.NodeID, len(located.Shards))
	for _, shard := range located.Shards {
		visible[shard.ShardNumber()] = shard.NodeID
	}
	for _, shard := range request.Shards {
		if visible[int(shard.ShardNumber)] != shard.NodeID {
			return false, nil
		}
	}
	return true, nil
}

// reconfigure pushes to every compute that is actually up. A suspended or starting compute is
// skipped rather than failed: it reads the same document from the spec endpoint on boot.
func (s *Server) reconfigure(ctx context.Context, w http.ResponseWriter, log *slog.Logger, instances []kube.Instance) {
	var pushed, skipped int
	for i := range instances {
		instance := &instances[i]
		if !instance.Running() {
			skipped++
			continue
		}

		spec, err := s.renderSpec(ctx, instance)
		if err != nil {
			log.Error("rendering spec", "compute", instance.ID, "error", err)
			writeError(w, http.StatusServiceUnavailable, "cannot render spec")
			return
		}
		client, err := s.computeClient(instance)
		if err == nil {
			err = client.Configure(ctx, spec)
		}
		if err != nil {
			log.Error("reconfiguring compute", "compute", instance.ID, "error", err)
			writeError(w, http.StatusServiceUnavailable, fmt.Sprintf("compute %s did not accept the new configuration", instance.ID))
			return
		}
		log.Info("reconfigured compute", "compute", instance.ID)
		pushed++
	}

	// No compute bound to this tenant or timeline is a normal state, not an error: returning 404
	// here is what wedges the controller into endless retries.
	writeJSON(w, http.StatusOK, map[string]int{"reconfigured": pushed, "skipped": skipped})
}
