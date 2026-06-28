// Package controller contains the Service and Gateway reconcilers that drive
// OPNsense DNS host overrides and WAN port-forwards from homelab.lab/*
// annotations, external-dns style.
package controller

import (
	"context"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/hzhou0/homelab/opnsense-operator/internal/config"
	"github.com/hzhou0/homelab/opnsense-operator/internal/opnsense"
)

// requeueNotReady is how long to wait before re-checking an object whose
// LoadBalancer IP has not been assigned yet.
const requeueNotReady = 15 * time.Second

// Syncer is the slice of the OPNsense client the reconcilers depend on. It is
// an interface so tests can substitute a fake.
type Syncer interface {
	Sync(ctx context.Context, owner opnsense.Owner, dns []opnsense.HostOverride, pf *opnsense.PortForward) error
	Delete(ctx context.Context, owner opnsense.Owner) error
}

// handle runs the shared reconcile flow for any source object: finalizer
// management, desired-state parsing, OPNsense sync, and status write-back. The
// caller resolves the object-specific bits (IP, default port/protocol) into
// ExposureInput.
func handle(
	ctx context.Context,
	c client.Client,
	rec record.EventRecorder,
	cfg *config.Config,
	opn Syncer,
	obj client.Object,
	owner opnsense.Owner,
	in ExposureInput,
) (ctrl.Result, error) {
	l := log.FromContext(ctx)

	// Deletion: clean up OPNsense state, then release the finalizer.
	if !obj.GetDeletionTimestamp().IsZero() {
		if controllerutil.ContainsFinalizer(obj, Finalizer) {
			if err := opn.Delete(ctx, owner); err != nil {
				rec.Eventf(obj, corev1.EventTypeWarning, "CleanupFailed", "OPNsense cleanup failed: %v", err)
				return ctrl.Result{}, err
			}
			base := obj.DeepCopyObject().(client.Object)
			controllerutil.RemoveFinalizer(obj, Finalizer)
			if err := c.Patch(ctx, obj, client.MergeFrom(base)); err != nil {
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{}, nil
	}

	desired, err := ParseExposure(in, cfg)
	if err != nil {
		// Bad annotations are a user error; record it and don't hot-loop.
		rec.Eventf(obj, corev1.EventTypeWarning, "InvalidExposure", "%v", err)
		l.Info("invalid exposure annotations", "error", err.Error())
		return ctrl.Result{}, nil
	}

	// Nothing requested: make sure any previously-created state is removed and
	// the finalizer released.
	if desired.Empty() {
		if controllerutil.ContainsFinalizer(obj, Finalizer) {
			if err := opn.Delete(ctx, owner); err != nil {
				return ctrl.Result{}, err
			}
			base := obj.DeepCopyObject().(client.Object)
			controllerutil.RemoveFinalizer(obj, Finalizer)
			removeAnnotation(obj, AnnExposed)
			if err := c.Patch(ctx, obj, client.MergeFrom(base)); err != nil {
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{}, nil
	}

	// Need an IP before we can wire anything up.
	if in.IP == "" {
		l.Info("waiting for LoadBalancer IP")
		return ctrl.Result{RequeueAfter: requeueNotReady}, nil
	}

	// Add the finalizer before creating external state so a delete can never
	// orphan OPNsense objects. Re-reconcile on the next pass with it in place.
	if !controllerutil.ContainsFinalizer(obj, Finalizer) {
		base := obj.DeepCopyObject().(client.Object)
		controllerutil.AddFinalizer(obj, Finalizer)
		if err := c.Patch(ctx, obj, client.MergeFrom(base)); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	if err := opn.Sync(ctx, owner, desired.Hosts, desired.PortForward); err != nil {
		rec.Eventf(obj, corev1.EventTypeWarning, "SyncFailed", "OPNsense sync failed: %v", err)
		return ctrl.Result{}, err
	}

	// Record what we wired up (without fighting the LB controller for status.loadBalancer).
	// Only log/emit on an actual change, so steady-state re-reconciles stay quiet.
	summary := desired.Summary()
	if obj.GetAnnotations()[AnnExposed] != summary {
		base := obj.DeepCopyObject().(client.Object)
		setAnnotation(obj, AnnExposed, summary)
		if err := c.Patch(ctx, obj, client.MergeFrom(base)); err != nil {
			return ctrl.Result{}, err
		}
		l.Info("reconciled OPNsense state", "ip", in.IP, "exposed", summary)
		rec.Eventf(obj, corev1.EventTypeNormal, "Synced", "OPNsense updated: %s", summary)
	}
	return ctrl.Result{}, nil
}

func setAnnotation(obj client.Object, key, val string) {
	ann := obj.GetAnnotations()
	if ann == nil {
		ann = map[string]string{}
	}
	ann[key] = val
	obj.SetAnnotations(ann)
}

func removeAnnotation(obj client.Object, key string) {
	ann := obj.GetAnnotations()
	if ann == nil {
		return
	}
	delete(ann, key)
	obj.SetAnnotations(ann)
}

// loadBalancerIP returns the first assigned IP (or hostname) from a Service's
// LoadBalancer status, or "" if none yet.
func loadBalancerIP(svc *corev1.Service) string {
	for _, ing := range svc.Status.LoadBalancer.Ingress {
		if ing.IP != "" {
			return ing.IP
		}
		if ing.Hostname != "" {
			return ing.Hostname
		}
	}
	return ""
}
