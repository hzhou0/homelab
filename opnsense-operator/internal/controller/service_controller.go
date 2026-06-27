package controller

import (
	"context"
	"strconv"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/hzhou0/homelab/opnsense-operator/internal/config"
	"github.com/hzhou0/homelab/opnsense-operator/internal/opnsense"
)

// ServiceReconciler reconciles type=LoadBalancer Services into OPNsense DNS and
// port-forward state.
type ServiceReconciler struct {
	client.Client
	OPN      Syncer
	Cfg      *config.Config
	Recorder record.EventRecorder
}

// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

// Reconcile implements reconcile.Reconciler.
func (r *ServiceReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var svc corev1.Service
	if err := r.Get(ctx, req.NamespacedName, &svc); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	owner := opnsense.Owner{Kind: "Service", Namespace: svc.Namespace, Name: svc.Name}

	// Only LoadBalancer Services participate. If a Service was one and is no
	// longer (or is being deleted), handle() still runs to release the finalizer
	// and clean up.
	if svc.Spec.Type != corev1.ServiceTypeLoadBalancer && !hasFinalizer(&svc) {
		return ctrl.Result{}, nil
	}

	in := ExposureInput{
		Annotations:     svc.Annotations,
		IP:              loadBalancerIP(&svc),
		DefaultPort:     firstServicePort(&svc),
		DefaultProtocol: firstServiceProtocol(&svc),
	}
	return handle(ctx, r.Client, r.Recorder, r.Cfg, r.OPN, &svc, owner, in)
}

// SetupWithManager wires the controller into the manager.
func (r *ServiceReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&corev1.Service{}).
		Complete(r)
}

func hasFinalizer(obj client.Object) bool {
	for _, f := range obj.GetFinalizers() {
		if f == Finalizer {
			return true
		}
	}
	return false
}

func firstServicePort(svc *corev1.Service) string {
	if len(svc.Spec.Ports) == 0 {
		return ""
	}
	return strconv.Itoa(int(svc.Spec.Ports[0].Port))
}

func firstServiceProtocol(svc *corev1.Service) string {
	if len(svc.Spec.Ports) == 0 {
		return "tcp"
	}
	switch svc.Spec.Ports[0].Protocol {
	case corev1.ProtocolUDP:
		return "udp"
	default:
		return "tcp"
	}
}
