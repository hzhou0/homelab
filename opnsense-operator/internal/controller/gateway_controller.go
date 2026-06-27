package controller

import (
	"context"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/hzhou0/homelab/opnsense-operator/internal/config"
	"github.com/hzhou0/homelab/opnsense-operator/internal/opnsense"
)

// ciliumGatewayPrefix is the name prefix Cilium gives the LoadBalancer Service
// it provisions for a Gateway (cilium-gateway-<gateway-name>).
const ciliumGatewayPrefix = "cilium-gateway-"

// GatewayReconciler reconciles Gateway-API Gateways. The Gateway's external IP
// lives on the Cilium-provisioned backing Service, which this controller
// resolves and watches.
type GatewayReconciler struct {
	client.Client
	OPN      Syncer
	Cfg      *config.Config
	Recorder record.EventRecorder
}

// +kubebuilder:rbac:groups=gateway.networking.k8s.io,resources=gateways,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch

// Reconcile implements reconcile.Reconciler.
func (r *GatewayReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var gw gatewayv1.Gateway
	if err := r.Get(ctx, req.NamespacedName, &gw); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	owner := opnsense.Owner{Kind: "Gateway", Namespace: gw.Namespace, Name: gw.Name}

	ip := r.backingServiceIP(ctx, &gw)
	in := ExposureInput{
		Annotations:     gw.Annotations,
		IP:              ip,
		DefaultPort:     firstListenerPort(&gw),
		DefaultProtocol: "tcp",
	}
	return handle(ctx, r.Client, r.Recorder, r.Cfg, r.OPN, &gw, owner, in)
}

// backingServiceIP looks up the Cilium-provisioned Service for the Gateway and
// returns its LoadBalancer IP, or "" if not found / not assigned.
func (r *GatewayReconciler) backingServiceIP(ctx context.Context, gw *gatewayv1.Gateway) string {
	var svc corev1.Service
	key := types.NamespacedName{Namespace: gw.Namespace, Name: ciliumGatewayPrefix + gw.Name}
	if err := r.Get(ctx, key, &svc); err != nil {
		return ""
	}
	return loadBalancerIP(&svc)
}

// SetupWithManager wires the controller in and maps backing-Service events back
// to their Gateway so an IP assignment triggers a reconcile.
func (r *GatewayReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&gatewayv1.Gateway{}).
		Watches(&corev1.Service{}, handler.EnqueueRequestsFromMapFunc(mapBackingServiceToGateway)).
		Complete(r)
}

// mapBackingServiceToGateway turns a cilium-gateway-<name> Service into a
// reconcile request for the owning Gateway.
func mapBackingServiceToGateway(_ context.Context, obj client.Object) []reconcile.Request {
	name := obj.GetName()
	if !strings.HasPrefix(name, ciliumGatewayPrefix) {
		return nil
	}
	return []reconcile.Request{{
		NamespacedName: types.NamespacedName{
			Namespace: obj.GetNamespace(),
			Name:      strings.TrimPrefix(name, ciliumGatewayPrefix),
		},
	}}
}

func firstListenerPort(gw *gatewayv1.Gateway) string {
	for _, l := range gw.Spec.Listeners {
		if l.Port != 0 {
			return strconv.Itoa(int(l.Port))
		}
	}
	return ""
}
