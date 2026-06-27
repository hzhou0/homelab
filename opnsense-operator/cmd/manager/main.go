// Command manager runs the OPNsense external-dns + port-forward controller.
package main

import (
	"flag"
	"os"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	crconfig "sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/hzhou0/homelab/opnsense-operator/internal/config"
	"github.com/hzhou0/homelab/opnsense-operator/internal/controller"
	"github.com/hzhou0/homelab/opnsense-operator/internal/opnsense"
)

var scheme = runtime.NewScheme()

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(gatewayv1.Install(scheme))
}

func main() {
	var metricsAddr, probeAddr string
	var enableLeaderElection bool
	var startupTimeout time.Duration
	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080", "Address the metric endpoint binds to.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "Address the probe endpoint binds to.")
	flag.BoolVar(&enableLeaderElection, "leader-elect", true, "Enable leader election for controller manager.")
	flag.DurationVar(&startupTimeout, "startup-timeout", 60*time.Second,
		"Per-controller cache-sync deadline. If a watch can't sync within this window "+
			"(e.g. a required CRD like Gateway is missing), the manager exits non-zero "+
			"instead of hanging NotReady, so Kubernetes restarts the pod.")
	opts := zap.Options{Development: false}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))
	setupLog := ctrl.Log.WithName("setup")

	cfg, err := config.FromEnv()
	if err != nil {
		setupLog.Error(err, "invalid configuration")
		os.Exit(1)
	}

	opnClient, err := opnsense.New(cfg)
	if err != nil {
		setupLog.Error(err, "unable to create OPNsense client")
		os.Exit(1)
	}

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsserver.Options{BindAddress: metricsAddr},
		HealthProbeBindAddress: probeAddr,
		LeaderElection:         enableLeaderElection,
		LeaderElectionID:       "opnsense-operator.homelab.lab",
		// Bound cache sync so a missing CRD / unreachable API server fails startup
		// loudly (mgr.Start returns an error -> os.Exit below) rather than hanging.
		Controller: crconfig.Controller{CacheSyncTimeout: startupTimeout},
	})
	if err != nil {
		setupLog.Error(err, "unable to start manager")
		os.Exit(1)
	}

	if err := (&controller.ServiceReconciler{
		Client:   mgr.GetClient(),
		OPN:      opnClient,
		Cfg:      cfg,
		Recorder: mgr.GetEventRecorderFor("opnsense-operator"),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "Service")
		os.Exit(1)
	}

	if err := (&controller.GatewayReconciler{
		Client:   mgr.GetClient(),
		OPN:      opnClient,
		Cfg:      cfg,
		Recorder: mgr.GetEventRecorderFor("opnsense-operator"),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "Gateway")
		os.Exit(1)
	}

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up ready check")
		os.Exit(1)
	}

	setupLog.Info("starting manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "problem running manager")
		os.Exit(1)
	}
}
