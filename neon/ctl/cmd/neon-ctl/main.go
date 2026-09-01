package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/hzhou0/homelab/neon/ctl/internal/config"
	"github.com/hzhou0/homelab/neon/ctl/internal/controlplane"
	"github.com/hzhou0/homelab/neon/ctl/internal/kube"
	"github.com/hzhou0/homelab/neon/ctl/internal/neon"
	"github.com/hzhou0/homelab/neon/ctl/internal/registry"
)

func main() {
	// Every component derives what it needs, so this mints nothing the deployment depends on. It
	// is here for reaching the storage layer by hand.
	if len(os.Args) > 1 && os.Args[1] == "token" {
		if err := printToken(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	if len(os.Args) > 1 && os.Args[1] == "derive" {
		if err := derive(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	cfg, err := config.Load(os.Args[1:])
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	storageKey, err := storageAuthKey(cfg)
	if err != nil {
		return err
	}
	controllerToken := cfg.StorageControllerToken
	if storageKey != nil {
		// Every controller route falls back to admin, so one token covers tenant and timeline
		// CRUD, placement lookups and the node and safekeeper listings alike.
		if controllerToken, err = storageKey.Token(neon.StorageClaims{Scope: neon.ScopeAdmin}); err != nil {
			return err
		}
	}

	storcon, err := neon.NewStorageController(cfg.StorageControllerURL, controllerToken,
		&http.Client{Timeout: 30 * time.Second})
	if err != nil {
		return err
	}

	store, err := registry.Open(config.RegistryPath)
	if err != nil {
		return err
	}
	defer store.Close()

	client, err := kubernetesClient()
	if err != nil {
		return err
	}

	computes, err := kube.NewComputes(client, kube.ComputeOptions{
		Namespace:       cfg.Namespace,
		PodTemplatePath: cfg.ComputePodTemplate,
		Images:          cfg.ComputeImages,
	})
	if err != nil {
		return err
	}

	key, err := signingKey(ctx, store)
	if err != nil {
		return err
	}

	server := controlplane.New(storcon, store, computes, key, storageKey, log, controlplane.Options{
		WakeTimeout:    cfg.WakeTimeout,
		SuspendTimeout: cfg.SuspendTimeout,
	})

	go server.RunSuspender(ctx)
	go kube.NewCertRoller(client, log, cfg.Namespace, cfg.ProxyDeployment).Run(ctx)
	go kube.NewSafekeeperRegistrar(client, storcon, log, cfg.Namespace).Run(ctx)

	serve := &http.Server{
		Addr:              cfg.Listen,
		Handler:           server.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}

	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_ = serve.Shutdown(shutdown)
	}()

	log.Info("serving", "address", cfg.Listen, "namespace", cfg.Namespace)
	if err := serve.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// Supplied rather than generated: every storage component validates against its public half, so it
// must exist before any of them start. Absent, the storage layer has to be running with auth off.
func storageAuthKey(cfg *config.Config) (*neon.StorageKey, error) {
	if cfg.AuthKey == "" {
		return nil, nil
	}
	return neon.NewStorageKey([]byte(cfg.AuthKey))
}

// The key outlives the process because a compute only ever trusts the key set it was served: a
// fresh key on every restart would lock this service out of every compute already running.
func signingKey(ctx context.Context, store *registry.Store) (*neon.SigningKey, error) {
	seed, err := store.Meta(ctx, "compute-signing-seed")
	if err != nil {
		return nil, err
	}
	if seed == nil {
		if seed, err = neon.NewSeed(); err != nil {
			return nil, err
		}
		if err := store.PutMeta(ctx, "compute-signing-seed", seed); err != nil {
			return nil, err
		}
	}
	return neon.NewSigningKey(seed)
}

// Writes the files itself because the image has no shell to redirect with, and writes them world
// readable: the destination is a per-pod emptyDir, already inside one trust boundary.
func derive(args []string) error {
	flags := flag.NewFlagSet("neon-ctl derive", flag.ContinueOnError)
	dir := flags.String("dir", "", "directory to write into")
	scopes := flags.String("scopes", "", "comma-separated scopes to mint a token for, each written as <scope>.jwt")
	keyPEM := flags.String("auth-key", os.Getenv("NEON_CTL_AUTH_KEY"), "ed25519 private key as PKCS#8 PEM")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *keyPEM == "" || *dir == "" {
		return errors.New("auth-key and dir are required")
	}

	key, err := neon.NewStorageKey([]byte(*keyPEM))
	if err != nil {
		return err
	}
	public, err := key.PublicKeyPEM()
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(*dir, "public.pem"), public, 0o444); err != nil {
		return err
	}
	for _, scope := range strings.Split(*scopes, ",") {
		if scope = strings.TrimSpace(scope); scope == "" {
			continue
		}
		token, err := key.Token(neon.StorageClaims{Scope: neon.StorageScope(scope)})
		if err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(*dir, scope+".jwt"), []byte(token), 0o444); err != nil {
			return err
		}
	}
	return nil
}

func printToken(args []string) error {
	flags := flag.NewFlagSet("neon-ctl token", flag.ContinueOnError)
	scope := flags.String("scope", "", "admin, pageserverapi, safekeeperdata or tenant")
	tenant := flags.String("tenant", "", "tenant id, for a tenant-scoped token")
	keyPEM := flags.String("auth-key", os.Getenv("NEON_CTL_AUTH_KEY"), "ed25519 private key as PKCS#8 PEM")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *keyPEM == "" {
		return errors.New("auth-key is required (or set NEON_CTL_AUTH_KEY)")
	}

	key, err := neon.NewStorageKey([]byte(*keyPEM))
	if err != nil {
		return err
	}
	claims := neon.StorageClaims{Scope: neon.StorageScope(*scope)}
	if *tenant != "" {
		parsed, err := neon.ParseTenantID(*tenant)
		if err != nil {
			return err
		}
		claims.TenantID = &parsed
	}
	token, err := key.Token(claims)
	if err != nil {
		return err
	}
	fmt.Println(token)
	return nil
}

func kubernetesClient() (kubernetes.Interface, error) {
	restConfig, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("kubernetes configuration: %w", err)
	}
	return kubernetes.NewForConfig(restConfig)
}
