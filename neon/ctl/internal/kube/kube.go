// Package kube is everything this service does with the Kubernetes API: it runs computes, restarts
// the proxy for a renewed certificate, and registers safekeepers from the Services describing them.
package kube

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/hzhou0/homelab/neon/ctl/internal/neon"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/yaml"
)

var ErrNotFound = errors.New("kube: compute not found")

// Binding is the join the storage layer cannot make. It is held on the runtime object itself
// rather than in the registry, so the spec path depends on nothing that can pause.
type Binding struct {
	ID         string
	TenantID   neon.TenantID
	TimelineID neon.TimelineID
	Mode       neon.ComputeMode
}

type Instance struct {
	Binding

	// ControlURL addresses compute_ctl; PgAddress is what a client connects Postgres to.
	ControlURL string
	PgAddress  string

	Replicas int32
	Ready    bool
}

func (i *Instance) Running() bool { return i.Replicas > 0 && i.Ready }

// Runtime is an interface so the hook and spec paths can be exercised without a cluster.
type Runtime interface {
	PgVersions() []int

	Get(ctx context.Context, id string) (*Instance, error)
	List(ctx context.Context) ([]Instance, error)
	ListByTenant(ctx context.Context, tenant neon.TenantID) ([]Instance, error)
	ListByTimeline(ctx context.Context, tenant neon.TenantID, timeline neon.TimelineID) ([]Instance, error)

	// A binding whose timeline changed replaces the pod: the timeline GUCs are start-only, and a
	// spec that changes them is accepted and silently ignored.
	Ensure(ctx context.Context, binding Binding, pgVersion int) (*Instance, error)

	Scale(ctx context.Context, id string, replicas int32) error
	Delete(ctx context.Context, id string) error
}

const (
	labelComputeID  = "neon.internal/compute-id"
	labelTenantID   = "neon.internal/tenant-id"
	labelTimelineID = "neon.internal/timeline-id"
	labelPgVersion  = "neon.internal/pg-version"

	// Mode is an annotation because a static compute records the LSN it is pinned at, and an LSN
	// is not a legal label value.
	annotationMode    = "neon.internal/mode"
	annotationModeLSN = "neon.internal/mode-lsn"

	namePrefix     = "compute-"
	portPostgres   = "postgres"
	portHTTP       = "http"
	managedByLabel = "app.kubernetes.io/managed-by"
	managedBy      = "neon-ctl"
)

type ComputeOptions struct {
	Namespace       string
	PodTemplatePath string

	// Images is the whole statement of what this deployment can run: a branch may be created at
	// any version present, and one that asks for none gets the newest.
	Images map[int]string
}

// Computes renders one template the chart supplies, naming a different image per Postgres major
// version: the storage layer records a version per timeline and serves any of them at once.
type Computes struct {
	client   kubernetes.Interface
	opts     ComputeOptions
	template corev1.PodTemplateSpec
	pgPort   int32
	httpPort int32
}

func NewComputes(client kubernetes.Interface, opts ComputeOptions) (*Computes, error) {
	template, err := loadPodTemplate(opts.PodTemplatePath)
	if err != nil {
		return nil, err
	}
	pgPort, err := templatePort(template, portPostgres)
	if err != nil {
		return nil, err
	}
	httpPort, err := templatePort(template, portHTTP)
	if err != nil {
		return nil, err
	}

	return &Computes{
		client:   client,
		opts:     opts,
		template: template,
		pgPort:   pgPort,
		httpPort: httpPort,
	}, nil
}

// The template declares the ports its container listens on, so they are read from it rather than
// configured twice and left to disagree.
func templatePort(template corev1.PodTemplateSpec, name string) (int32, error) {
	for _, container := range template.Spec.Containers {
		for _, port := range container.Ports {
			if port.Name == name {
				return port.ContainerPort, nil
			}
		}
	}
	return 0, fmt.Errorf("kube: pod template declares no port named %q", name)
}

func loadPodTemplate(path string) (corev1.PodTemplateSpec, error) {
	var template corev1.PodTemplateSpec
	raw, err := os.ReadFile(path)
	if err != nil {
		return template, fmt.Errorf("kube: reading pod template: %w", err)
	}
	if err := yaml.UnmarshalStrict(raw, &template); err != nil {
		return template, fmt.Errorf("kube: decoding pod template: %w", err)
	}
	return template, nil
}

func (r *Computes) PgVersions() []int {
	versions := make([]int, 0, len(r.opts.Images))
	for version := range r.opts.Images {
		versions = append(versions, version)
	}
	sort.Ints(versions)
	return versions
}

func (r *Computes) name(id string) string { return namePrefix + id }

func (r *Computes) selector(id string) map[string]string {
	return map[string]string{
		managedByLabel: managedBy,
		labelComputeID: id,
	}
}

func (r *Computes) Get(ctx context.Context, id string) (*Instance, error) {
	deployment, err := r.client.AppsV1().Deployments(r.opts.Namespace).Get(ctx, r.name(id), metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil, fmt.Errorf("%w: %s", ErrNotFound, id)
	}
	if err != nil {
		return nil, fmt.Errorf("kube: getting compute %s: %w", id, err)
	}
	return r.instance(deployment)
}

func (r *Computes) List(ctx context.Context) ([]Instance, error) {
	return r.list(ctx, managedByLabel+"="+managedBy)
}

func (r *Computes) ListByTenant(ctx context.Context, tenant neon.TenantID) ([]Instance, error) {
	return r.list(ctx, fmt.Sprintf("%s=%s,%s=%s", managedByLabel, managedBy, labelTenantID, tenant))
}

func (r *Computes) ListByTimeline(ctx context.Context, tenant neon.TenantID, timeline neon.TimelineID) ([]Instance, error) {
	return r.list(ctx, fmt.Sprintf("%s=%s,%s=%s,%s=%s",
		managedByLabel, managedBy, labelTenantID, tenant, labelTimelineID, timeline))
}

func (r *Computes) list(ctx context.Context, selector string) ([]Instance, error) {
	deployments, err := r.client.AppsV1().Deployments(r.opts.Namespace).List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return nil, fmt.Errorf("kube: listing computes: %w", err)
	}
	instances := make([]Instance, 0, len(deployments.Items))
	for i := range deployments.Items {
		instance, err := r.instance(&deployments.Items[i])
		if err != nil {
			return nil, err
		}
		instances = append(instances, *instance)
	}
	return instances, nil
}

func (r *Computes) instance(deployment *appsv1.Deployment) (*Instance, error) {
	labels := deployment.Spec.Template.Labels
	tenant, err := neon.ParseTenantID(labels[labelTenantID])
	if err != nil {
		return nil, fmt.Errorf("kube: compute %s carries an unusable tenant label: %w", deployment.Name, err)
	}
	timeline, err := neon.ParseTimelineID(labels[labelTimelineID])
	if err != nil {
		return nil, fmt.Errorf("kube: compute %s carries an unusable timeline label: %w", deployment.Name, err)
	}

	mode := neon.ComputeMode{Kind: neon.ComputeModeKind(deployment.Spec.Template.Annotations[annotationMode])}
	if mode.Kind == "" {
		mode.Kind = neon.ModePrimary
	}
	if raw := deployment.Spec.Template.Annotations[annotationModeLSN]; raw != "" {
		lsn, err := neon.ParseLSN(raw)
		if err != nil {
			return nil, fmt.Errorf("kube: compute %s carries an unusable lsn annotation: %w", deployment.Name, err)
		}
		mode.LSN = lsn
	}

	id := labels[labelComputeID]
	host := fmt.Sprintf("%s.%s", r.name(id), r.opts.Namespace)

	var replicas int32
	if deployment.Spec.Replicas != nil {
		replicas = *deployment.Spec.Replicas
	}

	return &Instance{
		Binding: Binding{
			ID:         id,
			TenantID:   tenant,
			TimelineID: timeline,
			Mode:       mode,
		},
		ControlURL: fmt.Sprintf("http://%s:%d", host, r.httpPort),
		PgAddress:  fmt.Sprintf("%s:%d", host, r.pgPort),
		Replicas:   replicas,
		Ready:      deployment.Status.ReadyReplicas > 0,
	}, nil
}

func (r *Computes) Ensure(ctx context.Context, binding Binding, pgVersion int) (*Instance, error) {
	desired, err := r.deployment(binding, pgVersion)
	if err != nil {
		return nil, err
	}
	deployments := r.client.AppsV1().Deployments(r.opts.Namespace)

	// Read-modify-write against a live object: the deployment controller writes status while this
	// runs, and a wake racing a suspend loses outright without the retry.
	err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
		existing, err := deployments.Get(ctx, desired.Name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			_, err = deployments.Create(ctx, desired, metav1.CreateOptions{})
			return err
		}
		if err != nil {
			return err
		}
		existing.Spec = desired.Spec
		existing.Labels = desired.Labels
		_, err = deployments.Update(ctx, existing, metav1.UpdateOptions{})
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("kube: ensuring compute %s: %w", binding.ID, err)
	}

	if err := r.ensureService(ctx, binding); err != nil {
		return nil, err
	}
	return r.Get(ctx, binding.ID)
}

func (r *Computes) Scale(ctx context.Context, id string, replicas int32) error {
	deployments := r.client.AppsV1().Deployments(r.opts.Namespace)
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		deployment, err := deployments.Get(ctx, r.name(id), metav1.GetOptions{})
		if err != nil {
			return err
		}
		deployment.Spec.Replicas = &replicas
		_, err = deployments.Update(ctx, deployment, metav1.UpdateOptions{})
		return err
	})
	if apierrors.IsNotFound(err) {
		return fmt.Errorf("%w: %s", ErrNotFound, id)
	}
	if err != nil {
		return fmt.Errorf("kube: scaling compute %s: %w", id, err)
	}
	return nil
}

func (r *Computes) Delete(ctx context.Context, id string) error {
	name := r.name(id)
	err := r.client.AppsV1().Deployments(r.opts.Namespace).Delete(ctx, name, metav1.DeleteOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("kube: deleting compute %s: %w", id, err)
	}
	err = r.client.CoreV1().Services(r.opts.Namespace).Delete(ctx, name, metav1.DeleteOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("kube: deleting compute service %s: %w", id, err)
	}
	return nil
}

func (r *Computes) deployment(binding Binding, pgVersion int) (*appsv1.Deployment, error) {
	image, known := r.opts.Images[pgVersion]
	if !known {
		return nil, fmt.Errorf("kube: no compute image for postgres %d; this deployment has %v", pgVersion, r.PgVersions())
	}
	template := *r.template.DeepCopy()

	template.Labels = merge(template.Labels, r.selector(binding.ID), map[string]string{
		labelTenantID:   binding.TenantID.String(),
		labelTimelineID: binding.TimelineID.String(),
		labelPgVersion:  strconv.Itoa(pgVersion),
	})
	annotations := map[string]string{annotationMode: string(binding.Mode.Kind)}
	if binding.Mode.Kind == neon.ModeStatic {
		annotations[annotationModeLSN] = binding.Mode.LSN.String()
	}
	template.Annotations = merge(template.Annotations, annotations)

	expand := r.expander(binding, image)
	for i := range template.Spec.InitContainers {
		expandContainer(&template.Spec.InitContainers[i], expand)
	}
	for i := range template.Spec.Containers {
		expandContainer(&template.Spec.Containers[i], expand)
	}

	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      r.name(binding.ID),
			Namespace: r.opts.Namespace,
			Labels:    template.Labels,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: ptr[int32](1),
			Selector: &metav1.LabelSelector{MatchLabels: r.selector(binding.ID)},
			// One postmaster per timeline: a rolling update would briefly run two.
			Strategy: appsv1.DeploymentStrategy{Type: appsv1.RecreateDeploymentStrategyType},
			Template: template,
		},
	}, nil
}

func (r *Computes) ensureService(ctx context.Context, binding Binding) error {
	name := r.name(binding.ID)
	desired := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: r.opts.Namespace,
			Labels:    r.selector(binding.ID),
		},
		Spec: corev1.ServiceSpec{
			Type:     corev1.ServiceTypeClusterIP,
			Selector: r.selector(binding.ID),
			Ports: []corev1.ServicePort{
				{Name: portPostgres, Port: r.pgPort, TargetPort: intstr.FromInt32(r.pgPort)},
				{Name: portHTTP, Port: r.httpPort, TargetPort: intstr.FromInt32(r.httpPort)},
			},
		},
	}

	services := r.client.CoreV1().Services(r.opts.Namespace)
	existing, err := services.Get(ctx, name, metav1.GetOptions{})
	switch {
	case apierrors.IsNotFound(err):
		if _, err := services.Create(ctx, desired, metav1.CreateOptions{}); err != nil {
			return fmt.Errorf("kube: creating compute service %s: %w", binding.ID, err)
		}
		return nil
	case err != nil:
		return fmt.Errorf("kube: getting compute service %s: %w", binding.ID, err)
	}

	existing.Labels = desired.Labels
	existing.Spec.Selector = desired.Spec.Selector
	existing.Spec.Ports = desired.Spec.Ports
	if _, err := services.Update(ctx, existing, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("kube: updating compute service %s: %w", binding.ID, err)
	}
	return nil
}

// These two are the only things that differ between one compute and another; everything else the
// chart can write literally.
func (r *Computes) expander(binding Binding, image string) func(string) string {
	values := map[string]string{
		"COMPUTE_ID":    binding.ID,
		"COMPUTE_IMAGE": image,
	}
	// An unrecognised placeholder is left as it stands rather than blanked, so a template written
	// against a different contract fails visibly instead of launching with an empty flag.
	return func(key string) string {
		if value, known := values[key]; known {
			return value
		}
		return "${" + key + "}"
	}
}

func expandContainer(container *corev1.Container, expand func(string) string) {
	container.Image = os.Expand(container.Image, expand)
	for i, arg := range container.Args {
		container.Args[i] = os.Expand(arg, expand)
	}
	for i, arg := range container.Command {
		container.Command[i] = os.Expand(arg, expand)
	}
	for i := range container.Env {
		if container.Env[i].ValueFrom == nil {
			container.Env[i].Value = os.Expand(container.Env[i].Value, expand)
		}
	}
}

func merge(maps ...map[string]string) map[string]string {
	merged := map[string]string{}
	for _, m := range maps {
		for key, value := range m {
			if strings.TrimSpace(value) == "" {
				continue
			}
			merged[key] = value
		}
	}
	return merged
}

// Neon's proxy reads its certificate once at startup and has no reload path, so a renewal reaches
// it only through a restart. Which secrets matter is read from the volumes it already mounts.
type CertRoller struct {
	client     kubernetes.Interface
	namespace  string
	deployment string
	log        *slog.Logger
}

const (
	annotationSecretVersions = "neon.internal/mounted-secret-versions"
	certPollInterval         = 5 * time.Minute
)

func NewCertRoller(client kubernetes.Interface, log *slog.Logger, namespace, deployment string) *CertRoller {
	return &CertRoller{client: client, namespace: namespace, deployment: deployment, log: log}
}

// A certificate is renewed weeks before it expires, so polling costs nothing that matters and
// avoids a watch that has to be reconnected.
func (c *CertRoller) Run(ctx context.Context) {
	if c.deployment == "" {
		return
	}
	ticker := time.NewTicker(certPollInterval)
	defer ticker.Stop()

	for {
		if err := c.roll(ctx); err != nil && ctx.Err() == nil {
			c.log.Error("rolling deployment for renewed secrets",
				"deployment", c.deployment, "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// Stamping versions rather than a timestamp makes the patch idempotent, so a tick with nothing
// renewed restarts nothing.
func (c *CertRoller) roll(ctx context.Context) error {
	deployments := c.client.AppsV1().Deployments(c.namespace)
	deployment, err := deployments.Get(ctx, c.deployment, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		c.log.Warn("no deployment to roll", "deployment", c.deployment)
		return nil
	}
	if err != nil {
		return err
	}

	var names []string
	for _, volume := range deployment.Spec.Template.Spec.Volumes {
		if volume.Secret != nil {
			names = append(names, volume.Secret.SecretName)
		}
	}
	if len(names) == 0 {
		return nil
	}
	sort.Strings(names)

	versions := make([]string, 0, len(names))
	for _, name := range names {
		secret, err := c.client.CoreV1().Secrets(c.namespace).Get(ctx, name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			continue
		}
		if err != nil {
			return err
		}
		versions = append(versions, name+"="+secret.ResourceVersion)
	}
	stamp := strings.Join(versions, ",")
	if stamp == "" || deployment.Spec.Template.Annotations[annotationSecretVersions] == stamp {
		return nil
	}

	patch, err := json.Marshal(map[string]any{
		"spec": map[string]any{
			"template": map[string]any{
				"metadata": map[string]any{
					"annotations": map[string]string{annotationSecretVersions: stamp},
				},
			},
		},
	})
	if err != nil {
		return err
	}
	if _, err := deployments.Patch(ctx, c.deployment, types.StrategicMergePatchType, patch, metav1.PatchOptions{}); err != nil {
		return fmt.Errorf("kube: rolling %s: %w", c.deployment, err)
	}
	c.log.Info("rolled deployment for renewed secrets", "deployment", c.deployment, "secrets", stamp)
	return nil
}

func ptr[T any](value T) *T { return &value }

// A safekeeper has no notion of the controller and the controller cannot discover one, so
// something must assert it exists. Reading the cluster makes that a fact rather than an event.
type SafekeeperRegistrar struct {
	client    kubernetes.Interface
	storcon   *neon.StorageController
	namespace string
	log       *slog.Logger
}

const (
	labelSafekeeperID     = "neon.internal/safekeeper-id"
	labelAvailabilityZone = "neon.internal/availability-zone"
	labelAppVersion       = "app.kubernetes.io/version"

	// Bounds how long a newly added safekeeper stays unusable, and no timeline can be created on
	// one the controller has not heard of.
	safekeeperPollInterval = 10 * time.Second

	// What upstream records for a safekeeper it has not yet posted.
	safekeeperVersionUnknown = 1

	// Required by the upsert, then recorded and handed back unread. There is one deployment and
	// one site, so there is nothing for a value here to vary with.
	safekeeperRegion = "neon"
)

func NewSafekeeperRegistrar(client kubernetes.Interface, storcon *neon.StorageController, log *slog.Logger, namespace string) *SafekeeperRegistrar {
	return &SafekeeperRegistrar{client: client, storcon: storcon, namespace: namespace, log: log}
}

func (r *SafekeeperRegistrar) Run(ctx context.Context) {
	ticker := time.NewTicker(safekeeperPollInterval)
	defer ticker.Stop()

	for {
		if err := r.register(ctx); err != nil && ctx.Err() == nil {
			r.log.Error("registering safekeepers", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// One failure must not hide the others: a controller that rejects one record has nothing to say
// about the rest, and the next tick retries them all anyway.
func (r *SafekeeperRegistrar) register(ctx context.Context) error {
	found, err := r.discover(ctx)
	if err != nil {
		return err
	}
	var failures []error
	for _, safekeeper := range found {
		if err := r.storcon.UpsertSafekeeper(ctx, safekeeper); err != nil {
			failures = append(failures, fmt.Errorf("safekeeper %d: %w", safekeeper.ID, err))
		}
	}
	return errors.Join(failures...)
}

// Everything the controller wants is already on the Service, which is also what its host resolves
// to. Restating it as configuration would be a second copy free to disagree with the first.
func (r *SafekeeperRegistrar) discover(ctx context.Context) ([]neon.SafekeeperUpsert, error) {
	services, err := r.client.CoreV1().Services(r.namespace).List(ctx, metav1.ListOptions{LabelSelector: labelSafekeeperID})
	if err != nil {
		return nil, err
	}

	found := make([]neon.SafekeeperUpsert, 0, len(services.Items))
	for i := range services.Items {
		service := &services.Items[i]
		id, err := strconv.ParseUint(service.Labels[labelSafekeeperID], 10, 64)
		if err != nil || id == 0 {
			r.log.Error("skipping a safekeeper service with an unusable id",
				"service", service.Name, "id", service.Labels[labelSafekeeperID])
			continue
		}
		pgPort, httpPort := servicePort(service, "pg"), servicePort(service, "http")
		if pgPort == 0 || httpPort == 0 {
			r.log.Error("skipping a safekeeper service missing a named port", "service", service.Name)
			continue
		}
		zone := service.Labels[labelAvailabilityZone]
		if zone == "" {
			zone = service.Name
		}
		found = append(found, neon.SafekeeperUpsert{
			ID:                 neon.NodeID(id),
			RegionID:           safekeeperRegion,
			Version:            safekeeperVersion(service.Labels[labelAppVersion]),
			Host:               fmt.Sprintf("%s.%s.svc.cluster.local", service.Name, r.namespace),
			Port:               pgPort,
			HTTPPort:           httpPort,
			AvailabilityZoneID: zone,
		})
	}
	return found, nil
}

func servicePort(service *corev1.Service, name string) int32 {
	for _, port := range service.Spec.Ports {
		if port.Name == name {
			return port.Port
		}
	}
	return 0
}

// Neon builds this from the commit count on the release branch, which is what the image tag of a
// released build already is.
func safekeeperVersion(tag string) int64 {
	version, err := strconv.ParseInt(tag, 10, 64)
	if err != nil || version < 0 {
		return safekeeperVersionUnknown
	}
	return version
}
