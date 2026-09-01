package kube

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"

	"github.com/hzhou0/homelab/neon/ctl/internal/neon"
)

const podTemplate = `
metadata:
  labels:
    app.kubernetes.io/name: neon-compute
spec:
  containers:
    - name: compute
      image: ${COMPUTE_IMAGE}
      ports:
        - name: postgres
          containerPort: 55433
        - name: http
          containerPort: 3080
      args:
        - --compute-id=${COMPUTE_ID}
        - --control-plane-uri=http://neon-ctl.neon:8080
        - --connstr=postgresql://cloud_admin@localhost:55433/postgres
      env:
        - name: PGDATA
          value: /var/db/pgdata
        - name: FROM_SECRET
          valueFrom:
            secretKeyRef:
              name: bucket
              key: accessKeyId
`

var testImages = map[int]string{
	16: "neondatabase/compute-node-v16:8464",
	17: "neondatabase/compute-node-v17:8464",
}

func newTestComputes(t *testing.T) (*Computes, kubernetes.Interface, string) {
	t.Helper()
	client, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		t.Fatal(err)
	}
	namespace := newNamespace(t, client)

	path := filepath.Join(t.TempDir(), "pod.yaml")
	if err := os.WriteFile(path, []byte(podTemplate), 0o600); err != nil {
		t.Fatal(err)
	}
	computes, err := NewComputes(client, ComputeOptions{
		Namespace:       namespace,
		PodTemplatePath: path,
		Images:          testImages,
	})
	if err != nil {
		t.Fatal(err)
	}
	return computes, client, namespace
}

func testBinding(t *testing.T) Binding {
	t.Helper()
	tenant, err := neon.ParseTenantID("1a2b3344556677881122334455667788")
	if err != nil {
		t.Fatal(err)
	}
	timeline, err := neon.ParseTimelineID("aa223344556677881122334455667788")
	if err != nil {
		t.Fatal(err)
	}
	return Binding{
		ID:         "main",
		TenantID:   tenant,
		TimelineID: timeline,
		Mode:       neon.ComputeMode{Kind: neon.ModePrimary},
	}
}

func TestEnsureRendersFromTheTemplate(t *testing.T) {
	computes, client, namespace := newTestComputes(t)
	ctx := context.Background()
	binding := testBinding(t)

	if _, err := computes.Ensure(ctx, binding, 17); err != nil {
		t.Fatal(err)
	}

	deployment, err := client.AppsV1().Deployments(namespace).Get(ctx, "compute-main", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}

	container := deployment.Spec.Template.Spec.Containers[0]
	if container.Args[0] != "--compute-id=main" {
		t.Errorf("arg = %q, want the compute id substituted", container.Args[0])
	}
	if container.Image != "neondatabase/compute-node-v17:8464" {
		t.Errorf("image = %q", container.Image)
	}
	if container.Env[0].Value != "/var/db/pgdata" {
		t.Errorf("literal env = %q", container.Env[0].Value)
	}
	// A value sourced from a secret has no literal to expand, and clobbering it would silently
	// drop the reference.
	if container.Env[1].ValueFrom == nil || container.Env[1].Value != "" {
		t.Errorf("env from a secret was rewritten: %+v", container.Env[1])
	}

	// Two postmasters on one timeline is the failure a rolling update would cause.
	if deployment.Spec.Strategy.Type != appsv1.RecreateDeploymentStrategyType {
		t.Errorf("strategy = %v", deployment.Spec.Strategy.Type)
	}

	labels := deployment.Spec.Template.Labels
	if labels[labelTenantID] != binding.TenantID.String() ||
		labels[labelTimelineID] != binding.TimelineID.String() ||
		labels[labelPgVersion] != "17" {
		t.Errorf("binding labels = %v", labels)
	}
	if labels["app.kubernetes.io/name"] != "neon-compute" {
		t.Error("the template's own labels were dropped")
	}

	if _, err := client.CoreV1().Services(namespace).Get(ctx, "compute-main", metav1.GetOptions{}); err != nil {
		t.Errorf("no service was created: %v", err)
	}
}

// The selector is immutable, so it must carry only what never changes. The timeline lives on the
// pod template instead, which is also what makes a re-bound compute roll its pod.
func TestSelectorExcludesTheBinding(t *testing.T) {
	computes, client, namespace := newTestComputes(t)
	ctx := context.Background()
	binding := testBinding(t)

	if _, err := computes.Ensure(ctx, binding, 17); err != nil {
		t.Fatal(err)
	}
	deployment, err := client.AppsV1().Deployments(namespace).Get(ctx, "compute-main", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{labelTenantID, labelTimelineID, labelPgVersion} {
		if _, present := deployment.Spec.Selector.MatchLabels[key]; present {
			t.Errorf("selector contains %q, which changes when a branch is re-pointed", key)
		}
	}

	rebound := binding
	rebound.TimelineID, _ = neon.ParseTimelineID("bb223344556677881122334455667788")
	if _, err := computes.Ensure(ctx, rebound, 17); err != nil {
		t.Fatal(err)
	}
	deployment, err = client.AppsV1().Deployments(namespace).Get(ctx, "compute-main", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if deployment.Spec.Template.Labels[labelTimelineID] != rebound.TimelineID.String() {
		t.Error("re-pointing a branch did not change the pod template, so no pod would be replaced")
	}

	// The premise the split rests on. If a selector ever became mutable, the binding could live
	// on it and this whole arrangement would be unnecessary.
	deployment.Spec.Selector.MatchLabels[labelTimelineID] = rebound.TimelineID.String()
	if _, err := client.AppsV1().Deployments(namespace).Update(ctx, deployment, metav1.UpdateOptions{}); err == nil {
		t.Error("the apiserver accepted a selector change")
	}
}

func TestGetReadsTheBindingBack(t *testing.T) {
	computes, _, namespace := newTestComputes(t)
	ctx := context.Background()
	binding := testBinding(t)

	if _, err := computes.Ensure(ctx, binding, 17); err != nil {
		t.Fatal(err)
	}
	instance, err := computes.Get(ctx, "main")
	if err != nil {
		t.Fatal(err)
	}
	if instance.Binding != binding {
		t.Errorf("binding = %+v, want %+v", instance.Binding, binding)
	}
	if want := "http://compute-main." + namespace + ":3080"; instance.ControlURL != want {
		t.Errorf("control url = %q, want %q", instance.ControlURL, want)
	}
	if want := "compute-main." + namespace + ":55433"; instance.PgAddress != want {
		t.Errorf("pg address = %q, want %q", instance.PgAddress, want)
	}
	if instance.Replicas != 1 {
		t.Errorf("replicas = %d", instance.Replicas)
	}
}

func TestStaticModeSurvivesRoundTrip(t *testing.T) {
	computes, _, _ := newTestComputes(t)
	ctx := context.Background()
	binding := testBinding(t)
	binding.Mode = neon.ComputeMode{Kind: neon.ModeStatic, LSN: 0x16B374D848}

	if _, err := computes.Ensure(ctx, binding, 17); err != nil {
		t.Fatal(err)
	}
	instance, err := computes.Get(ctx, "main")
	if err != nil {
		t.Fatal(err)
	}
	// An LSN is not a legal label value, which is why the mode is annotated rather than labelled.
	if instance.Mode != binding.Mode {
		t.Errorf("mode = %+v, want %+v", instance.Mode, binding.Mode)
	}
}

func TestScaleAndDelete(t *testing.T) {
	computes, client, namespace := newTestComputes(t)
	ctx := context.Background()

	if _, err := computes.Ensure(ctx, testBinding(t), 17); err != nil {
		t.Fatal(err)
	}
	if err := computes.Scale(ctx, "main", 0); err != nil {
		t.Fatal(err)
	}
	instance, err := computes.Get(ctx, "main")
	if err != nil {
		t.Fatal(err)
	}
	if instance.Replicas != 0 || instance.Running() {
		t.Errorf("instance after scaling to zero = %+v", instance)
	}

	if err := computes.Delete(ctx, "main"); err != nil {
		t.Fatal(err)
	}
	if _, err := computes.Get(ctx, "main"); err == nil {
		t.Error("the deployment survived deletion")
	}
	if _, err := client.CoreV1().Services(namespace).Get(ctx, "compute-main", metav1.GetOptions{}); err == nil {
		t.Error("the service survived deletion")
	}
	// Deleting what is already gone is how a failed delete is retried.
	if err := computes.Delete(ctx, "main"); err != nil {
		t.Errorf("second Delete = %v, want nil", err)
	}
}

func TestListsAreScopedToTheBinding(t *testing.T) {
	computes, _, _ := newTestComputes(t)
	ctx := context.Background()

	first := testBinding(t)
	second := testBinding(t)
	second.ID = "feature"
	second.TimelineID, _ = neon.ParseTimelineID("bb223344556677881122334455667788")

	for _, binding := range []Binding{first, second} {
		if _, err := computes.Ensure(ctx, binding, 17); err != nil {
			t.Fatal(err)
		}
	}

	all, err := computes.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 2 {
		t.Fatalf("List returned %d computes", len(all))
	}

	byTenant, err := computes.ListByTenant(ctx, first.TenantID)
	if err != nil {
		t.Fatal(err)
	}
	if len(byTenant) != 2 {
		t.Errorf("ListByTenant returned %d, want both: attachment is per tenant", len(byTenant))
	}

	byTimeline, err := computes.ListByTimeline(ctx, first.TenantID, first.TimelineID)
	if err != nil {
		t.Fatal(err)
	}
	if len(byTimeline) != 1 || byTimeline[0].ID != "main" {
		t.Errorf("ListByTimeline returned %+v, want only main: membership is per timeline", byTimeline)
	}
}

func TestNewComputesRejectsAnUnusableConfiguration(t *testing.T) {
	client, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		t.Fatal(err)
	}
	const namespace = "default"
	images := map[int]string{17: "neondatabase/compute-node-v17:8464"}

	write := func(t *testing.T, body string) string {
		t.Helper()
		path := filepath.Join(t.TempDir(), "pod.yaml")
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		return path
	}

	if _, err := NewComputes(client, ComputeOptions{Namespace: namespace, PodTemplatePath: write(t, "spec: {}\n"), Images: images}); err == nil {
		t.Error("a template with no containers was accepted")
	}
	// The ports are read from the template, so a template that does not declare them has to fail
	// at startup rather than produce a Service pointing nowhere.
	noPorts := "spec:\n  containers:\n    - name: compute\n      image: x\n"
	if _, err := NewComputes(client, ComputeOptions{Namespace: namespace, PodTemplatePath: write(t, noPorts), Images: images}); err == nil {
		t.Error("a template declaring no ports was accepted")
	}
	if _, err := NewComputes(client, ComputeOptions{Namespace: namespace, PodTemplatePath: "/nonexistent", Images: images}); err == nil {
		t.Error("a missing template was accepted")
	}
}

// The storage layer records a version per timeline and serves any of them at once, so what a
// deployment can run is exactly the set of images it was given.
func TestEachVersionGetsItsOwnImage(t *testing.T) {
	computes, client, namespace := newTestComputes(t)
	ctx := context.Background()

	if want := []int{16, 17}; fmt.Sprint(computes.PgVersions()) != fmt.Sprint(want) {
		t.Errorf("PgVersions() = %v, want %v", computes.PgVersions(), want)
	}

	older := testBinding(t)
	older.ID = "legacy"
	if _, err := computes.Ensure(ctx, older, 16); err != nil {
		t.Fatal(err)
	}
	if _, err := computes.Ensure(ctx, testBinding(t), 17); err != nil {
		t.Fatal(err)
	}

	for name, want := range map[string]string{
		"compute-legacy": "neondatabase/compute-node-v16:8464",
		"compute-main":   "neondatabase/compute-node-v17:8464",
	} {
		deployment, err := client.AppsV1().Deployments(namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if got := deployment.Spec.Template.Spec.Containers[0].Image; got != want {
			t.Errorf("%s image = %q, want %q", name, got, want)
		}
	}
}

// A branch asking for a version this deployment has no image for must fail where it is created,
// not as a pod that cannot be scheduled.
func TestEnsureRefusesAnUnavailableVersion(t *testing.T) {
	computes, _, _ := newTestComputes(t)

	if _, err := computes.Ensure(context.Background(), testBinding(t), 15); err == nil {
		t.Error("a compute was created for a version with no image")
	}
}

// Only the two per-compute values are substituted. Anything else is a template written against a
// different contract, and must stay visible rather than becoming an empty flag.
func TestUnknownPlaceholdersAreLeftAlone(t *testing.T) {
	computes, client, namespace := newTestComputes(t)
	ctx := context.Background()

	computes.template.Spec.Containers[0].Args = []string{"--compute-id=${COMPUTE_ID}", "--old=${CONTROL_PLANE_URL}"}
	if _, err := computes.Ensure(ctx, testBinding(t), 17); err != nil {
		t.Fatal(err)
	}

	deployment, err := client.AppsV1().Deployments(namespace).Get(ctx, "compute-main", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	args := deployment.Spec.Template.Spec.Containers[0].Args
	if args[0] != "--compute-id=main" {
		t.Errorf("arg = %q", args[0])
	}
	if args[1] != "--old=${CONTROL_PLANE_URL}" {
		t.Errorf("arg = %q, want the placeholder left intact", args[1])
	}
}

// The deployment's own volumes are the record of which secrets it depends on, so a renewal is
// noticed without anything naming the certificate.
func TestCertRollerFindsSecretsThroughTheDeployment(t *testing.T) {
	ctx := context.Background()
	client, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		t.Fatal(err)
	}
	namespace := newNamespace(t, client)

	labels := map[string]string{"app": "neon-proxy"}
	_, err = client.AppsV1().Deployments(namespace).Create(ctx, &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "neon-proxy"},
		Spec: appsv1.DeploymentSpec{
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: "proxy", Image: "neondatabase/neon:8464"}},
					Volumes: []corev1.Volume{{
						Name:         "tls",
						VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: "proxy-tls"}},
					}},
				},
			},
		},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	secret, err := client.CoreV1().Secrets(namespace).Create(ctx,
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "proxy-tls"}}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}

	roller := NewCertRoller(client, slog.New(slog.NewTextHandler(io.Discard, nil)), namespace, "neon-proxy")

	stamp := func(t *testing.T) string {
		t.Helper()
		if err := roller.roll(ctx); err != nil {
			t.Fatal(err)
		}
		deployment, err := client.AppsV1().Deployments(namespace).Get(ctx, "neon-proxy", metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		return deployment.Spec.Template.Annotations[annotationSecretVersions]
	}

	first := stamp(t)
	if want := "proxy-tls=" + secret.ResourceVersion; first != want {
		t.Fatalf("annotation = %q, want %q", first, want)
	}
	if again := stamp(t); again != first {
		t.Errorf("a tick with nothing renewed changed the annotation: %q", again)
	}

	// A real renewal is a write to the secret, and the version the apiserver assigns is what has
	// to reach the pod template.
	secret.Data = map[string][]byte{"tls.crt": []byte("renewed")}
	renewed, err := client.CoreV1().Secrets(namespace).Update(ctx, secret, metav1.UpdateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if want := "proxy-tls=" + renewed.ResourceVersion; stamp(t) != want {
		t.Errorf("the renewed version did not reach the pod template, want %q", want)
	}
}

func safekeeperService(name string, labels map[string]string, ports ...corev1.ServicePort) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: name, Labels: labels},
		Spec:       corev1.ServiceSpec{Ports: ports},
	}
}

func namedPorts() []corev1.ServicePort {
	return []corev1.ServicePort{
		{Name: "pg", Port: 5454, TargetPort: intstr.FromString("pg")},
		{Name: "http", Port: 7676, TargetPort: intstr.FromString("http")},
	}
}

// The Service is the whole record: what the controller is told about a safekeeper is derived from
// it rather than restated anywhere, so this is the test that the derivation is right.
func TestSafekeeperDiscoveryReadsTheService(t *testing.T) {
	ctx := context.Background()
	client, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		t.Fatal(err)
	}
	namespace := newNamespace(t, client)

	for _, service := range []*corev1.Service{
		safekeeperService("sk-1", map[string]string{
			labelSafekeeperID: "1",
			labelAppVersion:   "8464",
		}, namedPorts()...),
		safekeeperService("sk-2", map[string]string{
			labelSafekeeperID:     "2",
			labelAvailabilityZone: "rack-b",
			labelAppVersion:       "8464",
		}, namedPorts()...),
		// Not a safekeeper: no id label, so the selector must not return it.
		safekeeperService("proxy", map[string]string{"app.kubernetes.io/name": "neon"}, namedPorts()...),
	} {
		if _, err := client.CoreV1().Services(namespace).Create(ctx, service, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	}

	registrar := NewSafekeeperRegistrar(client, nil, slog.New(slog.NewTextHandler(io.Discard, nil)), namespace)
	found, err := registrar.discover(ctx)
	if err != nil {
		t.Fatal(err)
	}

	want := []neon.SafekeeperUpsert{
		{
			ID: 1, RegionID: safekeeperRegion, Version: 8464,
			Host: "sk-1." + namespace + ".svc.cluster.local",
			Port: 5454, HTTPPort: 7676,
			// Unstated, so it is its own zone: the controller places one member per zone, and a
			// shared one would cap every timeline at a single safekeeper.
			AvailabilityZoneID: "sk-1",
		},
		{
			ID: 2, RegionID: safekeeperRegion, Version: 8464,
			Host: "sk-2." + namespace + ".svc.cluster.local",
			Port: 5454, HTTPPort: 7676,
			AvailabilityZoneID: "rack-b",
		},
	}
	if !reflect.DeepEqual(found, want) {
		t.Errorf("discovered\n %+v\nwant\n %+v", found, want)
	}
}

// A malformed record is skipped rather than posted or fatal: the controller rejects it anyway, and
// one bad Service must not stop the others being registered.
func TestSafekeeperDiscoverySkipsUnusableServices(t *testing.T) {
	ctx := context.Background()
	client, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		t.Fatal(err)
	}
	namespace := newNamespace(t, client)

	for _, service := range []*corev1.Service{
		safekeeperService("sk-zero", map[string]string{labelSafekeeperID: "0"}, namedPorts()...),
		safekeeperService("sk-words", map[string]string{labelSafekeeperID: "one"}, namedPorts()...),
		safekeeperService("sk-unnamed-ports", map[string]string{labelSafekeeperID: "4"},
			corev1.ServicePort{Name: "pg", Port: 5454, TargetPort: intstr.FromString("pg")}),
		safekeeperService("sk-good", map[string]string{labelSafekeeperID: "5"}, namedPorts()...),
	} {
		if _, err := client.CoreV1().Services(namespace).Create(ctx, service, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	}

	registrar := NewSafekeeperRegistrar(client, nil, slog.New(slog.NewTextHandler(io.Discard, nil)), namespace)
	found, err := registrar.discover(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(found) != 1 || found[0].ID != 5 {
		t.Fatalf("discovered %+v, want only safekeeper 5", found)
	}
	// No version label at all: upstream's sentinel for a record it has not posted before.
	if found[0].Version != safekeeperVersionUnknown {
		t.Errorf("version = %d, want %d", found[0].Version, safekeeperVersionUnknown)
	}
}

// The controller answering one record badly says nothing about the rest, so every safekeeper is
// attempted and the failure is reported rather than swallowed.
func TestSafekeeperRegisterPostsEveryRecord(t *testing.T) {
	ctx := context.Background()
	client, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		t.Fatal(err)
	}
	namespace := newNamespace(t, client)

	for _, id := range []string{"1", "2", "3"} {
		service := safekeeperService("sk-"+id, map[string]string{labelSafekeeperID: id}, namedPorts()...)
		if _, err := client.CoreV1().Services(namespace).Create(ctx, service, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	}

	var (
		mu       sync.Mutex
		received []neon.SafekeeperUpsert
	)
	controller := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body neon.SafekeeperUpsert
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		mu.Lock()
		received = append(received, body)
		mu.Unlock()
		if body.ID == 2 {
			http.Error(w, "no", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer controller.Close()

	storcon, err := neon.NewStorageController(controller.URL, "", controller.Client())
	if err != nil {
		t.Fatal(err)
	}
	registrar := NewSafekeeperRegistrar(client, storcon, slog.New(slog.NewTextHandler(io.Discard, nil)), namespace)

	if err := registrar.register(ctx); err == nil {
		t.Fatal("a rejected record was reported as success")
	}
	mu.Lock()
	defer mu.Unlock()
	if len(received) != 3 {
		t.Fatalf("posted %d records, want all 3 attempted: %+v", len(received), received)
	}
	for i, sk := range received {
		if sk.ID != neon.NodeID(i+1) || sk.Host != fmt.Sprintf("sk-%d.%s.svc.cluster.local", i+1, namespace) {
			t.Errorf("record %d = %+v", i, sk)
		}
	}
}
