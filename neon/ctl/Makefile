IMG ?= ghcr.io/hzhou0/neon-ctl:0.1.0

.PHONY: all
all: fmt vet test build

.PHONY: fmt
fmt:
	go fmt ./...

.PHONY: vet
vet:
	go vet ./...

ENVTEST_VERSION ?= 1.30.3
ENVTEST_PLATFORM := $(shell go env GOOS)-$(shell go env GOARCH)
ENVTEST_DIR := $(CURDIR)/bin/k8s/$(ENVTEST_VERSION)-$(ENVTEST_PLATFORM)

# The kube package tests run against a real apiserver rather than a mock client, so its binaries
# have to be on disk first.
.PHONY: setup-envtest
setup-envtest:
	@test -x $(ENVTEST_DIR)/kube-apiserver || { \
		echo "fetching envtest $(ENVTEST_VERSION) binaries"; \
		mkdir -p $(ENVTEST_DIR); \
		curl -fsSL https://github.com/kubernetes-sigs/controller-tools/releases/download/envtest-v$(ENVTEST_VERSION)/envtest-v$(ENVTEST_VERSION)-$(ENVTEST_PLATFORM).tar.gz \
			| tar -xz -C $(ENVTEST_DIR) --strip-components=2; \
	}

.PHONY: test
test: setup-envtest
	KUBEBUILDER_ASSETS=$(ENVTEST_DIR) go test ./... -count=1

# Rewrites the pinned compute spec. Run it only when a Neon upgrade is meant to change the
# document, and read the diff.
.PHONY: update-golden
update-golden:
	go test ./internal/controlplane/ -run TestRenderSpecGolden -update

.PHONY: build
build:
	go build -o bin/neon-ctl ./cmd/neon-ctl

.PHONY: run
run:
	go run ./cmd/neon-ctl

.PHONY: docker-build
docker-build:
	docker build -t $(IMG) .

.PHONY: docker-push
docker-push:
	docker push $(IMG)
