IMG ?= ghcr.io/hzhou0/opnsense-operator:0.1.0

.PHONY: all
all: fmt vet test build

.PHONY: fmt
fmt:
	go fmt ./...

.PHONY: vet
vet:
	go vet ./...

.PHONY: test
test:
	go test ./... -count=1

.PHONY: build
build:
	go build -o bin/manager ./cmd/manager

.PHONY: run
run:
	go run ./cmd/manager

.PHONY: docker-build
docker-build:
	docker build -t $(IMG) .

.PHONY: docker-push
docker-push:
	docker push $(IMG)

.PHONY: helm-lint
helm-lint:
	helm lint chart

.PHONY: helm-template
helm-template:
	helm template opnsense-operator chart \
	  --set opnsense.apiKey=dummy --set opnsense.apiSecret=dummy
