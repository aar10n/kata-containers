REGISTRY ?= gcr.io/cohere-artifacts
TAG ?= latest
PUSH ?= false
DOCKER_BUILDKIT ?= 1
REMOTE ?= aaron@192.168.1.105:build/
PROTOC ?= protoc
REPO_ROOT := $(shell cd ../.. && pwd)
REMOTE_HOST := $(shell printf '%s' "$(REMOTE)" | sed -E 's/:.*$$//')
REMOTE_PATH := $(shell printf '%s' "$(REMOTE)" | sed -E 's/^[^:]+://')
REMOTE_REPO := $(REMOTE_PATH)kata-containers
REMOTE_BUILD_DIR := $(REMOTE_REPO)/src/sandbox-service
PROTO_DIR := $(REPO_ROOT)/src/sandbox-service/proto
PROTO_OUT := $(REPO_ROOT)/src/sandbox-service/pkg/api/pb

.PHONY: build docker-build docker-push test run-local run-local-kata proto-gen

build:
	go build -o bin/sandbox-service ./main.go

test:
	go test -v ./...

docker-build:
	DOCKER_BUILDKIT=$(DOCKER_BUILDKIT) docker build --platform linux/amd64 \
		-t $(REGISTRY)/sandbox-service:$(TAG) \
		-f ../../tools/packaging/sandbox-service/Dockerfile \
		../../
	@if [ "$(PUSH)" = "true" ] || [ "$(PUSH)" = "1" ]; then \
		docker push $(REGISTRY)/sandbox-service:$(TAG); \
	fi

# For building on remote Linux machine (from macOS)
docker-build-remote:
	@rsync -az --delete --relative \
		$(REPO_ROOT)/./src/sandbox-service \
		$(REPO_ROOT)/./tools/packaging/sandbox-service/Dockerfile \
		$(REMOTE)kata-containers/
	@ssh $(REMOTE_HOST) 'cd $(REMOTE_BUILD_DIR) && make docker-build PUSH=$(PUSH) TAG=$(TAG)'

run-local:
	go run ./main.go --platform=docker --http-addr=:8080

run-local-kata:
	go run ./main.go --platform=kata --http-addr=:8080

proto-gen:
	@mkdir -p $(PROTO_OUT)
	@PATH="$$(go env GOPATH)/bin:$$PATH" $(PROTOC) -I $(PROTO_DIR) \
		--go_out=$(PROTO_OUT) --go_opt=paths=source_relative \
		--go-grpc_out=$(PROTO_OUT) --go-grpc_opt=paths=source_relative \
		$(PROTO_DIR)/storage.proto
