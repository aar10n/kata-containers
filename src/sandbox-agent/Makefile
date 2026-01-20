#REGISTRY ?= gcr.io/cohere-artifacts
REGISTRY ?= us-docker.pkg.dev/cohere-artifacts/replicated/cohere
TAG ?= latest
PUSH ?= false
#REMOTE ?= aaron@192.168.1.105:build/
REMOTE ?= aaron-builder:build/
PROTOC ?= protoc
DOCKER_BUILDKIT ?= 1

REPO_ROOT := $(shell cd ../.. && pwd)
DOCKER_BUILD_CMD = DOCKER_BUILDKIT=$(DOCKER_BUILDKIT) docker build --platform linux/amd64 -t $(REGISTRY)/sandbox/agent:$(TAG) -f ../../tools/packaging/sandbox-agent/Dockerfile ../../
REMOTE_HOST := $(shell printf '%s' "$(REMOTE)" | sed -E 's/:.*$$//')
REMOTE_PATH := $(shell printf '%s' "$(REMOTE)" | sed -E 's/^[^:]+://')
REMOTE_REPO := $(REMOTE_PATH)kata-containers
REMOTE_BUILD_DIR := $(REMOTE_REPO)/src/sandbox-agent
RSYNC_PATHS = $(REPO_ROOT)/./src/runtime $(REPO_ROOT)/./src/sandbox-agent $(REPO_ROOT)/./tools/packaging/sandbox-agent/Dockerfile
PROTO_DIR := $(REPO_ROOT)/src/sandbox-agent/proto
PROTO_OUT := $(REPO_ROOT)/src/sandbox-agent/pkg/api/pb
STORAGE_PROTO_DIR := $(REPO_ROOT)/src/sandbox-service/proto
STORAGE_PROTO_OUT := $(REPO_ROOT)/src/sandbox-agent/pkg/storage/pb
PROTO_INCLUDES := -I /opt/homebrew/include -I $(PROTO_DIR) -I $(REPO_ROOT)/src/libs/protocols/protos -I $(PROTO_DIR)/google/api

.PHONY: docker-build docker-build-remote proto-gen proto-gen-storage

docker-build:
	$(DOCKER_BUILD_CMD)
	@if [ "$(PUSH)" = "true" ] || [ "$(PUSH)" = "1" ]; then \
		docker push $(REGISTRY)/sandbox/agent:$(TAG); \
	fi

docker-build-remote:
	@rsync -az --delete --relative $(RSYNC_PATHS) $(REMOTE)kata-containers/
	@ssh $(REMOTE_HOST) 'cd $(REMOTE_BUILD_DIR) && $(DOCKER_BUILD_CMD) && if [ "$(PUSH)" = "true" ] || [ "$(PUSH)" = "1" ]; then docker push $(REGISTRY)/sandbox/agent:$(TAG); fi'

proto-gen: proto-gen-storage
	@mkdir -p $(PROTO_OUT)
	@PATH="$$(go env GOPATH)/bin:$$PATH" $(PROTOC) $(PROTO_INCLUDES) \
		--go_out=$(PROTO_OUT) --go_opt=paths=source_relative \
		--go-grpc_out=$(PROTO_OUT) --go-grpc_opt=paths=source_relative \
		--grpc-gateway_out=$(PROTO_OUT) --grpc-gateway_opt=paths=source_relative \
		$(PROTO_DIR)/sandbox.proto

# Regenerate storage pb from sandbox-service's authoritative proto definition
# Note: Uses M option to override go_package for sandbox-agent's import path
proto-gen-storage:
	@mkdir -p $(STORAGE_PROTO_OUT)
	@PATH="$$(go env GOPATH)/bin:$$PATH" $(PROTOC) -I $(STORAGE_PROTO_DIR) \
		--go_out=$(STORAGE_PROTO_OUT) --go_opt=paths=source_relative \
		--go_opt=Mstorage.proto=github.com/cohere-ai/kata-containers/src/sandbox-agent/pkg/storage/pb \
		--go-grpc_out=$(STORAGE_PROTO_OUT) --go-grpc_opt=paths=source_relative \
		--go-grpc_opt=Mstorage.proto=github.com/cohere-ai/kata-containers/src/sandbox-agent/pkg/storage/pb \
		$(STORAGE_PROTO_DIR)/storage.proto
