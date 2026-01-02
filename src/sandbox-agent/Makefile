REGISTRY ?= gcr.io/cohere-artifacts
TAG ?= latest
PUSH ?= false
REMOTE ?= aaron@192.168.1.105:build/
PROTOC ?= protoc

REPO_ROOT := $(shell cd ../.. && pwd)
DOCKER_BUILD_CMD = docker build --platform linux/amd64 -t $(REGISTRY)/sandbox-agent:$(TAG) -f ../../tools/packaging/sandbox-agent/Dockerfile ../../
REMOTE_HOST := $(shell printf '%s' "$(REMOTE)" | sed -E 's/:.*$$//')
REMOTE_PATH := $(shell printf '%s' "$(REMOTE)" | sed -E 's/^[^:]+://')
REMOTE_REPO := $(REMOTE_PATH)kata-containers
REMOTE_BUILD_DIR := $(REMOTE_REPO)/src/sandbox-agent
RSYNC_PATHS = $(REPO_ROOT)/./src/runtime $(REPO_ROOT)/./src/sandbox-agent $(REPO_ROOT)/./tools/packaging/sandbox-agent/Dockerfile
PROTO_DIR := $(REPO_ROOT)/src/sandbox-agent/proto
PROTO_OUT := $(REPO_ROOT)/src/sandbox-agent/pkg/api/pb
PROTO_INCLUDES := -I /opt/homebrew/include -I $(PROTO_DIR) -I $(REPO_ROOT)/src/libs/protocols/protos -I $(PROTO_DIR)/google/api

.PHONY: docker-build docker-build-remote proto-gen

docker-build:
	$(DOCKER_BUILD_CMD)
	@if [ "$(PUSH)" = "true" ] || [ "$(PUSH)" = "1" ]; then \
		docker push $(REGISTRY)/sandbox-agent:$(TAG); \
	fi

docker-build-remote:
	@rsync -az --delete --relative $(RSYNC_PATHS) $(REMOTE)kata-containers/
	@ssh $(REMOTE_HOST) 'cd $(REMOTE_BUILD_DIR) && $(DOCKER_BUILD_CMD) && if [ "$(PUSH)" = "true" ] || [ "$(PUSH)" = "1" ]; then docker push $(REGISTRY)/sandbox-agent:$(TAG); fi'

proto-gen:
	@mkdir -p $(PROTO_OUT)
	@PATH="$$(go env GOPATH)/bin:$$PATH" $(PROTOC) $(PROTO_INCLUDES) \
		--go_out=$(PROTO_OUT) --go_opt=paths=source_relative \
		--go-grpc_out=$(PROTO_OUT) --go-grpc_opt=paths=source_relative \
		--grpc-gateway_out=$(PROTO_OUT) --grpc-gateway_opt=paths=source_relative \
		$(PROTO_DIR)/sandbox.proto
