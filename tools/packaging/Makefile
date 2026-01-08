#
# Copyright (c) 2018 Intel Corporation
#
# SPDX-License-Identifier: Apache-2.0
#
#

MK_DIR :=$(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))
REPO_ROOT := $(shell cd $(MK_DIR)/../.. && pwd)
SED := sed
YQ := $(MK_DIR)/yq
VERSIONS_YAML_FILE := ../../versions.yaml
VERSION_FILE := ../../VERSION

# Remote build configuration
# Format: user@host:path/ or user@host:port:path/ for custom SSH port
REMOTE ?= aaron@localhost:2222:build/
REMOTE_HOST := $(shell printf '%s' "$(REMOTE)" | sed -E 's/:.*$$//')
REMOTE_PORT := $(shell printf '%s' "$(REMOTE)" | sed -E 's/^[^:]+://; s/:.*//; s/[^0-9]//g')
REMOTE_PATH := $(shell printf '%s' "$(REMOTE)" | sed -E 's/^[^:]+://; s/^[0-9]+://')
REMOTE_REPO := $(REMOTE_PATH)kata-containers
SSH_OPTS := $(if $(REMOTE_PORT),-p $(REMOTE_PORT),)
RSYNC_SSH := $(if $(REMOTE_PORT),-e "ssh -p $(REMOTE_PORT)",)

# kata-deploy image configuration
REGISTRY ?= gcr.io/cohere-artifacts
TAG ?= latest
PUSH ?= false

export MK_DIR
export YQ
export VERSION_FILE
export VERSIONS_YAML_FILE

test-static-build:
	@make -f $(MK_DIR)/static-build/qemu/Makefile
	@make -f $(MK_DIR)/static-build/firecracker/Makefile

$(YQ):
	@bash -c "source scripts/lib.sh; install_yq $${MK_DIR}"

# ============================================================================
# kata-deploy remote build targets
# ============================================================================
# These targets build the kata-deploy image on a remote Linux machine.
# Useful when developing on macOS or other non-Linux platforms.
#
# Usage:
#   make kata-deploy-remote                    # Build kata-tarball on remote
#   make kata-deploy-image-remote              # Build full kata-deploy image
#   make kata-deploy-image-remote PUSH=true    # Build and push to registry
#   make REMOTE=user@host:path/ kata-deploy-remote
#
# The remote machine must have:
#   - Docker installed and running
#   - Sufficient disk space (~50GB recommended)
#   - SSH access configured (key-based auth recommended)
# ============================================================================

# Rsync exclusions for remote sync
# Note: .git is included because build scripts need it for version info
RSYNC_EXCLUDES := \
	--exclude='src/sandbox-agent' \
	--exclude='src/sandbox-service' \
	--exclude='tools/packaging/kata-deploy/local-build/build' \
	--exclude='tools/packaging/kata-deploy/kata-static*.tar.zst' \
	--exclude='*.o' \
	--exclude='*.a' \
	--exclude='target/' \
	--exclude='node_modules/' \
	--exclude='.claude/'

# Sync the repository to the remote machine
kata-deploy-sync:
	@echo "Syncing kata-containers to $(REMOTE_HOST):$(REMOTE_REPO)..."
	@rsync -az --delete $(RSYNC_SSH) $(RSYNC_EXCLUDES) \
		$(REPO_ROOT)/ $(REMOTE_HOST):$(REMOTE_REPO)/

# Build kata-tarball (all components) on remote machine
kata-deploy-remote: kata-deploy-sync
	@echo "Building kata-tarball on $(REMOTE_HOST)..."
	@ssh $(SSH_OPTS) $(REMOTE_HOST) 'cd $(REMOTE_REPO)/tools/packaging/kata-deploy/local-build && make kata-tarball'

# Build individual component tarballs on remote (useful for debugging)
# Usage: make kata-deploy-component-remote COMPONENT=agent
COMPONENT ?= agent
kata-deploy-component-remote: kata-deploy-sync
	@echo "Building $(COMPONENT)-tarball on $(REMOTE_HOST)..."
	@ssh $(SSH_OPTS) $(REMOTE_HOST) 'cd $(REMOTE_REPO)/tools/packaging/kata-deploy/local-build && make $(COMPONENT)-tarball'

# Build the full kata-deploy Docker image on remote machine
kata-deploy-image-remote: kata-deploy-sync
	@echo "Building kata-deploy image on $(REMOTE_HOST)..."
	@ssh $(SSH_OPTS) $(REMOTE_HOST) '\
		cd $(REMOTE_REPO)/tools/packaging/kata-deploy/local-build && \
		make kata-tarball && \
		cd $(REMOTE_REPO)/tools/packaging/kata-deploy && \
		cp local-build/build/kata-static.tar.zst . && \
		docker build -t $(REGISTRY)/kata-deploy:$(TAG) . && \
		if [ "$(PUSH)" = "true" ] || [ "$(PUSH)" = "1" ]; then \
			docker push $(REGISTRY)/kata-deploy:$(TAG); \
		fi'
	@echo "kata-deploy image built: $(REGISTRY)/kata-deploy:$(TAG)"

# Build kata-deploy image using Rust-based deploy scripts
kata-deploy-image-rust-remote: kata-deploy-sync
	@echo "Building kata-deploy (Rust) image on $(REMOTE_HOST)..."
	@ssh $(SSH_OPTS) $(REMOTE_HOST) '\
		cd $(REMOTE_REPO)/tools/packaging/kata-deploy/local-build && \
		make kata-tarball && \
		cd $(REMOTE_REPO)/tools/packaging/kata-deploy && \
		cp local-build/build/kata-static.tar.zst . && \
		docker build -f Dockerfile.rust -t $(REGISTRY)/kata-deploy:$(TAG)-rust . && \
		if [ "$(PUSH)" = "true" ] || [ "$(PUSH)" = "1" ]; then \
			docker push $(REGISTRY)/kata-deploy:$(TAG)-rust; \
		fi'
	@echo "kata-deploy image built: $(REGISTRY)/kata-deploy:$(TAG)-rust"

# Fetch the built tarball from remote machine
kata-deploy-fetch:
	@echo "Fetching kata-static.tar.zst from $(REMOTE_HOST)..."
	@scp $(SSH_OPTS) $(REMOTE_HOST):$(REMOTE_REPO)/tools/packaging/kata-deploy/local-build/build/kata-static.tar.zst \
		$(MK_DIR)/kata-deploy/local-build/build/

# Clean build artifacts on remote machine
kata-deploy-clean-remote:
	@echo "Cleaning build artifacts on $(REMOTE_HOST)..."
	@ssh $(SSH_OPTS) $(REMOTE_HOST) 'rm -rf $(REMOTE_REPO)/tools/packaging/kata-deploy/local-build/build'

.PHONY: test-static-build kata-deploy-sync kata-deploy-remote kata-deploy-component-remote \
	kata-deploy-image-remote kata-deploy-image-rust-remote kata-deploy-fetch kata-deploy-clean-remote
