# Filesystem-backed Kubernetes API server experiment
#
# Targets:
#   make build  — compile both binaries into ./bin/
#   make up     — generate pki+kubeconfig (if missing), start apiserver + kubelet-lite
#   make down   — stop background processes started by `make up`
#   make logs   — tail logs from ./run/
#   make data   — show on-disk state (the "etcd")
#   make demo   — apply examples/nginx.yaml against the running apiserver
#   make clean  — make down + remove ./data and ./run
#   make tidy   — go mod tidy
#   make test   — go test ./...
#   make lint   — golangci-lint run
#
# All targets assume you're inside the nix dev shell (`nix develop`).

SHELL       := bash
.SHELLFLAGS := -eu -o pipefail -c

BIN_DIR  := bin
RUN_DIR  := run
DATA_DIR := data
PKI_DIR  := $(RUN_DIR)/pki
KUBECONFIG_PATH := $(RUN_DIR)/kubeconfig

APISERVER_BIN := $(BIN_DIR)/apiserver
KUBELET_BIN   := $(BIN_DIR)/kubelet-lite

APISERVER_PID := $(RUN_DIR)/apiserver.pid
KUBELET_PID   := $(RUN_DIR)/kubelet-lite.pid

APISERVER_LOG := $(RUN_DIR)/apiserver.log
KUBELET_LOG   := $(RUN_DIR)/kubelet-lite.log

GO ?= go

.DEFAULT_GOAL := help

.PHONY: help
help: ## Show this help
	@awk 'BEGIN{FS=":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

# ---- build ---------------------------------------------------------------

.PHONY: build
build: $(APISERVER_BIN) $(KUBELET_BIN) ## Build both binaries

$(APISERVER_BIN): $(shell find . -name '*.go' -not -path './.gopath/*' 2>/dev/null) go.mod go.sum
	@mkdir -p $(BIN_DIR)
	$(GO) build -o $@ ./cmd/apiserver

$(KUBELET_BIN): $(shell find . -name '*.go' -not -path './.gopath/*' 2>/dev/null) go.mod go.sum
	@mkdir -p $(BIN_DIR)
	$(GO) build -o $@ ./cmd/kubelet-lite

# ---- run / stop ----------------------------------------------------------

.PHONY: up
up: build ## Start apiserver + kubelet-lite in the background
	@mkdir -p $(RUN_DIR) $(DATA_DIR)
	@bash scripts/dev.sh up

.PHONY: down
down: ## Stop background processes
	@bash scripts/dev.sh down

.PHONY: logs
logs: ## Tail logs from both binaries
	@tail -F $(APISERVER_LOG) $(KUBELET_LOG)

# ---- inspection / demo ---------------------------------------------------

.PHONY: data
data: ## Show on-disk state (the "etcd")
	@if command -v tree >/dev/null; then tree -a $(DATA_DIR) 2>/dev/null || echo "$(DATA_DIR) does not exist yet"; \
	else find $(DATA_DIR) -type f 2>/dev/null || echo "$(DATA_DIR) does not exist yet"; fi

.PHONY: demo
demo: ## Apply example Pod and list Pods
	KUBECONFIG=$(KUBECONFIG_PATH) kubectl apply -f examples/nginx.yaml
	KUBECONFIG=$(KUBECONFIG_PATH) kubectl get pods -A

# ---- housekeeping --------------------------------------------------------

.PHONY: clean
clean: down ## Stop processes and remove all runtime state
	rm -rf $(BIN_DIR) $(RUN_DIR) $(DATA_DIR)

.PHONY: tidy
tidy: ## go mod tidy
	$(GO) mod tidy

.PHONY: test
test: ## Run unit tests
	$(GO) test ./...

.PHONY: smoke
smoke: ## Full end-to-end smoke test (requires Docker)
	@bash scripts/smoke.sh

.PHONY: lint
lint: ## Run golangci-lint
	golangci-lint run ./...
