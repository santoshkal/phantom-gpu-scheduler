BINARY := gpusim-scheduler
GOOS  ?= linux
GOARCH ?= $(shell go env GOARCH)

.PHONY: build
build: ## Build the scheduler binary
	go build -o bin/$(BINARY) .

.PHONY: test
test: ## Run all tests with race detector
	go test -race -count=1 -cover ./...

.PHONY: vet
vet: ## Run go vet
	go vet ./...

.PHONY: lint
lint: ## Run golangci-lint
	golangci-lint run

.PHONY: clean
clean: ## Remove build artifacts
	rm -rf bin/

.PHONY: deploy
deploy: ## Deploy scheduler manifests to the current kube context
	kubectl apply -f deploy/manifests/

.PHONY: help
help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'
