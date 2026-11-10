.PHONY: all test test-postgres test-tck clean mock lint build help

# Go parameters
GOCMD=go
GOTEST=$(GOCMD) test
GOGET=$(GOCMD) get
BINARY_NAME=dataplane-sdk-go
GOBUILD=$(GOCMD) build

all: test

help: ## Display this help
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n\nTargets:\n"} /^[a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 }' $(MAKEFILE_LIST)

clean: ## Clean build artifacts
	rm -f $(BINARY_NAME)
	rm -f pkg/**/mocks_test.go

deps: ## Get dependencies
	$(GOGET) -v -t -d ./...

mock: ## Generate mocks using go generate
	$(GOCMD) generate ./...

test: ## Run tests without postgres
	$(GOTEST) -v ./... -tags !postgres

test-postgres: ## Run all tests including postgres
	$(GOTEST) -v ./... -tags postgres

test-tck: ## Run the Data Plane Signaling TCK against the SDK (needs Docker)
	$(GOTEST) -v ./internal/tests/tck/... -tags tck

test-coverage: ## Run tests with coverage
	$(GOTEST) -v -coverprofile=coverage.out ./...
	$(GOCMD) tool cover -html=coverage.out -o coverage.html

lint: ## Run linter
	$(GOCMD) vet ./...
	if ! command -v golangci-lint >/dev/null 2>&1; then \
		curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(GOPATH)/bin; \
	fi
	golangci-lint run

build: ## Build the binary
	$(GOBUILD) -v ./...

ci: deps mock lint test test-postgres ## Run all CI tasks