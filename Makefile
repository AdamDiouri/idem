# Pin to the locally installed toolchain; dependencies are chosen to support Go 1.25.
export GOTOOLCHAIN := local

GOLANGCI_LINT ?= $(shell command -v golangci-lint 2>/dev/null || echo $(shell go env GOPATH)/bin/golangci-lint)

.PHONY: generate build build-release test lint up down chaos

generate:
	go tool sqlc generate

build:
	go build -o bin/idem ./cmd/idem
	go build -o bin/fakeprovider ./cmd/fakeprovider
	go build -o bin/chaos ./cmd/chaos

# Release build: failpoints compiled out.
build-release:
	go build -tags nofailpoint -o bin/idem ./cmd/idem

test:
	go test -race -count=1 ./...

lint:
	$(GOLANGCI_LINT) run

up:
	docker compose up -d --build

down:
	docker compose down

# Chaos campaign against real processes (needs Docker). Example:
#   make chaos CHAOS_ARGS="--seed 1 --duration 90s"
chaos: build
	bin/chaos $(CHAOS_ARGS)
