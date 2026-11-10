# Pin to the locally installed toolchain; dependencies are chosen to support Go 1.25.
export GOTOOLCHAIN := local

GOLANGCI_LINT ?= $(shell command -v golangci-lint 2>/dev/null || echo $(shell go env GOPATH)/bin/golangci-lint)

.PHONY: generate build build-release test lint up down reset app-user chaos

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

# Local secrets live in .env (never committed). Created once from the example;
# an existing .env is never overwritten.
.env:
	cp .env.example .env
	@echo "created .env from .env.example: edit it to change the local secrets"

up: .env
	docker compose up -d --build

down:
	docker compose down

# Like down, but also deletes the local database (needed after changing
# POSTGRES_PASSWORD in .env).
reset:
	docker compose down -v

# Create or update idem_app_user (the restricted login) on an existing local
# database, e.g. after changing IDEM_APP_DB_PASSWORD in .env. A new database
# gets it automatically.
app-user: .env
	docker compose up -d --wait postgres
	docker compose exec postgres /docker-entrypoint-initdb.d/10-app-user.sh

# Chaos campaign against real processes (needs Docker). Example:
#   make chaos CHAOS_ARGS="--seed 1 --duration 90s"
chaos: build
	bin/chaos $(CHAOS_ARGS)
