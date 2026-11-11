# The Go toolchain is pinned by go.mod's toolchain line: the go command fetches
# that exact version if it is not installed, so local builds, CI and the Docker
# image all use the same one.

# The same golangci-lint as CI (.github/workflows/ci.yml); bump both together.
GOLANGCI_LINT_VERSION := v2.15.0
GOLANGCI_LINT ?= go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)

.PHONY: generate build build-release test lint vuln up down reset app-user chaos

generate:
	go tool sqlc generate

build:
	go build -o bin/idem ./cmd/idem
	go build -o bin/fakeprovider ./cmd/fakeprovider
	go build -o bin/chaos ./cmd/chaos
	go build -o bin/fakeshop ./cmd/fakeshop

# Release build: failpoints compiled out.
build-release:
	go build -tags nofailpoint -o bin/idem ./cmd/idem

test:
	go test -race -count=1 ./...

lint:
	$(GOLANGCI_LINT) run

# Known vulnerabilities our code can reach (same check as CI's Security workflow).
vuln:
	go run golang.org/x/vuln/cmd/govulncheck@v1.9.0 ./...

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
