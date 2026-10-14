.PHONY: build run test test-integration vet fmt fmt-check lint up down migrate ci

BIN_DIR := bin

build: ## Build all binaries
	go build -o $(BIN_DIR)/ ./cmd/...

run: ## Run the API locally (expects a reachable Postgres)
	go run ./cmd/api

migrate: ## Apply migrations against DATABASE_URL
	go run ./cmd/migrate up

test: ## Run unit tests with the race detector
	go test -race -count=1 ./...

test-integration: ## Run all tests including DB integration (needs TEST_DATABASE_URL)
	TEST_DATABASE_URL=$${TEST_DATABASE_URL:-postgres://postgres:postgres@localhost:5432/payments_test?sslmode=disable} \
		go test -race -count=1 ./...

vet:
	go vet ./...

fmt:
	gofmt -s -w .

fmt-check: ## Fail if any file is not gofmt'd
	@unformatted=$$(gofmt -s -l .); \
	if [ -n "$$unformatted" ]; then \
		echo "Files not formatted:"; echo "$$unformatted"; exit 1; \
	fi

ci: fmt-check vet test build

up: ## Start the full stack with Docker Compose
	docker compose up --build

down:
	docker compose down -v
