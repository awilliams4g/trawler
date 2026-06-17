# trawler/Makefile
.PHONY: build test lint up down smoke

.ONESHELL:
SHELL := /bin/bash

build:
	go build ./...

test:
	go test ./... -count=1

lint:
	go run github.com/golangci/golangci-lint/cmd/golangci-lint@latest run ./...

up:
	docker compose -f ../docker-compose.yml up -d

down:
	docker compose -f ../docker-compose.yml down

smoke: ## End-to-end: bring up stack, run trawler, insert a row, check Redis
	@set -euo pipefail
	echo "=== Starting stack ==="
	docker compose -f ../docker-compose.yml up -d postgres redis
	echo "=== Waiting for Postgres ==="
	until docker exec tr_postgres pg_isready -U postgres -d mydb; do sleep 1; done
	echo "=== Building trawler ==="
	go build -o /tmp/trawler ./cmd/trawler
	echo "=== Starting trawler in background ==="
	TRAWLER_PG_DSN="postgres://postgres:postgres@localhost:5432/mydb" \
	  TRAWLER_REDIS_ADDR="localhost:6380" \
	  TRAWLER_CONFIG="./config.yaml" \
	  /tmp/trawler &
	TRAWLER_PID=$$!
	trap 'kill $$TRAWLER_PID 2>/dev/null || true' EXIT
	sleep 2
	echo "=== Inserting test row ==="
	docker exec tr_postgres psql -U postgres -d mydb -c \
	  "INSERT INTO orders(customer_name, item, quantity, status) VALUES ('Smoke', 'Widget A', 1, 'smoke-test');"
	sleep 2
	echo "=== Checking Redis shared stream (expect >= 1) ==="
	docker exec tr_redis redis-cli -p 6379 XLEN cdc.orders
