# Agent Guidelines — Trawler

## Definition of Done

Before claiming any task complete:

1. **Tests pass**: `cd trawler && go test ./...`
2. **Lint clean**: `cd trawler && make lint`
   (`go run github.com/golangci/golangci-lint/cmd/golangci-lint@latest run ./...`)

Both must be green. No exceptions.

## Scope

- Trawler is a capture-table → shared-stream relay. **Enrichment is out of
  scope** (separate microservice). Do not add a `lookup`/`enrich` package or DB
  reads beyond claiming/deleting from the capture table.
- Keep business logic out of the Postgres trigger.

## Layout

- Go module lives in `trawler/`. Local dev stack (`docker-compose.yml`,
  `postgres/init.sql`) lives at the repo root.

## Git

- Do not use interactive rebase.
- Concise, one-line commit messages.
