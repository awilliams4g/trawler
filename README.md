# Trawler

Trawler is a Go service that relays Postgres row changes to **shared** Redis
streams. It reads changes from a trigger-populated capture table (`cdc_events`)
— **no logical replication required** — and emits one event per change to a
literal, non-instance-scoped stream key per logical event type.

It is the production CDC relay that **replaces WALker**, whose logical-decoding
source cannot run on Postgres instances without logical replication enabled.

## How it works

```
Postgres (AFTER trigger) ──► cdc_events ──► Trawler ──► Redis Stream ──► consumers
            (same txn)        (poll +         (claim-and-delete)   (shared key,
                               SKIP LOCKED)                         source in payload)
```

1. A generic `AFTER INSERT/UPDATE/DELETE` trigger serializes `NEW`/`OLD` to
   jsonb and inserts one row into `cdc_events`, **inside the originating
   transaction**.
2. Trawler polls `cdc_events`, claims a batch (`FOR UPDATE SKIP LOCKED`,
   `ORDER BY id`), `XADD`s each change to its configured shared stream, then
   `DELETE`s the claimed rows and commits.
3. Consumers (e.g. Dasher) read the shared stream as plain fields.

> Enrichment (SQL lookups colocated with the change data) is **out of scope** in
> this iteration and will live in a separate microservice. Trawler today is a
> pure capture-table → shared-stream relay.

## Delivery semantics

**At-least-once.** A row is deleted only after a durable relay, so a crash
between `XADD` and `DELETE` re-relays the row. Each entry carries the capture-row
`id` as its dedup token — consumers dedup on it. Redis stream IDs (assigned in
relay order) remain the consumer-side ordering token.

Claiming uses `FOR UPDATE SKIP LOCKED` rather than a high-water-mark, because
`cdc_events.id` is assigned at statement time (not commit time): a watermark
would skip a late-committing lower id. Nothing is skipped.

## Event shape

Each Redis stream entry contains:

| Field | Description |
|---|---|
| `op` | `insert` / `update` / `delete` |
| `table` | e.g. `orders` |
| `schema` | e.g. `public` |
| `id` | capture-row id (use for dedup) |
| `lsn` | transitional alias of `id` for WALker-era consumers; drop once all consumers dedup on `id` |
| `source` | origin `instance_id` |
| `streamed_at` | RFC3339 timestamp |
| `data` | full new row (insert/update) or full old row (delete), as JSON |
| `old` | previous row (update/delete), as JSON; omitted otherwise |

The origin instance is carried in `source`, **not** the key, so many source
databases can fan into one stream per logical event type.

## Configuration

### Environment (secrets / connection / tuning)

| Env var | Default | Description |
|---|---|---|
| `TRAWLER_PG_DSN` | `postgres://postgres:postgres@localhost:5432/mydb` | Postgres DSN (normal pool, not replication) |
| `TRAWLER_REDIS_ADDR` | `localhost:6380` | Redis address |
| `TRAWLER_CONFIG` | `config.yaml` | Path to the YAML catalog/table-mapping file |
| `TRAWLER_POLL_INTERVAL` | `1s` | Idle poll cadence (when the capture table is empty) |
| `TRAWLER_BATCH_SIZE` | `100` | Max rows claimed per cycle |
| `TRAWLER_ESCALATE_AFTER` | `10` | Consecutive transient failures before WARN→ERROR |
| `SLOG_LEVEL` | `info` | Log level: `debug`/`info`/`warn`/`error`. `debug` logs each relayed change |

### YAML (catalog + per-table mapping)

```yaml
instance_id: bayer-17909          # populates the `source` field on every entry
cdc_table: public.cdc_events      # capture table ("schema.table" or "table")
tables:
  orders:
    emit: cdc.orders              # literal shared Redis stream key
  products:
    emit: cdc.products
```

Each captured table must have a mapping; a captured row for an unmapped table is
fatal (the process exits non-zero and the row stays in `cdc_events` for
inspection).

## Error handling / back-pressure

- **Transient** (DB / Redis error): the claimed rows are **not** deleted; the
  cycle retries with exponential backoff. The capture table holds the work,
  which is the natural back-pressure — a persistent stall grows `cdc_events` on
  the primary. Logs escalate WARN → ERROR after `TRAWLER_ESCALATE_AFTER`.
- **Fatal** (unmapped table, malformed capture row): exit non-zero. The
  supervisor restarts; the offending row stays in `cdc_events`.

## Running locally

```bash
# Start Postgres (with cdc_events + trigger) and Redis
docker compose up -d postgres redis

# Run Trawler against the local stack
cd trawler
TRAWLER_CONFIG=./config.yaml go run ./cmd/trawler
```

Or the full stack including Trawler: `docker compose up -d`.

## Code layout

```
cmd/trawler/       — wiring, entrypoint
internal/config/   — env (secrets/conn/tuning) + YAML (catalog/tables) loader
internal/cdc/      — poll loop (claim → build Change → emit → delete → commit) + pg store
internal/model/    — Change struct + jsonb decode
internal/sink/     — map Change → XADD to the shared key (writes `source`)
```

## Design

See [`docs/2026-06-15-trawler-design.md`](docs/2026-06-15-trawler-design.md).
