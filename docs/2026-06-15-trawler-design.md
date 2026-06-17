# Trawler — design

Status: implemented (v0, enrichment deferred)
Date: 2026-06-15

Trawler is a Go service that **replaces WALker** as the production CDC relay.
It reads row changes from a trigger-populated capture table and writes the
result to **shared** Redis streams.

> **Implementation status (v0).** Enrichment is intentionally **not** built into
> Trawler; it will live in a separate microservice. Trawler today is a pure
> capture-table → shared-stream relay. The original design (which folded
> enrichment onto the relay) is preserved below for context; see
> "Implementation notes (v0)" for what actually shipped and why.

## Background

WALker reads committed Postgres changes via logical decoding (replication slot +
wal2json) and writes one event per change to an instance-scoped Redis stream
(`<instanceID>.cdc.<table>`). Production Postgres instances do **not** have
logical replication enabled, so WALker's source cannot run there.

## Goal

- Source row changes from a trigger-written capture table (no logical
  replication required).
- Emit events to **shared** (non-instance-scoped) Redis streams, so multiple
  source databases can fan into one stream per logical event type.
- (Deferred) Optionally enrich each change with simple SQL lookups colocated
  with the change data.

## Architecture

```
Postgres (AFTER trigger) ──► cdc_events ──► Trawler ──► Redis Stream ──► consumers
            (same txn)        (poll +         (claim-and-delete)   (shared key,
                               SKIP LOCKED)                         source in payload)
```

- **Trigger**: a generic AFTER INSERT/UPDATE/DELETE trigger serializes
  `NEW`/`OLD` to jsonb and inserts one row into `cdc_events`, **inside the
  originating transaction**. No business logic; a trigger failure fails the
  user's write.
- **Trawler**: polls `cdc_events`, claims rows, builds a `Change`, XADDs the
  event to the configured shared stream, then deletes the claimed rows and
  commits.
- **Consumers** (e.g. Dasher): read the shared stream as plain fields.

## Capture table

```sql
CREATE TABLE cdc_events (
    id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    op          text        NOT NULL,   -- insert | update | delete
    schema_name text        NOT NULL,
    table_name  text        NOT NULL,
    data        jsonb       NOT NULL,   -- full new row (insert/update) or full old row (delete)
    old         jsonb,                  -- previous row (update/delete); NULL on insert
    created_at  timestamptz NOT NULL DEFAULT now()
);
```

- One **shared** capture table (not per-table); `ORDER BY id` gives a single
  relay order.
- `old` is the full old row on DELETE/UPDATE (cheap via trigger; enables richer
  delete enrichment later).

## Delivery semantics

- **Claim-and-delete**, not watermark. The `id` sequence is assigned at
  statement time, not commit time, so a high-water-mark would skip a
  late-committing lower id. Trawler claims rows with `FOR UPDATE SKIP LOCKED`
  and deletes them after a successful relay — nothing is skipped.
- **At-least-once.** A crash between XADD and DELETE re-relays the row. Each
  entry carries the capture-row **`id`** as its dedup token (the role `lsn`
  played in WALker). Redis stream IDs (assigned in relay order) remain the
  consumer-side ordering token.

### Poll loop (per cycle)

1. Begin a transaction.
2. `SELECT id, op, schema_name, table_name, data, old FROM cdc_events
    ORDER BY id LIMIT N FOR UPDATE SKIP LOCKED`.
3. For each claimed row: build `model.Change`, XADD to the table's configured
   stream.
4. `DELETE FROM cdc_events WHERE id = ANY(claimed_ids)`; commit.

## Sink — shared streams

- Per-table config names the **literal shared key** (e.g. `cdc.orders`). All
  Trawler instances across databases write the same key per logical event type.
- The origin instance is carried as a **`source`** field in the entry; the key
  no longer encodes it. `instance_id` exists in config solely to populate
  `source`.
- Entry fields: `op`, `table`, `schema`, `id`, `source`, `streamed_at`, `data`,
  and `old` (update/delete only).

## Config

### Env (secrets / connection / tuning)

| Env var | Description |
|---|---|
| `TRAWLER_PG_DSN` | Postgres connection (normal read pool, not replication) |
| `TRAWLER_REDIS_ADDR` | Redis address |
| `TRAWLER_CONFIG` | Path to the YAML catalog/table-mapping file |
| `TRAWLER_POLL_INTERVAL` | Idle poll cadence (default 1s) |
| `TRAWLER_BATCH_SIZE` | Max rows claimed per cycle (default 100) |
| `TRAWLER_ESCALATE_AFTER` | Consecutive transient failures before WARN→ERROR (default 10) |

### YAML (catalog + per-table mapping)

```yaml
instance_id: bayer-17909
cdc_table: public.cdc_events
tables:
  orders:
    emit: cdc.orders
  products:
    emit: cdc.products
```

## Error handling / back-pressure

- **Transient** (DB / Redis error): do **not** delete the claimed rows; roll
  back and retry the cycle with exponential backoff. The capture table holds the
  work, which is the natural back-pressure — a persistent stall grows
  `cdc_events` on the primary. Escalate log WARN → ERROR after N retries.
- **Fatal** (unmapped table, malformed capture row): fail loud, exit non-zero.
  The supervisor restarts; the row stays in `cdc_events` for inspection.

## Testing

- `internal/model`: Change-construction / jsonb-decode unit tests.
- `internal/sink`: XADD mapping unit tests with miniredis (shared key, `source`
  field, optional `old`, fan-in).
- `internal/cdc`: poll-loop tests with a fake store/emitter (claim → relay →
  delete ordering; transient-retry leaves rows; fatal exits).
- `internal/config`: env + YAML loader validation tests.

## Migration / sequencing

1. **Postgres**: add `cdc_events` + generic trigger function; attach triggers to
   captured tables (additive; no logical replication).
2. **Trawler**: this service.
3. **Retire WALker**: archive the logical-decoding service once Trawler is the
   production relay.
4. **Dasher** (separate change): consume literal shared keys.

## Implementation notes (v0)

These are the deliberate deviations from the original (enrichment-on-relay) plan:

1. **No enrichment.** There is no `internal/lookup` or `internal/enrich`
   package, no DB read beyond claiming/deleting capture rows, and no
   `lookups`/`enrich`/`cache` config. Enrichment moves to a separate
   microservice. Extra YAML keys are ignored, so an enrichment-bearing config
   still loads.
2. **No redundant index.** The capture table relies on the `id` PRIMARY KEY
   index for `ORDER BY id` and the SKIP LOCKED scan; the separately-proposed
   `cdc_events_id_idx` was dropped (it would only add write overhead on the hot
   insert path).
3. **Back-pressure lives in the relay loop, not the sink.** The sink does a
   single XADD and returns errors; the cdc loop rolls back the claim (releasing
   the SKIP LOCKED locks) and retries with backoff. This keeps the
   delete-only-after-durable-relay contract while letting the capture table be
   the buffer.
4. **XADD runs inside the open claim transaction.** Without enrichment the relay
   is fast, so there is no need to relay outside the transaction to avoid
   `idle-in-transaction`. Reconsider if/when slow enrichment is added.
5. **Unmapped table is fatal.** A captured row whose table has no `emit` mapping
   exits the process (config/trigger drift should be surfaced, not silently
   dropped).

## Open questions

1. Poll cadence + batch size defaults are env-tunable (1s / 100); revisit, and
   consider `NOTIFY`/`LISTEN` later for lower latency.
2. Capture-table retention beyond delete-after-relay (add a sweeper only if
   orphan rows appear).
