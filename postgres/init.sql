-- Trawler local-dev schema: capture table, generic capture trigger, and a
-- couple of demo tables wired to the trigger.
--
-- In production these objects are added via a migration in the owning service
-- (e.g. Prancer); this file just provisions the local compose stack.

-- ── Capture table ────────────────────────────────────────────────────────────
-- One shared capture table (not per-table): ORDER BY id gives a single relay
-- order. The id sequence is assigned at statement time (not commit time), which
-- is why the relay claims rows with FOR UPDATE SKIP LOCKED and deletes them
-- after relay rather than tracking a high-water-mark.
CREATE TABLE cdc_events (
    id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    op          text        NOT NULL,   -- insert | update | delete
    schema_name text        NOT NULL,
    table_name  text        NOT NULL,
    data        jsonb       NOT NULL,   -- full new row (insert/update) or full old row (delete)
    old         jsonb,                  -- previous row (update/delete); NULL on insert
    created_at  timestamptz NOT NULL DEFAULT now()
);
-- NOTE: the PRIMARY KEY already provides the index used by `ORDER BY id` and the
-- SKIP LOCKED scan, so no extra index on (id) is created here.

-- ── Generic capture trigger ──────────────────────────────────────────────────
-- Serializes NEW/OLD to jsonb and inserts one cdc_events row inside the
-- originating transaction. Carries no business logic; a trigger failure fails
-- the user's write. AFTER row triggers ignore the return value (RETURN NULL).
CREATE OR REPLACE FUNCTION cdc_capture() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF (TG_OP = 'INSERT') THEN
        INSERT INTO cdc_events (op, schema_name, table_name, data, old)
        VALUES ('insert', TG_TABLE_SCHEMA, TG_TABLE_NAME, to_jsonb(NEW), NULL);
    ELSIF (TG_OP = 'UPDATE') THEN
        INSERT INTO cdc_events (op, schema_name, table_name, data, old)
        VALUES ('update', TG_TABLE_SCHEMA, TG_TABLE_NAME, to_jsonb(NEW), to_jsonb(OLD));
    ELSIF (TG_OP = 'DELETE') THEN
        INSERT INTO cdc_events (op, schema_name, table_name, data, old)
        VALUES ('delete', TG_TABLE_SCHEMA, TG_TABLE_NAME, to_jsonb(OLD), to_jsonb(OLD));
    END IF;
    RETURN NULL;
END;
$$;

-- ── Demo tables ──────────────────────────────────────────────────────────────
CREATE TABLE orders (
  id            SERIAL PRIMARY KEY,
  customer_name TEXT        NOT NULL,
  item          TEXT        NOT NULL,
  quantity      INT         NOT NULL DEFAULT 1,
  status        TEXT        NOT NULL DEFAULT 'pending',
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE products (
  id          SERIAL PRIMARY KEY,
  name        TEXT        NOT NULL,
  price_cents INT         NOT NULL,
  stock       INT         NOT NULL DEFAULT 0,
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ── Attach triggers to captured tables ───────────────────────────────────────
CREATE TRIGGER orders_cdc
  AFTER INSERT OR UPDATE OR DELETE ON orders
  FOR EACH ROW EXECUTE FUNCTION cdc_capture();

CREATE TRIGGER products_cdc
  AFTER INSERT OR UPDATE OR DELETE ON products
  FOR EACH ROW EXECUTE FUNCTION cdc_capture();

-- ── Seed data (also exercises the trigger on first boot) ─────────────────────
INSERT INTO products (name, price_cents, stock) VALUES
  ('Widget A', 999,  100),
  ('Widget B', 1499, 50),
  ('Gadget X', 2999, 25);

INSERT INTO orders (customer_name, item, quantity, status) VALUES
  ('Alice', 'Widget A', 2, 'pending'),
  ('Bob',   'Gadget X', 1, 'shipped');
