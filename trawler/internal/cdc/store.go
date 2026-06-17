package cdc

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// pgStore is the Postgres-backed store. The capture-table identifier is
// sanitized once at construction so the poll/delete queries can interpolate it
// safely (it cannot be a bind parameter).
type pgStore struct {
	pool  *pgxpool.Pool
	table string // sanitized SQL identifier, e.g. "public"."cdc_events"
}

// New builds a Relay backed by a Postgres connection pool.
// cdcTable is the unquoted capture-table name ("schema.table" or "table");
// it must already be validated by config.
func New(pool *pgxpool.Pool, cdcTable string, streams map[string]string, emit Emitter, opts Options) *Relay {
	return newRelay(&pgStore{pool: pool, table: sanitizeTable(cdcTable)}, emit, streams, opts)
}

// sanitizeTable quotes a "schema.table" or "table" name into a safe SQL identifier.
func sanitizeTable(name string) string {
	parts := strings.SplitN(name, ".", 2)
	return pgx.Identifier(parts).Sanitize()
}

// Claim begins a transaction and claims up to n rows with FOR UPDATE SKIP
// LOCKED, ordered by id. The rows are fully read (so the connection is free for
// the subsequent Delete/Commit) before the transaction handle is returned.
func (s *pgStore) Claim(ctx context.Context, n int) ([]capturedRow, claimTx, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, nil, err
	}

	q := fmt.Sprintf(
		`SELECT id, op, schema_name, table_name, data, old
		 FROM %s ORDER BY id LIMIT $1 FOR UPDATE SKIP LOCKED`, s.table)
	rows, err := tx.Query(ctx, q, n)
	if err != nil {
		_ = tx.Rollback(ctx)
		return nil, nil, err
	}

	var claimed []capturedRow
	for rows.Next() {
		var cr capturedRow
		if err := rows.Scan(&cr.ID, &cr.Op, &cr.Schema, &cr.Table, &cr.Data, &cr.Old); err != nil {
			rows.Close()
			_ = tx.Rollback(ctx)
			return nil, nil, err
		}
		claimed = append(claimed, cr)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		_ = tx.Rollback(ctx)
		return nil, nil, err
	}

	return claimed, &pgTx{tx: tx, table: s.table}, nil
}

// pgTx is the transactional handle for one claimed batch.
type pgTx struct {
	tx    pgx.Tx
	table string
}

func (t *pgTx) Delete(ctx context.Context, ids []int64) error {
	q := fmt.Sprintf(`DELETE FROM %s WHERE id = ANY($1)`, t.table)
	_, err := t.tx.Exec(ctx, q, ids)
	return err
}

func (t *pgTx) Commit(ctx context.Context) error   { return t.tx.Commit(ctx) }
func (t *pgTx) Rollback(ctx context.Context) error { return t.tx.Rollback(ctx) }
