// Package cdc implements the relay loop: it claims rows from the trigger-written
// capture table, builds a model.Change for each, emits it to the configured
// shared Redis stream, then deletes the claimed rows and commits.
//
// Delivery is at-least-once: a row is deleted only after a durable relay, so a
// crash between emit and delete re-relays the row (consumers dedup on the
// capture-row id). Nothing is skipped — claiming uses FOR UPDATE SKIP LOCKED
// rather than a high-water-mark, because cdc_events ids are assigned at
// statement time, not commit time.
package cdc

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"4gclinical.com/trawler/internal/model"
)

const (
	minBackoff = 100 * time.Millisecond
	maxBackoff = 30 * time.Second
)

// ErrUnmappedTable is returned when a captured row references a table that has
// no emit mapping in config. It is fatal: triggers must only be attached to
// configured tables, so this indicates a config/trigger drift worth surfacing.
var ErrUnmappedTable = errors.New("captured table has no emit mapping")

// FatalError marks an error as non-retryable. The relay loop exits on it
// (the supervisor restarts the process; the offending row stays in the capture
// table for inspection).
type FatalError struct{ err error }

func (e *FatalError) Error() string { return e.err.Error() }
func (e *FatalError) Unwrap() error { return e.err }

func fatal(err error) error { return &FatalError{err: err} }

// IsFatal reports whether err is (or wraps) a FatalError.
func IsFatal(err error) bool {
	var f *FatalError
	return errors.As(err, &f)
}

// capturedRow is one raw row read from the capture table.
type capturedRow struct {
	ID     int64
	Op     string
	Schema string
	Table  string
	Data   []byte // jsonb as raw JSON
	Old    []byte // jsonb as raw JSON; nil when SQL NULL
}

// store claims a batch of capture rows inside a transaction. The returned
// claimTx must be committed (after a successful relay) or rolled back.
type store interface {
	Claim(ctx context.Context, n int) ([]capturedRow, claimTx, error)
}

// claimTx is the transactional handle for a single claimed batch.
type claimTx interface {
	Delete(ctx context.Context, ids []int64) error
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
}

// Emitter writes one change to a resolved (literal) stream key.
type Emitter interface {
	Write(ctx context.Context, stream string, c model.Change) error
}

// Options carries the relay's loop tuning.
type Options struct {
	BatchSize     int
	PollInterval  time.Duration
	EscalateAfter int
}

// Relay owns the poll/claim/relay/delete loop.
type Relay struct {
	store         store
	emit          Emitter
	streams       map[string]string // table name → literal emit stream key
	batchSize     int
	pollInterval  time.Duration
	escalateAfter int
}

// newRelay builds a Relay around an arbitrary store (used by tests).
func newRelay(s store, emit Emitter, streams map[string]string, opts Options) *Relay {
	return &Relay{
		store:         s,
		emit:          emit,
		streams:       streams,
		batchSize:     opts.BatchSize,
		pollInterval:  opts.PollInterval,
		escalateAfter: opts.EscalateAfter,
	}
}

// Run drives the relay loop until ctx is cancelled or a fatal error occurs.
// Transient errors are retried with exponential backoff (the capture table is
// the natural back-pressure buffer). The caller should exit non-zero on a
// non-context error so the supervisor restarts.
func (r *Relay) Run(ctx context.Context) error {
	backoff := minBackoff
	failures := 0

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		n, err := r.cycle(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if IsFatal(err) {
				return err
			}
			failures++
			level := slog.LevelWarn
			if failures >= r.escalateAfter {
				level = slog.LevelError
			}
			slog.Log(ctx, level, "relay cycle failed; retrying",
				"err", err, "consecutive_failures", failures, "backoff", backoff)
			if !sleep(ctx, backoff) {
				return ctx.Err()
			}
			backoff = nextBackoff(backoff)
			continue
		}

		failures = 0
		backoff = minBackoff
		if n == 0 {
			// Idle: nothing to relay, wait a poll interval before re-checking.
			if !sleep(ctx, r.pollInterval) {
				return ctx.Err()
			}
		}
		// When n > 0 we loop immediately to drain any backlog.
	}
}

// cycle runs one claim → relay → delete → commit transaction. It returns the
// number of rows relayed. On any error the transaction is rolled back (nothing
// is deleted), leaving the work in the capture table.
func (r *Relay) cycle(ctx context.Context) (int, error) {
	rows, tx, err := r.store.Claim(ctx, r.batchSize)
	if err != nil {
		return 0, fmt.Errorf("claim: %w", err)
	}

	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback(ctx)
		}
	}()

	if len(rows) == 0 {
		return 0, nil
	}

	ids := make([]int64, 0, len(rows))
	for _, row := range rows {
		c, err := model.NewChange(row.ID, row.Op, row.Schema, row.Table, row.Data, row.Old)
		if err != nil {
			return 0, fatal(fmt.Errorf("build change id=%d: %w", row.ID, err))
		}
		stream, ok := r.streams[row.Table]
		if !ok {
			return 0, fatal(fmt.Errorf("%w: %s.%s (id=%d)", ErrUnmappedTable, row.Schema, row.Table, row.ID))
		}
		if err := r.emit.Write(ctx, stream, c); err != nil {
			return 0, fmt.Errorf("emit id=%d: %w", row.ID, err)
		}
		ids = append(ids, row.ID)
	}

	if err := tx.Delete(ctx, ids); err != nil {
		return 0, fmt.Errorf("delete: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit: %w", err)
	}
	committed = true
	return len(rows), nil
}

// nextBackoff doubles d up to maxBackoff.
func nextBackoff(d time.Duration) time.Duration {
	d *= 2
	if d > maxBackoff {
		return maxBackoff
	}
	return d
}

// sleep waits for d or until ctx is cancelled. Returns false if cancelled.
func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
