// Package sink maps a captured Change to a Redis stream entry (XADD).
package sink

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"4gclinical.com/trawler/internal/model"
)

// xAdder is the minimal Redis surface the sink needs. *redis.Client satisfies it.
type xAdder interface {
	XAdd(ctx context.Context, a *redis.XAddArgs) *redis.StringCmd
}

// Sink writes captured changes to shared (non-instance-scoped) Redis streams.
// The origin instance is carried in the `source` field rather than the key, so
// many source databases can fan into one stream per logical event type.
type Sink struct {
	rdb    xAdder
	source string
}

// New creates a Sink. source is the origin instance_id stamped on every entry.
func New(rdb xAdder, source string) *Sink {
	return &Sink{rdb: rdb, source: source}
}

// Write XADDs one change to the given (already-resolved, literal) stream key.
//
// A single XADD is attempted; transient Redis errors are returned to the caller,
// which owns back-pressure (the cdc relay does not delete the capture row unless
// this returns nil, so the work is retried from the capture table). This upholds
// the at-least-once contract: a row is deleted only after a durable relay.
func (s *Sink) Write(ctx context.Context, stream string, c model.Change) error {
	dataJSON, err := json.Marshal(c.Data)
	if err != nil {
		return fmt.Errorf("marshal data: %w", err)
	}

	id := strconv.FormatInt(c.ID, 10)
	values := []any{
		"op", c.Op,
		"table", c.Table,
		"schema", c.Schema,
		"id", id,
		// lsn is a transitional alias of id, emitted so consumers still keying
		// on WALker's `lsn` field (the role id now plays) keep working. Remove
		// once all consumers dedup on `id`.
		"lsn", id,
		"source", s.source,
		"streamed_at", time.Now().UTC().Format(time.RFC3339),
		"data", string(dataJSON),
	}

	if c.Old != nil {
		oldJSON, err := json.Marshal(c.Old)
		if err != nil {
			return fmt.Errorf("marshal old: %w", err)
		}
		values = append(values, "old", string(oldJSON))
	}

	args := &redis.XAddArgs{Stream: stream, ID: "*", Values: values}
	if err := s.rdb.XAdd(ctx, args).Err(); err != nil {
		return fmt.Errorf("xadd %q: %w", stream, err)
	}
	return nil
}
