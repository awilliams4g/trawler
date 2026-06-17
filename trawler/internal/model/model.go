// Package model defines the in-flight representation of a single captured row
// change as it moves from the cdc_events capture table to a Redis stream.
package model

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// Row is a column name → value map for a single CDC row.
// Numeric values are carried as json.Number (exact decimal text) so that
// bigint/numeric columns survive the round-trip without float64 rounding.
type Row = map[string]any

// Change is one captured row change, built from a cdc_events row.
type Change struct {
	Op     string // "insert" | "update" | "delete"
	Schema string
	Table  string
	// ID is the cdc_events capture-row id. It is the at-least-once dedup token
	// carried to consumers (the role LSN played in WALker).
	ID   int64
	Data Row // full new row (insert/update) or full old row (delete)
	Old  Row // previous row (update/delete); nil for insert
}

// NewChange builds a Change from a raw cdc_events row. data and old are the
// jsonb columns as raw JSON bytes; old may be nil/empty (insert).
func NewChange(id int64, op, schema, table string, data, old []byte) (Change, error) {
	d, err := decodeRow(data)
	if err != nil {
		return Change{}, fmt.Errorf("decode data: %w", err)
	}
	o, err := decodeRow(old)
	if err != nil {
		return Change{}, fmt.Errorf("decode old: %w", err)
	}
	return Change{
		Op:     op,
		Schema: schema,
		Table:  table,
		ID:     id,
		Data:   d,
		Old:    o,
	}, nil
}

// decodeRow parses a jsonb payload into a Row, preserving numeric precision.
// Empty input (nil/zero-length, or a literal JSON null) yields a nil Row.
func decodeRow(raw []byte) (Row, error) {
	if len(raw) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil, nil
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var r Row
	if err := dec.Decode(&r); err != nil {
		return nil, err
	}
	return r, nil
}
