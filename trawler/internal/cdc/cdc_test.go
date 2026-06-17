package cdc

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"4gclinical.com/trawler/internal/model"
)

// ---------------------------------------------------------------------------
// Fakes
// ---------------------------------------------------------------------------

type fakeTx struct {
	deleted    []int64
	committed  bool
	rolledBack bool
	deleteErr  error
	commitErr  error
}

func (t *fakeTx) Delete(_ context.Context, ids []int64) error {
	if t.deleteErr != nil {
		return t.deleteErr
	}
	t.deleted = append([]int64(nil), ids...)
	return nil
}

func (t *fakeTx) Commit(_ context.Context) error {
	if t.commitErr != nil {
		return t.commitErr
	}
	t.committed = true
	return nil
}

func (t *fakeTx) Rollback(_ context.Context) error {
	t.rolledBack = true
	return nil
}

type fakeStore struct {
	batches  [][]capturedRow
	txs      []*fakeTx
	claimErr error
	idx      int
}

func (s *fakeStore) Claim(_ context.Context, _ int) ([]capturedRow, claimTx, error) {
	if s.claimErr != nil {
		return nil, nil, s.claimErr
	}
	var batch []capturedRow
	if s.idx < len(s.batches) {
		batch = s.batches[s.idx]
	}
	s.idx++
	tx := &fakeTx{}
	s.txs = append(s.txs, tx)
	return batch, tx, nil
}

type writeRecord struct {
	stream string
	change model.Change
}

type fakeEmitter struct {
	writes []writeRecord
	err    error
}

func (e *fakeEmitter) Write(_ context.Context, stream string, c model.Change) error {
	if e.err != nil {
		return e.err
	}
	e.writes = append(e.writes, writeRecord{stream: stream, change: c})
	return nil
}

func insertRow(id int64, table string) capturedRow {
	return capturedRow{ID: id, Op: "insert", Schema: "public", Table: table, Data: []byte(`{"id":1}`)}
}

func ordersRelay(s store, e Emitter) *Relay {
	return newRelay(s, e, map[string]string{"orders": "cdc.orders"},
		Options{BatchSize: 10, PollInterval: time.Millisecond, EscalateAfter: 5})
}

// ---------------------------------------------------------------------------
// cycle()
// ---------------------------------------------------------------------------

func TestCycleRelaysInOrderThenDeletesAndCommits(t *testing.T) {
	store := &fakeStore{batches: [][]capturedRow{{
		insertRow(1, "orders"),
		{ID: 2, Op: "update", Schema: "public", Table: "orders",
			Data: []byte(`{"id":2}`), Old: []byte(`{"id":2}`)},
	}}}
	emit := &fakeEmitter{}
	r := ordersRelay(store, emit)

	n, err := r.cycle(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 2, n)

	require.Len(t, emit.writes, 2)
	assert.Equal(t, "cdc.orders", emit.writes[0].stream)
	assert.Equal(t, int64(1), emit.writes[0].change.ID)
	assert.Equal(t, int64(2), emit.writes[1].change.ID)

	tx := store.txs[0]
	assert.Equal(t, []int64{1, 2}, tx.deleted, "claimed ids are deleted after relay")
	assert.True(t, tx.committed)
	assert.False(t, tx.rolledBack)
}

func TestCycleEmptyBatchCommitsNothing(t *testing.T) {
	store := &fakeStore{batches: [][]capturedRow{nil}}
	emit := &fakeEmitter{}
	r := ordersRelay(store, emit)

	n, err := r.cycle(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 0, n)
	assert.Empty(t, emit.writes)
	assert.True(t, store.txs[0].rolledBack, "empty batch releases the txn without deleting")
}

func TestCycleEmitErrorIsTransientAndLeavesRows(t *testing.T) {
	store := &fakeStore{batches: [][]capturedRow{{insertRow(1, "orders")}}}
	emit := &fakeEmitter{err: errors.New("redis unavailable")}
	r := ordersRelay(store, emit)

	_, err := r.cycle(context.Background())
	require.Error(t, err)
	assert.False(t, IsFatal(err), "transient errors must not be fatal")

	tx := store.txs[0]
	assert.Nil(t, tx.deleted, "no rows deleted when relay fails")
	assert.False(t, tx.committed)
	assert.True(t, tx.rolledBack)
}

func TestCycleUnmappedTableIsFatal(t *testing.T) {
	store := &fakeStore{batches: [][]capturedRow{{insertRow(1, "widgets")}}}
	emit := &fakeEmitter{}
	r := ordersRelay(store, emit)

	_, err := r.cycle(context.Background())
	require.Error(t, err)
	assert.True(t, IsFatal(err))
	assert.ErrorIs(t, err, ErrUnmappedTable)
	assert.Empty(t, emit.writes, "unmapped row is never emitted")
	assert.True(t, store.txs[0].rolledBack)
}

func TestCycleMalformedJSONIsFatal(t *testing.T) {
	store := &fakeStore{batches: [][]capturedRow{{
		{ID: 1, Op: "insert", Schema: "public", Table: "orders", Data: []byte(`not json`)},
	}}}
	emit := &fakeEmitter{}
	r := ordersRelay(store, emit)

	_, err := r.cycle(context.Background())
	require.Error(t, err)
	assert.True(t, IsFatal(err))
	assert.True(t, store.txs[0].rolledBack)
}

func TestCycleClaimErrorIsTransient(t *testing.T) {
	store := &fakeStore{claimErr: errors.New("db down")}
	emit := &fakeEmitter{}
	r := ordersRelay(store, emit)

	_, err := r.cycle(context.Background())
	require.Error(t, err)
	assert.False(t, IsFatal(err))
}

func TestCycleDeleteErrorIsTransient(t *testing.T) {
	ds := &deleteFailStore{rows: []capturedRow{insertRow(1, "orders")}}
	r := ordersRelay(ds, &fakeEmitter{})

	_, err := r.cycle(context.Background())
	require.Error(t, err)
	assert.False(t, IsFatal(err))
	assert.True(t, ds.tx.rolledBack)
	assert.False(t, ds.tx.committed)
}

// deleteFailStore returns a tx whose Delete always fails.
type deleteFailStore struct {
	rows []capturedRow
	tx   *fakeTx
}

func (s *deleteFailStore) Claim(_ context.Context, _ int) ([]capturedRow, claimTx, error) {
	s.tx = &fakeTx{deleteErr: errors.New("delete failed")}
	return s.rows, s.tx, nil
}

// ---------------------------------------------------------------------------
// Run()
// ---------------------------------------------------------------------------

func TestRunExitsOnFatal(t *testing.T) {
	store := &fakeStore{batches: [][]capturedRow{{insertRow(1, "widgets")}}}
	r := ordersRelay(store, &fakeEmitter{})

	err := r.Run(context.Background())
	require.Error(t, err)
	assert.True(t, IsFatal(err))
	assert.ErrorIs(t, err, ErrUnmappedTable)
}

// cancelingStore cancels its context during Claim so Run unwinds deterministically.
type cancelingStore struct {
	cancel context.CancelFunc
}

func (s *cancelingStore) Claim(_ context.Context, _ int) ([]capturedRow, claimTx, error) {
	s.cancel()
	return nil, &fakeTx{}, nil
}

func TestRunStopsOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	store := &cancelingStore{cancel: cancel}
	r := newRelay(store, &fakeEmitter{}, nil,
		Options{BatchSize: 10, PollInterval: time.Hour, EscalateAfter: 5})

	err := r.Run(ctx)
	require.ErrorIs(t, err, context.Canceled)
}

// TestRunRetriesTransientThenDrains verifies a transient failure is retried and
// the loop later makes progress, exiting cleanly when the context ends.
func TestRunRetriesTransientThenDrains(t *testing.T) {
	store := &flakyStore{}
	r := newRelay(store, &fakeEmitter{}, map[string]string{"orders": "cdc.orders"},
		Options{BatchSize: 10, PollInterval: 5 * time.Millisecond, EscalateAfter: 100})

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	err := r.Run(ctx)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.GreaterOrEqual(t, store.claims, 2, "expected at least one retry after the transient error")
}

// flakyStore fails the first claim, then always returns an empty batch.
type flakyStore struct {
	claims int
}

func (s *flakyStore) Claim(_ context.Context, _ int) ([]capturedRow, claimTx, error) {
	s.claims++
	if s.claims == 1 {
		return nil, nil, errors.New("transient db error")
	}
	return nil, &fakeTx{}, nil
}
