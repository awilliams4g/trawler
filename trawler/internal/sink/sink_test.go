package sink_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"4gclinical.com/trawler/internal/model"
	"4gclinical.com/trawler/internal/sink"
)

func newTestSink(t *testing.T) (*sink.Sink, *redis.Client) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	return sink.New(rdb, "bayer-17909"), rdb
}

func TestWriteInsertSharedKeyAndSource(t *testing.T) {
	s, rdb := newTestSink(t)
	ctx := context.Background()

	c := model.Change{
		Op: "insert", Schema: "public", Table: "orders", ID: 42,
		Data: map[string]any{"id": json.Number("1"), "status": "pending"},
	}
	require.NoError(t, s.Write(ctx, "cdc.orders", c))

	entries, err := rdb.XRange(ctx, "cdc.orders", "-", "+").Result()
	require.NoError(t, err)
	require.Len(t, entries, 1)

	f := entries[0].Values
	assert.Equal(t, "insert", f["op"])
	assert.Equal(t, "orders", f["table"])
	assert.Equal(t, "public", f["schema"])
	assert.Equal(t, "42", f["id"], "capture-row id is the dedup token")
	assert.Equal(t, "bayer-17909", f["source"], "origin instance is in source, not the key")
	assert.Contains(t, f, "streamed_at")
	assert.NotContains(t, f, "old", "insert has no old")

	var data map[string]any
	require.NoError(t, json.Unmarshal([]byte(f["data"].(string)), &data))
	assert.Equal(t, "pending", data["status"])
}

func TestWriteDeleteIncludesOld(t *testing.T) {
	s, rdb := newTestSink(t)
	ctx := context.Background()

	c := model.Change{
		Op: "delete", Schema: "public", Table: "products", ID: 7,
		Data: map[string]any{"id": json.Number("99")},
		Old:  map[string]any{"id": json.Number("99"), "name": "Gadget"},
	}
	require.NoError(t, s.Write(ctx, "cdc.products", c))

	entries, err := rdb.XRange(ctx, "cdc.products", "-", "+").Result()
	require.NoError(t, err)
	require.Len(t, entries, 1)

	f := entries[0].Values
	assert.Equal(t, "delete", f["op"])
	require.Contains(t, f, "old")
	var old map[string]any
	require.NoError(t, json.Unmarshal([]byte(f["old"].(string)), &old))
	assert.Equal(t, "Gadget", old["name"])
}

// Two source databases fan into the same shared key; both entries land there,
// distinguished only by their source field.
func TestWriteFanInToSharedKey(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	ctx := context.Background()

	a := sink.New(rdb, "bayer-1")
	b := sink.New(rdb, "bayer-2")
	c := model.Change{Op: "insert", Schema: "public", Table: "orders", ID: 1,
		Data: map[string]any{"id": json.Number("1")}}

	require.NoError(t, a.Write(ctx, "cdc.orders", c))
	require.NoError(t, b.Write(ctx, "cdc.orders", c))

	entries, err := rdb.XRange(ctx, "cdc.orders", "-", "+").Result()
	require.NoError(t, err)
	require.Len(t, entries, 2)
	assert.Equal(t, "bayer-1", entries[0].Values["source"])
	assert.Equal(t, "bayer-2", entries[1].Values["source"])
}
