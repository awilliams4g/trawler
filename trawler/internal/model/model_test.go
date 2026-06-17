package model_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"4gclinical.com/trawler/internal/model"
)

func TestNewChangeInsert(t *testing.T) {
	c, err := model.NewChange(7, "insert", "public", "orders",
		[]byte(`{"id":1,"status":"pending"}`), nil)
	require.NoError(t, err)

	assert.Equal(t, "insert", c.Op)
	assert.Equal(t, "public", c.Schema)
	assert.Equal(t, "orders", c.Table)
	assert.Equal(t, int64(7), c.ID)
	assert.Equal(t, json.Number("1"), c.Data["id"])
	assert.Equal(t, "pending", c.Data["status"])
	assert.Nil(t, c.Old)
}

func TestNewChangeUpdateHasOld(t *testing.T) {
	c, err := model.NewChange(8, "update", "public", "orders",
		[]byte(`{"id":1,"status":"shipped"}`),
		[]byte(`{"id":1,"status":"pending"}`))
	require.NoError(t, err)

	assert.Equal(t, "shipped", c.Data["status"])
	require.NotNil(t, c.Old)
	assert.Equal(t, "pending", c.Old["status"])
}

func TestNewChangeDeleteCarriesFullOld(t *testing.T) {
	c, err := model.NewChange(9, "delete", "public", "products",
		[]byte(`{"id":99,"name":"Gadget"}`),
		[]byte(`{"id":99,"name":"Gadget"}`))
	require.NoError(t, err)

	assert.Equal(t, json.Number("99"), c.Data["id"])
	require.NotNil(t, c.Old)
	assert.Equal(t, "Gadget", c.Old["name"])
}

// Bigint values must round-trip as exact decimal text, never float64.
func TestNewChangePreservesBigintPrecision(t *testing.T) {
	c, err := model.NewChange(1, "insert", "public", "orders",
		[]byte(`{"id":9007199254740993}`), nil)
	require.NoError(t, err)
	assert.Equal(t, json.Number("9007199254740993"), c.Data["id"])
}

// A literal JSON null in old is treated as absent.
func TestNewChangeNullOldIsNil(t *testing.T) {
	c, err := model.NewChange(1, "insert", "public", "orders",
		[]byte(`{"id":1}`), []byte(`null`))
	require.NoError(t, err)
	assert.Nil(t, c.Old)
}

func TestNewChangeMalformedDataErrors(t *testing.T) {
	_, err := model.NewChange(1, "insert", "public", "orders",
		[]byte(`not json`), nil)
	require.Error(t, err)
}
