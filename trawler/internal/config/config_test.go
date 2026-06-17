package config_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"4gclinical.com/trawler/internal/config"
)

// writeConfig writes body to a temp file and points TRAWLER_CONFIG at it.
func writeConfig(t *testing.T, body string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte(body), 0o600))
	t.Setenv("TRAWLER_CONFIG", path)
}

const validBody = `
instance_id: bayer-17909
cdc_table: public.cdc_events
tables:
  orders:
    emit: cdc.orders
  user_role_grant:
    emit: enriched.user_role_grant
`

func TestLoadDefaults(t *testing.T) {
	writeConfig(t, validBody)
	// Ensure env tunables are unset.
	t.Setenv("TRAWLER_PG_DSN", "")
	t.Setenv("TRAWLER_REDIS_ADDR", "")
	t.Setenv("TRAWLER_POLL_INTERVAL", "")
	t.Setenv("TRAWLER_BATCH_SIZE", "")
	t.Setenv("TRAWLER_ESCALATE_AFTER", "")

	cfg, err := config.Load()
	require.NoError(t, err)

	assert.Equal(t, "postgres://postgres:postgres@localhost:5432/mydb", cfg.PGDSN)
	assert.Equal(t, "localhost:6380", cfg.RedisAddr)
	assert.Equal(t, "bayer-17909", cfg.InstanceID)
	assert.Equal(t, "public.cdc_events", cfg.CDCTable)
	assert.Equal(t, "cdc.orders", cfg.Tables["orders"])
	assert.Equal(t, "enriched.user_role_grant", cfg.Tables["user_role_grant"])
	assert.Equal(t, time.Second, cfg.PollInterval)
	assert.Equal(t, 100, cfg.BatchSize)
	assert.Equal(t, 10, cfg.EscalateAfter)
}

func TestLoadEnvOverrides(t *testing.T) {
	writeConfig(t, validBody)
	t.Setenv("TRAWLER_PG_DSN", "postgres://u:p@db:5432/x")
	t.Setenv("TRAWLER_REDIS_ADDR", "redis:6379")
	t.Setenv("TRAWLER_POLL_INTERVAL", "250ms")
	t.Setenv("TRAWLER_BATCH_SIZE", "500")
	t.Setenv("TRAWLER_ESCALATE_AFTER", "3")

	cfg, err := config.Load()
	require.NoError(t, err)

	assert.Equal(t, "postgres://u:p@db:5432/x", cfg.PGDSN)
	assert.Equal(t, "redis:6379", cfg.RedisAddr)
	assert.Equal(t, 250*time.Millisecond, cfg.PollInterval)
	assert.Equal(t, 500, cfg.BatchSize)
	assert.Equal(t, 3, cfg.EscalateAfter)
}

func TestCDCTableDefaultsWhenOmitted(t *testing.T) {
	writeConfig(t, `
instance_id: x
tables:
  orders:
    emit: cdc.orders
`)
	cfg, err := config.Load()
	require.NoError(t, err)
	assert.Equal(t, "public.cdc_events", cfg.CDCTable)
}

func TestMissingInstanceID(t *testing.T) {
	writeConfig(t, `
tables:
  orders:
    emit: cdc.orders
`)
	_, err := config.Load()
	require.ErrorIs(t, err, config.ErrMissingInstanceID)
}

func TestNoTables(t *testing.T) {
	writeConfig(t, `instance_id: x`)
	_, err := config.Load()
	require.ErrorIs(t, err, config.ErrNoTables)
}

func TestMissingEmit(t *testing.T) {
	writeConfig(t, `
instance_id: x
tables:
  orders: {}
`)
	_, err := config.Load()
	require.ErrorIs(t, err, config.ErrMissingEmit)
}

func TestInvalidCDCTable(t *testing.T) {
	writeConfig(t, `
instance_id: x
cdc_table: "public.cdc; DROP TABLE"
tables:
  orders:
    emit: cdc.orders
`)
	_, err := config.Load()
	require.ErrorIs(t, err, config.ErrInvalidCDCTable)
}

func TestInvalidTableName(t *testing.T) {
	writeConfig(t, `
instance_id: x
tables:
  "bad name":
    emit: cdc.orders
`)
	_, err := config.Load()
	require.ErrorIs(t, err, config.ErrInvalidTableName)
}

func TestInvalidPollInterval(t *testing.T) {
	writeConfig(t, validBody)
	t.Setenv("TRAWLER_POLL_INTERVAL", "nope")
	_, err := config.Load()
	require.ErrorIs(t, err, config.ErrInvalidValue)
}

func TestNonPositiveBatchSize(t *testing.T) {
	writeConfig(t, validBody)
	t.Setenv("TRAWLER_BATCH_SIZE", "0")
	_, err := config.Load()
	require.ErrorIs(t, err, config.ErrBelowMinimum)
}

func TestMissingConfigFile(t *testing.T) {
	t.Setenv("TRAWLER_CONFIG", filepath.Join(t.TempDir(), "does-not-exist.yaml"))
	_, err := config.Load()
	require.Error(t, err)
}
