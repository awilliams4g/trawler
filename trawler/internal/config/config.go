// Package config loads Trawler's runtime configuration from environment
// variables (secrets / connection) and a YAML file (catalog + table mapping).
package config

import (
	"errors"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"time"

	"gopkg.in/yaml.v3"
)

// identRe matches a single SQL identifier (unquoted).
var identRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// qualifiedNameRe matches an optionally schema-qualified table name
// ("table" or "schema.table").
var qualifiedNameRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)?$`)

// Defaults for env-tunable values.
const (
	defaultPGDSN        = "postgres://postgres:postgres@localhost:5432/mydb"
	defaultRedisAddr    = "localhost:6380"
	defaultConfigPath   = "config.yaml"
	defaultCDCTable     = "public.cdc_events"
	defaultPollInterval = 1 * time.Second
	defaultBatchSize    = 100
	defaultEscalate     = 10
)

// Sentinel errors. Callers may use errors.Is to identify a failure kind.
var (
	ErrMissingInstanceID = errors.New("config: instance_id is required")
	ErrInvalidCDCTable   = errors.New("config: cdc_table is not a valid table name")
	ErrNoTables          = errors.New("config: at least one table mapping is required")
	ErrMissingEmit       = errors.New("config: table mapping is missing emit")
	ErrInvalidTableName  = errors.New("config: table key is not a valid identifier")
	ErrInvalidValue      = errors.New("config: invalid value")
	ErrBelowMinimum      = errors.New("config: value below minimum")
)

// TableMapping is the per-table relay configuration.
type TableMapping struct {
	// Emit is the literal shared Redis stream key for this table's events.
	Emit string `yaml:"emit"`
}

// fileConfig is the on-disk YAML shape.
type fileConfig struct {
	InstanceID string                  `yaml:"instance_id"`
	CDCTable   string                  `yaml:"cdc_table"`
	Tables     map[string]TableMapping `yaml:"tables"`
}

// Config is the resolved runtime configuration for this Trawler process.
type Config struct {
	PGDSN      string
	RedisAddr  string
	InstanceID string
	// CDCTable is the validated capture-table name ("schema.table" or "table").
	CDCTable string
	// Tables maps a captured table name to its literal shared emit stream key.
	Tables map[string]string

	PollInterval  time.Duration
	BatchSize     int
	EscalateAfter int
}

// Load reads env vars + the YAML config file and validates the result.
func Load() (Config, error) {
	path := getenv("TRAWLER_CONFIG", defaultConfigPath)
	raw, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read config %q: %w", path, err)
	}
	var f fileConfig
	if err := yaml.Unmarshal(raw, &f); err != nil {
		return Config{}, fmt.Errorf("parse config %q: %w", path, err)
	}

	if f.InstanceID == "" {
		return Config{}, ErrMissingInstanceID
	}

	cdcTable := f.CDCTable
	if cdcTable == "" {
		cdcTable = defaultCDCTable
	}
	if !qualifiedNameRe.MatchString(cdcTable) {
		return Config{}, fmt.Errorf("%w: %q", ErrInvalidCDCTable, cdcTable)
	}

	if len(f.Tables) == 0 {
		return Config{}, ErrNoTables
	}
	tables := make(map[string]string, len(f.Tables))
	for name, m := range f.Tables {
		if !identRe.MatchString(name) {
			return Config{}, fmt.Errorf("%w: %q", ErrInvalidTableName, name)
		}
		if m.Emit == "" {
			return Config{}, fmt.Errorf("%w: table %q", ErrMissingEmit, name)
		}
		tables[name] = m.Emit
	}

	pollInterval, err := parseDurationEnv("TRAWLER_POLL_INTERVAL", defaultPollInterval)
	if err != nil {
		return Config{}, err
	}
	batchSize, err := parsePositiveIntEnv("TRAWLER_BATCH_SIZE", defaultBatchSize)
	if err != nil {
		return Config{}, err
	}
	escalate, err := parsePositiveIntEnv("TRAWLER_ESCALATE_AFTER", defaultEscalate)
	if err != nil {
		return Config{}, err
	}

	return Config{
		PGDSN:         getenv("TRAWLER_PG_DSN", defaultPGDSN),
		RedisAddr:     getenv("TRAWLER_REDIS_ADDR", defaultRedisAddr),
		InstanceID:    f.InstanceID,
		CDCTable:      cdcTable,
		Tables:        tables,
		PollInterval:  pollInterval,
		BatchSize:     batchSize,
		EscalateAfter: escalate,
	}, nil
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// parseDurationEnv reads a positive time.Duration env var, returning def if unset.
func parseDurationEnv(key string, def time.Duration) (time.Duration, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("%s %q: %w", key, v, ErrInvalidValue)
	}
	if d <= 0 {
		return 0, fmt.Errorf("%s %q must be positive: %w", key, v, ErrBelowMinimum)
	}
	return d, nil
}

// parsePositiveIntEnv reads a positive int env var, returning def if unset.
func parsePositiveIntEnv(key string, def int) (int, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("%s %q: %w", key, v, ErrInvalidValue)
	}
	if n < 1 {
		return 0, fmt.Errorf("%s %q must be >= 1: %w", key, v, ErrBelowMinimum)
	}
	return n, nil
}
