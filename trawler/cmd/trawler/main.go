package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"4gclinical.com/trawler/internal/cdc"
	"4gclinical.com/trawler/internal/config"
	"4gclinical.com/trawler/internal/sink"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		slog.Error("invalid configuration", "err", err)
		os.Exit(1)
	}

	slog.Info("Trawler starting",
		"instance_id", cfg.InstanceID,
		"cdc_table", cfg.CDCTable,
		"tables", len(cfg.Tables),
		"redis", cfg.RedisAddr,
		"poll_interval", cfg.PollInterval,
		"batch_size", cfg.BatchSize,
	)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := pgxpool.New(ctx, cfg.PGDSN)
	if err != nil {
		slog.Error("connect postgres", "err", err)
		os.Exit(1)
	}
	defer pool.Close()

	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	defer rdb.Close()

	s := sink.New(rdb, cfg.InstanceID)
	relay := cdc.New(pool, cfg.CDCTable, cfg.Tables, s, cdc.Options{
		BatchSize:     cfg.BatchSize,
		PollInterval:  cfg.PollInterval,
		EscalateAfter: cfg.EscalateAfter,
	})

	if err := relay.Run(ctx); err != nil && ctx.Err() == nil {
		slog.Error("relay exited", "err", err)
		os.Exit(1)
	}
	slog.Info("Trawler stopped")
}
