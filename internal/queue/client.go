package queue

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivermigrate"

	"github.com/ogen-app/river-service/internal/config"
	"github.com/ogen-app/river-service/internal/logging"
)

// migrationLockKey is the Postgres advisory-lock key migrations run under.
const migrationLockKey int64 = 0x72697672_6d690001 // "rivr" "mi" 1

// EnqueueKeysTable is the table JobService de-duplicates deliveries in, in
// schema.
func EnqueueKeysTable(schema string) string {
	return pgx.Identifier{schema, "enqueue_keys"}.Sanitize()
}

// Migrate installs or updates River's tables and the enqueue-keys table in
// schema. Replicas booting together migrate one at a time under an advisory
// lock. The schema must exist and belong to the service's role.
func Migrate(ctx context.Context, pool *pgxpool.Pool, schema string) error {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("migrate: get connection: %w", err)
	}
	defer conn.Release()

	if _, err := conn.Exec(ctx, "SELECT pg_advisory_lock($1)", migrationLockKey); err != nil {
		return fmt.Errorf("migrate: acquire lock: %w", err)
	}
	defer func() {
		if _, err := conn.Exec(context.WithoutCancel(ctx), "SELECT pg_advisory_unlock($1)", migrationLockKey); err != nil {
			slog.WarnContext(ctx, "migration unlock failed; closing the connection releases it",
				logging.AttrComponent, "migrate", logging.AttrError, err)
			_ = conn.Conn().Close(context.WithoutCancel(ctx))
		}
	}()

	// Both steps run on the locked connection, so a one-connection pool works.
	migrator, err := rivermigrate.New(riverpgxv5.New(nil), &rivermigrate.Config{Schema: schema})
	if err != nil {
		return fmt.Errorf("migrate: river migrator: %w", err)
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("migrate: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if _, err := migrator.MigrateTx(ctx, tx, rivermigrate.DirectionUp, nil); err != nil {
		return fmt.Errorf("migrate: river: %w", err)
	}
	if _, err := tx.Exec(ctx, `CREATE TABLE IF NOT EXISTS `+EnqueueKeysTable(schema)+` (
		idempotency_key text PRIMARY KEY,
		job_id bigint NOT NULL,
		created_at timestamptz NOT NULL DEFAULT now()
	)`); err != nil {
		return fmt.Errorf("migrate: enqueue keys: %w", err)
	}
	return tx.Commit(ctx)
}

// NewClient builds the River client. With no registered kinds it is an
// insert-only client: a client cannot work queues without workers.
func NewClient(pool *pgxpool.Pool, cfg *config.Config, reg *Registry, logger *slog.Logger) (*river.Client[pgx.Tx], error) {
	rc := &river.Config{
		Schema:               cfg.RiverSchema,
		Logger:               logger,
		Middleware:           Middleware(),
		RescueStuckJobsAfter: cfg.RescueStuckJobsAfter,
		// Past the soft stop, River cancels the jobs still running; the drain
		// middleware reschedules them for the replacement replica.
		SoftStopTimeout: cfg.EffectiveSoftStop(),
	}
	if reg.Len() > 0 && len(cfg.RiverQueues) > 0 {
		rc.Workers = reg.Workers()
		rc.PeriodicJobs = reg.PeriodicJobs()
		rc.Queues = map[string]river.QueueConfig{}
		for q, n := range cfg.QueueWorkers() {
			rc.Queues[q] = river.QueueConfig{MaxWorkers: n}
		}
	}
	return river.NewClient(riverpgxv5.New(pool), rc)
}

// Works reports whether a client built from cfg and reg works queues. An
// insert-only client is never started.
func Works(cfg *config.Config, reg *Registry) bool {
	return reg.Len() > 0 && len(cfg.RiverQueues) > 0
}
