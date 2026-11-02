package taskrunner

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// queueSchema lists the DDL steps for one queue's tables, in order. A step is
// applied once per queue and recorded in the versions table, so a later
// change appends a step and never edits one.
var queueSchema = []func(q *queue) string{
	func(q *queue) string {
		return fmt.Sprintf(`
CREATE TABLE %[1]s (
	seq        bigint GENERATED ALWAYS AS IDENTITY UNIQUE,
	id         uuid PRIMARY KEY,
	subject    text NOT NULL,
	action     text NOT NULL,
	created    timestamptz NOT NULL DEFAULT clock_timestamp(),
	not_before timestamptz,
	expires    timestamptz,
	args       bytea,
	sync       boolean NOT NULL,
	state      text NOT NULL DEFAULT 'pending'
	           CHECK (state IN ('pending', 'succeeded', 'failed', 'expired')),
	result     bytea,
	error      text,
	finished   timestamptz
);
CREATE INDEX %[3]s ON %[1]s (subject, seq) WHERE state = 'pending';
CREATE TABLE %[2]s (
	subject      text PRIMARY KEY,
	owner        uuid NOT NULL,
	expires      timestamptz NOT NULL,
	tree_created timestamptz NOT NULL,
	tree_id      uuid NOT NULL
);`, q.tasks, q.leases, pgx.Identifier{q.table + "_tasks_pending"}.Sanitize())
	},
}

// Migrate creates or updates the tables of every queue in Options.Queues.
// Concurrent calls against one database serialize on a transaction-scoped
// advisory lock, so several processes may call it at startup.
func (r *Runner) Migrate(ctx context.Context) error {
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("migrate task queues: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext($1))`, "taskrunner:"+r.opts.Schema); err != nil {
		return fmt.Errorf("migrate task queues: lock: %w", err)
	}
	schema := pgx.Identifier{r.opts.Schema}.Sanitize()
	versions := pgx.Identifier{r.opts.Schema, "taskrunner_versions"}.Sanitize()
	if _, err := tx.Exec(ctx, `CREATE SCHEMA IF NOT EXISTS `+schema); err != nil {
		return fmt.Errorf("migrate task queues: schema: %w", err)
	}
	if _, err := tx.Exec(ctx, `CREATE TABLE IF NOT EXISTS `+versions+` (
		queue   text PRIMARY KEY,
		version integer NOT NULL
	)`); err != nil {
		return fmt.Errorf("migrate task queues: versions table: %w", err)
	}

	for _, q := range r.order {
		var version int
		err := tx.QueryRow(ctx, `SELECT version FROM `+versions+` WHERE queue = $1`, q.name).Scan(&version)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("migrate queue %s: read version: %w", q.name, err)
		}
		for ; version < len(queueSchema); version++ {
			if _, err := tx.Exec(ctx, queueSchema[version](q)); err != nil {
				return fmt.Errorf("migrate queue %s to version %d: %w", q.name, version+1, err)
			}
		}
		if _, err := tx.Exec(ctx, `INSERT INTO `+versions+` (queue, version) VALUES ($1, $2)
			ON CONFLICT (queue) DO UPDATE SET version = EXCLUDED.version`, q.name, version); err != nil {
			return fmt.Errorf("migrate queue %s: record version: %w", q.name, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("migrate task queues: commit: %w", err)
	}
	return nil
}
