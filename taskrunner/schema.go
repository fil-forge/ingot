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
	           CHECK (state IN ('pending', 'succeeded', 'failed', 'expired', 'cancelled')),
	result     bytea,
	error      text,
	finished   timestamptz,
	-- waiting_until is set while a sync task's caller waits in Run, which
	-- keeps workers off it until the claim lapses.
	waiting_until timestamptz,
	-- started is set while the task runs: then it can no longer be cancelled
	-- or replaced. A retry clears it.
	started     timestamptz,
	replaceable boolean NOT NULL DEFAULT false,
	attempts    integer NOT NULL DEFAULT 0,
	-- retry_at holds a retry that holds its subject (RetryPolicy.HoldsSubject).
	retry_at    timestamptz
);
CREATE INDEX %[3]s ON %[1]s (subject, seq) WHERE state = 'pending';
`+dueIndex(q)+`
CREATE UNIQUE INDEX %[4]s ON %[1]s (subject, action)
	WHERE state = 'pending' AND replaceable AND started IS NULL;
CREATE TABLE %[2]s (
	subject      text PRIMARY KEY,
	owner        uuid NOT NULL,
	expires      timestamptz NOT NULL,
	tree_created timestamptz NOT NULL,
	tree_id      uuid NOT NULL
);`, q.tasks, q.leases,
			pgx.Identifier{q.table + "_tasks_pending"}.Sanitize(),
			pgx.Identifier{q.table + "_tasks_replaceable"}.Sanitize())
	},
	// Placement.
	func(q *queue) string {
		return fmt.Sprintf(`ALTER TABLE %s ADD COLUMN node text;`, q.tasks)
	},
}

// dueIndex lets the poll for due tasks read only the pending tasks whose
// NotBefore has passed, or that have none, rather than every pending task: a
// queue can hold many more delayed tasks than due ones, such as an eviction
// pending for every cached blob.
func dueIndex(q *queue) string {
	return fmt.Sprintf(`CREATE INDEX %s ON %s (not_before) WHERE state = 'pending';`,
		pgx.Identifier{q.table + "_tasks_due"}.Sanitize(), q.tasks)
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
