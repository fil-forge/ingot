-- +goose Up
-- The gateway counts its own objects, and reports the counts to the auth
-- service with the rest of its usage, where it used to register every object
-- version with the upload service so that it could be counted there.
--
-- A bucket counts what the S3 object count counts: every version of every
-- key, delete markers included. It keeps two totals over its life, the
-- objects it has gained and the objects it has lost, which a commit adds to
-- with the versions it creates and discards. Each only grows, and the bucket
-- holds the difference. A bucket that exists already starts both at zero, so
-- its count is wrong until its data is reset.
ALTER TABLE ingot.buckets
    ADD COLUMN objects_added bigint NOT NULL DEFAULT 0 CHECK (objects_added >= 0),
    ADD COLUMN objects_removed bigint NOT NULL DEFAULT 0 CHECK (objects_removed >= 0);

-- A bucket's two totals as of each hour in which they changed, by the
-- bucket's space, and which of them the auth service has acknowledged. A
-- change copies the bucket's totals to the current hour's row in its own
-- transaction, so the row exists exactly when the change does.
--
-- added_reported and removed_reported are the totals the service
-- acknowledged. A report carries an hour's totals so far and the service
-- keeps the largest of each, so a row is due whenever either differs, the
-- hour in progress included. dirty_since is when the row last became due,
-- for the age the reporter's gauge shows; it is NULL while the row is up to
-- date, and reported_at is when it last got there. The rows outlive their
-- bucket, whose last changes are reported after it has gone.
CREATE TABLE ingot.object_usage (
    space            text        NOT NULL,
    hour             timestamptz NOT NULL,
    added            bigint      NOT NULL CHECK (added >= 0),
    removed          bigint      NOT NULL CHECK (removed >= 0),
    added_reported   bigint      NOT NULL DEFAULT 0 CHECK (added_reported >= 0),
    removed_reported bigint      NOT NULL DEFAULT 0 CHECK (removed_reported >= 0),
    dirty_since      timestamptz,
    reported_at      timestamptz,
    PRIMARY KEY (space, hour),
    CHECK ((dirty_since IS NULL) = (added = added_reported AND removed = removed_reported))
);

-- The reporter reads the rows that are due, oldest hour first.
CREATE INDEX object_usage_due_idx ON ingot.object_usage (hour, space)
    WHERE added <> added_reported OR removed <> removed_reported;

-- queue-space-upload sent the registrations and is gone. The task runner
-- created its tables and no longer knows the queue, so they are dropped here,
-- with whatever was still queued: the upload service's count is not read any
-- more. A database the runner has not been started on has none of them.
DROP TABLE IF EXISTS ingot.queue_space_upload_tasks;
DROP TABLE IF EXISTS ingot.queue_space_upload_leases;
-- +goose StatementBegin
DO $$
BEGIN
    IF to_regclass('ingot.taskrunner_versions') IS NOT NULL THEN
        DELETE FROM ingot.taskrunner_versions WHERE queue = 'queue-space-upload';
    END IF;
END
$$;
-- +goose StatementEnd

-- +goose Down
DROP TABLE ingot.object_usage;
ALTER TABLE ingot.buckets
    DROP COLUMN objects_added,
    DROP COLUMN objects_removed;
