-- +goose Up
-- The changes to ingot's own tables as its work moves onto the task runner
-- (fil-forge/ingot#328), one migration for the whole move. The runner's queue
-- tables are its own and created by it, not here. Nothing in the tables this
-- replaces is carried over: there is no deployment to keep, and dev and staging
-- can be reset.

-- Upload registrations are queue-space-upload tasks, queued in the commit's own
-- transaction, so the outbox they went through before is gone.
DROP TABLE ingot.upload_registrations;

-- +goose Down
-- In reverse order of the Up statements.
CREATE TABLE ingot.upload_registrations (
    seq         bigserial PRIMARY KEY,
    bucket      text        NOT NULL,
    object_key  text        NOT NULL,
    space       text        NOT NULL,
    root        bytea       NOT NULL,
    op          text        NOT NULL CHECK (op IN ('add', 'remove')),
    proofs      bytea       NOT NULL,
    attempts    integer     NOT NULL DEFAULT 0,
    next_at     timestamptz NOT NULL DEFAULT now(),
    created_at  timestamptz NOT NULL DEFAULT now(),
    dead_lettered_at   timestamptz,
    dead_letter_reason text,
    FOREIGN KEY (bucket) REFERENCES ingot.buckets (name) ON DELETE CASCADE
);
CREATE INDEX upload_registrations_due_idx ON ingot.upload_registrations (next_at, seq)
    WHERE dead_lettered_at IS NULL;
CREATE INDEX upload_registrations_key_idx ON ingot.upload_registrations (bucket, object_key, seq)
    WHERE dead_lettered_at IS NULL;
