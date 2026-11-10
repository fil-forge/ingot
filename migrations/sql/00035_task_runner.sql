-- +goose Up
-- The changes to ingot's own tables as its work moves onto the task runner
-- (fil-forge/ingot#328), one migration for the whole move. The runner's queue
-- tables are its own and created by it, not here. Nothing in the tables this
-- replaces is carried over: there is no deployment to keep, and dev and staging
-- can be reset.

-- Upload registrations are queue-space-upload tasks, queued in the commit's own
-- transaction, so the outbox they went through before is gone.
DROP TABLE ingot.upload_registrations;

-- A cached blob's lifetime is a pending queue-shard.evict task, and the budget
-- pass evicts from the cache directory itself, so intents no longer record
-- eviction.
DROP INDEX ingot.upload_intents_evictable_idx;
ALTER TABLE ingot.upload_intents DROP COLUMN evicted_at;

-- A streamed upload is no longer recorded from allocation to acceptance. One
-- that fails aborts its allocation at once, and one a dead request abandons
-- is expired by the provider, so the table and the stream sweeper are gone.
DROP INDEX ingot.blob_parks_add_task;
DROP TABLE ingot.blob_streams;

-- A blob's release is a queue-space-shard.release task, queued in the
-- transaction that drops its last claim, so its record table is gone.
DROP TABLE ingot.blob_release_intents;

-- +goose Down
-- In reverse order of the Up statements.
CREATE TABLE ingot.blob_release_intents (
    space      text        NOT NULL,
    digest     bytea       NOT NULL,
    not_before timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    started_at timestamptz,
    PRIMARY KEY (space, digest)
);
CREATE INDEX blob_release_intents_due_idx ON ingot.blob_release_intents (not_before);
CREATE INDEX blob_release_intents_digest_idx ON ingot.blob_release_intents (digest);

CREATE TABLE ingot.blob_streams (
    add_task   bytea PRIMARY KEY,
    space      text        NOT NULL,
    bucket     text        NOT NULL,
    size       bigint      NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    touched_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX blob_streams_touched_at ON ingot.blob_streams (touched_at);
CREATE INDEX blob_parks_add_task ON ingot.blob_parks (add_task);

ALTER TABLE ingot.upload_intents ADD COLUMN evicted_at timestamptz;
CREATE INDEX upload_intents_evictable_idx
    ON ingot.upload_intents (updated_at, digest, space)
    WHERE evicted_at IS NULL AND state IN ('parked', 'accepted', 'published');

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
