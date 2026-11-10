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

-- A part's blobs are parked before its row is written, so a part starts
-- 'parked'. A part still spooled has no parked blobs and is dropped; the
-- client uploads it again.
ALTER TABLE ingot.multipart_parts DROP CONSTRAINT multipart_parts_state_check;
DELETE FROM ingot.multipart_parts WHERE state = 'spooled';
ALTER TABLE ingot.multipart_parts
    ADD CONSTRAINT multipart_parts_state_check
        CHECK (state IN ('parked','accepted')),
    ALTER COLUMN state SET DEFAULT 'parked';

-- A parked part lists the blobs still to be accepted. Each blob's accept takes
-- its digest off the list, and the part is accepted once the list is empty, so
-- marking a part costs each accept one update of the part's row rather than a
-- check of every blob the part names.
ALTER TABLE ingot.multipart_parts ADD COLUMN unaccepted bytea[] NOT NULL DEFAULT '{}';
UPDATE ingot.multipart_parts SET unaccepted = blob_digests WHERE state = 'parked';

-- Every body blob streams to its provider as the body arrives, so an intent
-- starts 'uploading', none is 'spooled', and there is no local path for it to
-- record. A stalled upload is one left 'uploading'.
DROP INDEX ingot.upload_intents_stalled_idx;
DELETE FROM ingot.upload_intents WHERE state = 'spooled';
ALTER TABLE ingot.upload_intents
    DROP CONSTRAINT upload_intents_state_check,
    ADD CONSTRAINT upload_intents_state_check
        CHECK (state IN ('uploading','parked','accepted','published')),
    DROP COLUMN local_path;
CREATE INDEX upload_intents_stalled_idx
    ON ingot.upload_intents (updated_at) INCLUDE (size)
    WHERE state = 'uploading';

-- A write's use of a blob is a pending reference, taken by its
-- queue-space-shard.add, until the write's commit or part row takes it over or
-- its queue-space-shard.expire drops it at the deadline.
CREATE TABLE ingot.blob_pending_refs (
    add_task    uuid        PRIMARY KEY,
    expire_task uuid        NOT NULL UNIQUE,
    space       text        NOT NULL,
    digest      bytea       NOT NULL,
    deadline    timestamptz NOT NULL
);
CREATE INDEX blob_pending_refs_blob ON ingot.blob_pending_refs (space, digest);

-- Every change to a multipart upload runs on its subject on the task queue,
-- so an upload is open until a Complete commits it or an Abort deletes it.
-- A session caught mid-transition is aborted with the others the stack drops.
DELETE FROM ingot.multipart_sessions WHERE state IN ('completing', 'aborting');
ALTER TABLE ingot.multipart_sessions
    DROP CONSTRAINT multipart_sessions_state_check,
    ADD CONSTRAINT multipart_sessions_state_check CHECK (state IN ('open','completed'));

-- A bucket being deleted is closed first, so no commit or new multipart
-- upload gets in while its uploads are aborted.
ALTER TABLE ingot.buckets ADD COLUMN closing boolean NOT NULL DEFAULT false;

-- Indexes no query reads: every lookup of a blob's location names its space,
-- and a segment's op-roots are read by the segment.
DROP INDEX ingot.blob_locations_digest_idx;
DROP INDEX ingot.segment_op_roots_bucket_seq_idx;

-- A bucket is its own space: Hilt mints one per bucket at create, so no two
-- buckets share one. The commits that drop a space's claims are therefore one
-- bucket's commits, which run one at a time, and a release queued only when
-- no claim is left relies on that. The index replaces the plain one no query
-- read.
DROP INDEX ingot.buckets_space_idx;
CREATE UNIQUE INDEX buckets_space_key ON ingot.buckets (space);

-- +goose Down
-- In reverse order of the Up statements.
DROP INDEX ingot.buckets_space_key;
CREATE INDEX buckets_space_idx ON ingot.buckets (space);
CREATE INDEX segment_op_roots_bucket_seq_idx ON ingot.segment_op_roots (bucket, seq);
CREATE INDEX blob_locations_digest_idx ON ingot.blob_locations (digest);
ALTER TABLE ingot.buckets DROP COLUMN closing;
ALTER TABLE ingot.multipart_sessions
    DROP CONSTRAINT multipart_sessions_state_check,
    ADD CONSTRAINT multipart_sessions_state_check
        CHECK (state IN ('open','completing','aborting','completed'));
DROP TABLE ingot.blob_pending_refs;
DROP INDEX ingot.upload_intents_stalled_idx;
ALTER TABLE ingot.upload_intents
    ADD COLUMN local_path text NOT NULL DEFAULT '',
    DROP CONSTRAINT upload_intents_state_check,
    ADD CONSTRAINT upload_intents_state_check
        CHECK (state IN ('spooled','uploading','parked','accepted','published'));
ALTER TABLE ingot.upload_intents ALTER COLUMN local_path DROP DEFAULT;
CREATE INDEX upload_intents_stalled_idx
    ON ingot.upload_intents (updated_at) INCLUDE (size, state)
    WHERE state IN ('spooled', 'uploading');
ALTER TABLE ingot.multipart_parts DROP COLUMN unaccepted;
ALTER TABLE ingot.multipart_parts DROP CONSTRAINT multipart_parts_state_check;
ALTER TABLE ingot.multipart_parts
    ADD CONSTRAINT multipart_parts_state_check
        CHECK (state IN ('spooled','parked','accepted')),
    ALTER COLUMN state SET DEFAULT 'spooled';
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
