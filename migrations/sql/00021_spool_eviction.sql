-- +goose Up
-- Spool eviction: the budget sweeper removes a blob's local file once the
-- provider holds it, and records that here. An evicted blob keeps its intent
-- row and its state: a session's release recognises a committed part blob by
-- 'published', and Complete reads part sizes from intents.

-- NULL means the file is on disk as far as the table knows. PutIntent clears
-- it when a digest is spooled again.
ALTER TABLE ingot.upload_intents
    ADD COLUMN evicted_at timestamptz;

-- The sweeper's candidate scan: intents still on disk in a state that can be
-- evictable, oldest state change first.
CREATE INDEX upload_intents_evictable_idx
    ON ingot.upload_intents (updated_at, digest)
    WHERE evicted_at IS NULL AND state IN ('parked', 'accepted', 'published');

-- The sweeper's sum of stalled uploads: bodies still waiting for upload long
-- after any request that wrote them has ended.
CREATE INDEX upload_intents_stalled_idx
    ON ingot.upload_intents (updated_at) INCLUDE (size)
    WHERE state IN ('spooled', 'uploading');

-- The candidate scan asks whether a digest has a location in any space; the
-- primary key leads with space.
CREATE INDEX blob_locations_digest_idx ON ingot.blob_locations (digest);

-- +goose Down
DROP INDEX ingot.blob_locations_digest_idx;
DROP INDEX ingot.upload_intents_stalled_idx;
DROP INDEX ingot.upload_intents_evictable_idx;
ALTER TABLE ingot.upload_intents DROP COLUMN evicted_at;
