-- +goose Up
-- A multipart session records its bucket's space, so tearing the session down
-- never depends on the bucket row: a session created while its bucket was
-- being deleted can outlive the bucket, and its part rows are the only index
-- to blobs the space still holds. Rows that predate the column are backfilled
-- from their bucket where it still exists; the empty string means unknown, and
-- the code then resolves the bucket as before.
ALTER TABLE ingot.multipart_sessions
    ADD COLUMN space text NOT NULL DEFAULT '';
UPDATE ingot.multipart_sessions s
    SET space = b.space
    FROM ingot.buckets b
    WHERE s.bucket = b.name;

-- +goose Down
ALTER TABLE ingot.multipart_sessions DROP COLUMN space;
