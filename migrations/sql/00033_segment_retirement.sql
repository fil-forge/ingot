-- +goose Up
-- Retention used to delete a segment's row once it unlinked the files, but a
-- shipped segment's CAR and index blobs stay registered in the bucket's space
-- until DeleteBucket releases them, and the row is the only record of their
-- digests. retired_at marks a row whose files are gone; NULL means the
-- segment is still on disk.
ALTER TABLE ingot.segments ADD COLUMN retired_at BIGINT;

-- Retired rows accumulate until DeleteBucket, while opening a bucket's log and
-- the startup sweep read only the few unretired ones.
CREATE INDEX segments_unretired_idx ON ingot.segments (plane, bucket, seq)
    WHERE retired_at IS NULL;

-- +goose Down
DROP INDEX ingot.segments_unretired_idx;
ALTER TABLE ingot.segments DROP COLUMN retired_at;
