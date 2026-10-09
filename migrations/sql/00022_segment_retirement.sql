-- +goose Up
-- Retention used to delete a segment's row once it unlinked the files, but a
-- shipped segment's CAR and index blobs stay registered in the bucket's space
-- until DeleteBucket releases them, and the row is the only record of their
-- digests. retired_at marks a row whose files are gone; NULL means the
-- segment is still on disk.
ALTER TABLE ingot.segments ADD COLUMN retired_at BIGINT;

-- +goose Down
ALTER TABLE ingot.segments DROP COLUMN retired_at;
