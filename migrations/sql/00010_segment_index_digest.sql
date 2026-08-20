-- +goose Up
-- A shipped segment registers TWO blobs in the bucket's space: the CAR and
-- its sharded-dag-index. index_digest records the index blob's multihash at
-- ship time so DeleteBucket can release both registrations (the tenant
-- service refuses to delete a space that still holds any). NULL means the
-- segment registered nothing (unshipped, header-only, or a non-publishing
-- uploader).
ALTER TABLE ingot.segments ADD COLUMN index_digest bytea;

-- +goose Down
ALTER TABLE ingot.segments DROP COLUMN index_digest;
