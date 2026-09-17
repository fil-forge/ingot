-- +goose Up
-- Bucket tagging (docs/s3-object-tagging.md §9). The tag set is bucket-level
-- metadata, so it rides the registry beside the object-lock configuration
-- rather than the catalog.
--
--   bucket_tagging — the controller's validated tag set as a JSON object
--       ({"key":"value"}). NULL = no tag set (GetBucketTagging answers
--       NoSuchTagSet); DeleteBucketTagging restores NULL.
ALTER TABLE ingot.buckets
    ADD COLUMN bucket_tagging bytea;

-- +goose Down
ALTER TABLE ingot.buckets
    DROP COLUMN bucket_tagging;
