-- +goose Up
-- Object tagging (docs/s3-object-tagging.md §4). Tag sets live in the
-- catalog (the per-key version-state tree); the registry carries only
-- CreateMultipartUpload's raw x-amz-tagging header on its way to Complete,
-- which stamps the parsed set onto the version it commits.
ALTER TABLE ingot.multipart_sessions
    ADD COLUMN tagging text;

-- +goose Down
ALTER TABLE ingot.multipart_sessions
    DROP COLUMN tagging;
