-- +goose Up
-- Complete records its committed result on the session. A duplicate or
-- latch-losing CompleteMultipartUpload replays the ETag (and, for versioned
-- buckets, the version id) the winner returned, instead of resolving the live
-- key, which a later overwrite may have replaced. Both columns are NULL until
-- the session reaches 'completed'; the completing→completed latch writes them
-- in the same statement.
ALTER TABLE ingot.multipart_sessions
    ADD COLUMN committed_etag       text,
    ADD COLUMN committed_version_id text;

-- +goose Down
ALTER TABLE ingot.multipart_sessions
    DROP COLUMN committed_etag,
    DROP COLUMN committed_version_id;
