-- +goose Up
-- A part's ETag derives from the sha256 of its bytes (s3frontend/etag.go),
-- so the column that holds the digest is named for what it is. Rows written
-- before this migration hold an MD5 and no longer match the ETag the client
-- was given for that part, so a multipart upload left open across the
-- migration cannot complete (InvalidPart) and must be aborted and re-driven.
ALTER TABLE ingot.multipart_parts
    RENAME COLUMN etag_md5 TO etag_digest;

-- +goose Down
ALTER TABLE ingot.multipart_parts
    RENAME COLUMN etag_digest TO etag_md5;
