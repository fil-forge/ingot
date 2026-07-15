-- +goose Up
-- Multipart listing + completion idempotency (FIL-520):
--   * multipart_parts.created_at backs ListParts' per-part LastModified;
--     sessions already carry created_at for ListMultipartUploads' Initiated.
--   * Sessions gain the standard HTTP metadata headers CreateMultipartUpload
--     may carry, so Complete writes them into the manifest exactly like a
--     single-shot PUT does.
--   * A 'completed' session state: Complete now retains the session (and its
--     parts) instead of deleting it, so a duplicate CompleteMultipartUpload
--     with identical parts is idempotent per S3. The abandoned-session
--     sweeper reaps completed rows after the TTL.

ALTER TABLE ingot.multipart_parts
    ADD COLUMN created_at timestamptz NOT NULL DEFAULT now();

ALTER TABLE ingot.multipart_sessions
    ADD COLUMN content_encoding          text,
    ADD COLUMN content_disposition       text,
    ADD COLUMN content_language          text,
    ADD COLUMN cache_control             text,
    ADD COLUMN expires                   text,
    ADD COLUMN website_redirect_location text,
    ADD COLUMN checksum_algorithm        text,
    ADD COLUMN checksum_type             text;

ALTER TABLE ingot.multipart_sessions
    DROP CONSTRAINT multipart_sessions_state_check,
    ADD CONSTRAINT multipart_sessions_state_check
        CHECK (state IN ('open','completing','aborting','completed'));

-- +goose Down
ALTER TABLE ingot.multipart_sessions
    DROP CONSTRAINT multipart_sessions_state_check,
    ADD CONSTRAINT multipart_sessions_state_check
        CHECK (state IN ('open','completing','aborting'));

ALTER TABLE ingot.multipart_sessions
    DROP COLUMN content_encoding,
    DROP COLUMN content_disposition,
    DROP COLUMN content_language,
    DROP COLUMN cache_control,
    DROP COLUMN expires,
    DROP COLUMN website_redirect_location,
    DROP COLUMN checksum_algorithm,
    DROP COLUMN checksum_type;

ALTER TABLE ingot.multipart_parts
    DROP COLUMN created_at;
