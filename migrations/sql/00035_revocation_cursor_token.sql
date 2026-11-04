-- +goose Up
-- The revocation firehose's resume token: the cursor Swarf sends as each
-- event's id, which the consumer resumes after with
-- GET /revocations/after/{token}. NULL = a row saved before tokens, holding
-- only recorded_at; the consumer resumes it once with the time query from
-- recorded_at and saves tokens from then on. recorded_at and revoke stay,
-- describing the last processed record, for observability.
ALTER TABLE ingot.revocation_cursor
    ADD COLUMN token text;

-- +goose Down
ALTER TABLE ingot.revocation_cursor
    DROP COLUMN token;
