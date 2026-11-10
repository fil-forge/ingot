-- +goose Up
-- The revocation firehose's resume token: the cursor Swarf sends as each
-- event's id, which the consumer resumes after with
-- GET /revocations/after/{token}. Swarf no longer serves the time query a row
-- from before tokens resumed with, so those rows go, and the consumer starts
-- at the firehose's head. A head the consumer saves before its first record
-- has no record to describe, so recorded_at and revoke, which describe the
-- last processed record for observability, are NULL until one arrives.
DELETE FROM ingot.revocation_cursor;
ALTER TABLE ingot.revocation_cursor
    ADD COLUMN token text NOT NULL,
    ALTER COLUMN recorded_at DROP NOT NULL,
    ALTER COLUMN revoke DROP NOT NULL;

-- +goose Down
-- A row without a record cannot satisfy the old NOT NULLs, and the old
-- consumer has no use for a token: start it over at its own "now".
DELETE FROM ingot.revocation_cursor;
ALTER TABLE ingot.revocation_cursor
    DROP COLUMN token,
    ALTER COLUMN recorded_at SET NOT NULL,
    ALTER COLUMN revoke SET NOT NULL;
