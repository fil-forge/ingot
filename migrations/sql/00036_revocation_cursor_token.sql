-- +goose Up
-- The revocation firehose's resume token: the cursor Swarf sends as each
-- event's id, which the consumer resumes after with
-- GET /revocations/after/{token}. Swarf no longer serves the time query a row
-- from before tokens resumed with, so those rows go, and the consumer starts
-- at the firehose's head. recorded_at, that query's resume point, goes too,
-- and so does revoke: nothing read it, and the consumer logs each record it
-- processes. updated_at still says when the cursor last moved.
DELETE FROM ingot.revocation_cursor;
ALTER TABLE ingot.revocation_cursor
    ADD COLUMN token text NOT NULL,
    DROP COLUMN recorded_at,
    DROP COLUMN revoke;

-- +goose Down
-- The old consumer has no use for a token, and the dropped columns have
-- nothing to restore: start it over at its own "now".
DELETE FROM ingot.revocation_cursor;
ALTER TABLE ingot.revocation_cursor
    DROP COLUMN token,
    ADD COLUMN recorded_at timestamptz NOT NULL,
    ADD COLUMN revoke bytea NOT NULL;
