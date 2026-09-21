-- +goose Up
-- The session sweeper measures staleness from the last state change rather
-- than from creation. A Complete latches an open session to 'completing' as it
-- starts, so a live Complete on an old session holds the row for a TTL of its
-- own instead of racing a sweep that would abort the parts it is concluding.
-- Existing rows count from their creation, as before.
ALTER TABLE ingot.multipart_sessions
    ADD COLUMN state_changed_at timestamptz NOT NULL DEFAULT now();
UPDATE ingot.multipart_sessions SET state_changed_at = created_at;

-- +goose Down
ALTER TABLE ingot.multipart_sessions DROP COLUMN state_changed_at;
