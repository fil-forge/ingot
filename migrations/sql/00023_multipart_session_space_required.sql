-- +goose Up
-- A session's space is the subject of every release of its blobs. Rows the
-- 00019 backfill could not resolve (their bucket row was already gone) carry
-- the empty string: they have no space to release against, and resolving the
-- bucket name instead is unsafe, since a name recreated later names another
-- space and another tenant. Such a row is a dead index and goes; the column
-- is then required, so no session can be created or read without a space.
DELETE FROM ingot.multipart_sessions WHERE space = '';
ALTER TABLE ingot.multipart_sessions
    ADD CONSTRAINT multipart_sessions_space_check CHECK (space <> '');

-- +goose Down
ALTER TABLE ingot.multipart_sessions DROP CONSTRAINT multipart_sessions_space_check;
