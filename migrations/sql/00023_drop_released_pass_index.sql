-- +goose Up
-- The startup released pass, which 00022's index served, has done its
-- one-time cleanup and is gone; nothing else looks up a pending release by
-- digest alone.
DROP INDEX IF EXISTS ingot.blob_release_intents_digest_idx;

-- +goose Down
CREATE INDEX blob_release_intents_digest_idx ON ingot.blob_release_intents (digest);
