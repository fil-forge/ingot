-- +goose Up
-- The startup released pass asks whether a digest has a pending
-- release in any space; the primary key leads with space.
CREATE INDEX blob_release_intents_digest_idx ON ingot.blob_release_intents (digest);

-- +goose Down
DROP INDEX ingot.blob_release_intents_digest_idx;
