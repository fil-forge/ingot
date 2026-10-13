-- +goose Up
-- A release sets started_at, under the blob's claim-key lock, once it has
-- found no claim and no live part reference and is about to remove the blob
-- from its provider. A write that finds the blob already stored for its
-- space and means to reuse it cancels a release not yet started, under the
-- same lock, or waits for a started one to finish and uploads afresh: the
-- two sides agree on who owns the digest, which a blob stored as received
-- can share with a write that follows its deletion.
ALTER TABLE ingot.blob_release_intents ADD COLUMN started_at timestamptz;

-- +goose Down
ALTER TABLE ingot.blob_release_intents DROP COLUMN started_at;
