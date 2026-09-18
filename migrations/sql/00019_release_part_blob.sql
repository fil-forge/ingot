-- +goose Up
-- A release record for a multipart part blob that was never committed. Such a
-- blob's spool copy and upload intent go with the release: they are ingest
-- artifacts nothing will read again. A committed blob's release leaves them,
-- because its spool copy is the insurance copy until eviction. Recorded on the
-- row so the release sweeper unwinds a part blob exactly as the inline pass
-- after a session teardown does.
ALTER TABLE ingot.blob_release_intents
    ADD COLUMN part_blob boolean NOT NULL DEFAULT false;

-- +goose Down
ALTER TABLE ingot.blob_release_intents DROP COLUMN part_blob;
