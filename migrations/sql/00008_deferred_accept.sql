-- +goose Up
-- Deferred-accept multipart (§7.2): a part's blobs upload to the provider at
-- UploadPart but the /http/put conclude — which triggers /blob/accept — is
-- deferred to Complete. blob_parks holds the state needed to conclude (or
-- unallocate) later. put_invocation is the sealed /http/put invocation whose
-- metadata embeds the derived signer keys; rows are deleted promptly at
-- conclude/unallocate. Keyed globally by digest, like upload_intents:
-- content-addressed dedup shares a park across sessions and parts.
CREATE TABLE ingot.blob_parks (
    digest         bytea PRIMARY KEY,
    add_task       bytea  NOT NULL,   -- /space/blob/add task CID (unallocate cause)
    accept_task    bytea  NOT NULL,   -- /blob/accept task CID (conclude poll target)
    put_invocation bytea  NOT NULL,   -- sealed /http/put invocation
    size           bigint NOT NULL,
    created_at     timestamptz NOT NULL DEFAULT now()
);

-- +goose Down
DROP TABLE ingot.blob_parks;
