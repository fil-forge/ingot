-- +goose Up
-- Deferred blob release. When a blob's last reference claim drops (overwrite,
-- delete, or multipart cleanup of an accepted-but-unclaimed part), the release
-- is not executed inline: a row lands here — in the same transaction as the
-- claim delete, so there is no crash window between "last claim gone" and
-- "release recorded" — and a background sweeper executes it after not_before.
-- The grace window lets in-flight readers holding the prior catalog root
-- finish their encryption-params/location prefetch before the rows they need
-- are shredded; the sweeper also re-checks the claim count at drain time, so
-- a stale intent (a digest re-claimed since enqueue) self-heals into a no-op.
-- Executing a release means: delete the blob_encryption_params row (the
-- crypto-shred), delete the blob_locations row, invoke the network remove.
-- The row is deleted only when all three succeed; failures retry next sweep —
-- the durability the old best-effort inline release lacked.
CREATE TABLE ingot.blob_release_intents (
    space      text        NOT NULL,
    digest     bytea       NOT NULL,                 -- ciphertext blob multihash, as in blob_refs
    not_before timestamptz NOT NULL,                 -- earliest execution time (enqueue + release grace)
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (space, digest)
);
-- The sweeper's "list due" scan.
CREATE INDEX blob_release_intents_due_idx ON ingot.blob_release_intents (not_before);

-- +goose Down
DROP TABLE ingot.blob_release_intents;
