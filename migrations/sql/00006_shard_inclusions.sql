-- +goose Up
-- Shard inclusions: the local half of the indexing-service contract the
-- appliance topology mimics (docs/architecture.md §8). blob_locations answers
-- "where is blob X stored whole"; this table answers "which stored blob
-- contains block B, and at what byte range". Together they let the read tier
-- resolve a catalog block (manifest / MST node) whose local segment CAR has
-- been retired by retention: block digest → enclosing shard CAR → ranged
-- /content/retrieve against the shard's blob_locations row.
--
-- One row per block of every shipped catalog segment CAR, written in the
-- flush path before the segment is marked shipped — so retention can never
-- retire a segment whose blocks are unresolvable. Range bounds are inclusive
-- (matching libforge blobindex.Range).
CREATE TABLE ingot.shard_inclusions (
    space        text   NOT NULL,
    digest       bytea  NOT NULL, -- inner block multihash
    shard_digest bytea  NOT NULL, -- enclosing shard CAR multihash
    range_start  bigint NOT NULL,
    range_end    bigint NOT NULL,
    PRIMARY KEY (space, digest)
);

-- +goose Down
DROP TABLE ingot.shard_inclusions;
