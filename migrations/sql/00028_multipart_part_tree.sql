-- +goose Up
-- Per-part BLAKE3 tree material, recorded by UploadPart at the offset the
-- part was assumed to sit at and merged by Complete into the object's
-- digest and block list (see blake3tree). It is in its own table because it
-- is large (up to about 320 KiB for a 5 GiB part, nearly all of it blocks)
-- and short-lived: Complete deletes an upload's rows once the object's
-- manifest holds the object's tree, while the part rows are retained for the
-- idempotent re-Complete. Every other part listing never reads it.
-- nodes holds the part's aligned subtrees, blocks its blocks at chunk_log
-- (the block size as a base-2 exponent of BLAKE3 chunks; both as
-- blake3tree.EncodeSubtrees), head and tail the part's bytes before its first
-- chunk boundary and after its last (under 1 KiB each; Complete hashes the
-- chunk straddling a part boundary from them), and root the part's own
-- BLAKE3 hash when it was hashed at offset 0. A part without a row (recorded
-- before the tree existed, or a copy that read no bytes) is re-hashed by
-- Complete.
CREATE TABLE ingot.multipart_part_trees (
    upload_id    text     NOT NULL,
    part_number  int      NOT NULL,
    tree_offset  bigint   NOT NULL,
    chunk_log    smallint NOT NULL,
    nodes        bytea,
    blocks       bytea,
    head         bytea,
    tail         bytea,
    root         bytea,
    PRIMARY KEY (upload_id, part_number),
    FOREIGN KEY (upload_id, part_number)
        REFERENCES ingot.multipart_parts(upload_id, part_number) ON DELETE CASCADE
);

-- +goose Down
DROP TABLE ingot.multipart_part_trees;
