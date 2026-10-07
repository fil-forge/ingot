-- +goose Up
-- Per-part BLAKE3 tree material, recorded by UploadPart at the offset the
-- part was assumed to sit at and merged by Complete into the object's
-- digest and block list (see blake3tree). tree_nodes holds the part's
-- aligned subtrees, tree_blocks its blocks at tree_chunk_log (the block
-- size as a base-2 exponent of BLAKE3 chunks; both as
-- blake3tree.EncodeSubtrees), tree_head and tree_tail the part's bytes
-- before its first chunk boundary and after its last (under 1 KiB each;
-- Complete hashes the chunk straddling a part boundary from them), and
-- tree_root the part's own BLAKE3 hash when it was hashed at offset 0. A
-- part recorded before the tree existed has tree_chunk_log 0, and Complete
-- re-hashes it.
ALTER TABLE ingot.multipart_parts
    ADD COLUMN tree_offset bigint   NOT NULL DEFAULT 0,
    ADD COLUMN tree_chunk_log smallint NOT NULL DEFAULT 0,
    ADD COLUMN tree_nodes  bytea,
    ADD COLUMN tree_blocks bytea,
    ADD COLUMN tree_head   bytea,
    ADD COLUMN tree_tail   bytea,
    ADD COLUMN tree_root   bytea;

-- +goose Down
ALTER TABLE ingot.multipart_parts
    DROP COLUMN tree_offset,
    DROP COLUMN tree_chunk_log,
    DROP COLUMN tree_nodes,
    DROP COLUMN tree_blocks,
    DROP COLUMN tree_head,
    DROP COLUMN tree_tail,
    DROP COLUMN tree_root;
