-- +goose Up
-- Per-part BLAKE3 tree material, recorded by UploadPart at the offset the
-- part was assumed to sit at and merged by Complete into the object's
-- digest and leaf list (see blake3tree). tree_nodes holds the part's
-- aligned subtrees, tree_leaves its group-aligned leaves (both as
-- blake3tree.EncodeSubtrees), tree_root the part's own BLAKE3 hash when it
-- was hashed at offset 0. A part recorded before the tree existed has NULL
-- nodes, and Complete re-hashes it.
ALTER TABLE ingot.multipart_parts
    ADD COLUMN tree_offset bigint   NOT NULL DEFAULT 0,
    ADD COLUMN tree_group  smallint NOT NULL DEFAULT 0,
    ADD COLUMN tree_nodes  bytea,
    ADD COLUMN tree_leaves bytea,
    ADD COLUMN tree_root   bytea;

-- +goose Down
ALTER TABLE ingot.multipart_parts
    DROP COLUMN tree_offset,
    DROP COLUMN tree_group,
    DROP COLUMN tree_nodes,
    DROP COLUMN tree_leaves,
    DROP COLUMN tree_root;
