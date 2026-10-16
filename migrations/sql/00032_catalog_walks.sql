-- +goose Up
-- catalog_walks holds each bucket's last completed walk of its live catalog:
-- the root walked, when the walk started and finished, the bytes it reached,
-- and the bucket's shipped, dead-on-arrival and unmeasured catalog bytes as
-- of its start, so the dead bytes they imply are all of one moment.
CREATE TABLE ingot.catalog_walks (
    bucket                text   PRIMARY KEY REFERENCES ingot.buckets(name) ON DELETE CASCADE,
    root_cid              bytea,
    started_at            bigint NOT NULL,
    finished_at           bigint NOT NULL,
    live_bytes            bigint NOT NULL,
    forge_bytes           bigint NOT NULL,
    dead_on_arrival_bytes bigint NOT NULL,
    unmeasured_bytes      bigint NOT NULL
);

-- catalog_walk_subtrees remembers the bytes and blocks reachable from each
-- catalog block whose subtree is large enough, so a later walk skips every
-- subtree that hasn't changed: a CID names the same bytes forever. walked_at
-- is the start of the last walk that computed or used the row; a completed
-- walk drops the bucket's rows it didn't use.
CREATE TABLE ingot.catalog_walk_subtrees (
    bucket    text   NOT NULL REFERENCES ingot.buckets(name) ON DELETE CASCADE,
    cid       bytea  NOT NULL,
    bytes     bigint NOT NULL,
    blocks    bigint NOT NULL,
    walked_at bigint NOT NULL,
    PRIMARY KEY (bucket, cid)
);

-- +goose Down
DROP TABLE ingot.catalog_walk_subtrees;
DROP TABLE ingot.catalog_walks;
