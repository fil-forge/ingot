-- +goose Up
-- ingot log segments (LSM-style write log) and the per-segment record
-- of bucket-root advances that landed in each segment.
--
-- Each segment splits into TWO CAR files on disk — a data plane (raw
-- object-body chunks) and a catalog plane (dag-cbor MST nodes,
-- manifests, indexes) — that ship to Forge through independent
-- pipelines and may retire independently. The row tracks per-plane
-- size/sha and a per-plane ship high-water timestamp.
--
-- segments
--   seq             — monotonic segment id (filename stem `seg-<seq>`)
--   state           — 'open' or 'sealed' (shipping is per-plane, below)
--   sealed_at       — unix seconds when seal completed; NULL while open
--   data_size_bytes — data CAR size at seal
--   data_sha256     — sha256 of the data CAR at seal
--   cat_size_bytes  — catalog CAR size at seal
--   cat_sha256      — sha256 of the catalog CAR at seal
--   data_shipped_at — unix seconds the data CAR shipped; NULL otherwise
--   cat_shipped_at  — unix seconds the catalog CAR shipped; NULL otherwise
--
-- segment_op_roots
--   seq, seq_within — composite ordering of S3 ops within a segment
--   bucket          — the bucket whose root advanced for this op
--   root_cid        — the new MST root the op produced
--
-- The on-disk `seg-<seq>.{data,cat}.idx` sidecars are the source of
-- truth at recovery; these tables are rehydrated from them when rows
-- are missing. Shipping the CATALOG plane advances per-bucket
-- forge_root_cid in `ingot.buckets` (catalog roots are the MST roots
-- durable on Forge); shipping the data plane does not.

CREATE SEQUENCE ingot.segment_seq;

CREATE TABLE ingot.segments (
    seq             BIGINT PRIMARY KEY,
    state           TEXT   NOT NULL CHECK (state IN ('open', 'sealed')),
    sealed_at       BIGINT,
    data_size_bytes BIGINT NOT NULL DEFAULT 0,
    data_sha256     BYTEA,
    cat_size_bytes  BIGINT NOT NULL DEFAULT 0,
    cat_sha256      BYTEA,
    data_shipped_at BIGINT,
    cat_shipped_at  BIGINT
);

CREATE TABLE ingot.segment_op_roots (
    seq         BIGINT NOT NULL REFERENCES ingot.segments(seq) ON DELETE CASCADE,
    seq_within  INT    NOT NULL,
    bucket      TEXT   NOT NULL,
    root_cid    BYTEA  NOT NULL,
    PRIMARY KEY (seq, seq_within)
);
CREATE INDEX segment_op_roots_bucket_seq_idx ON ingot.segment_op_roots (bucket, seq);

-- +goose Down
DROP TABLE ingot.segment_op_roots;
DROP TABLE ingot.segments;
DROP SEQUENCE ingot.segment_seq;
