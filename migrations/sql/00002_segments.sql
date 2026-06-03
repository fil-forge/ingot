-- +goose Up
-- ingot log segments (LSM-style write log) and the per-segment record of
-- bucket-root advances that landed in each segment.
--
-- Each segment belongs to exactly ONE plane — a data plane (raw
-- object-body chunks) or a catalog plane (dag-cbor MST nodes, manifests,
-- indexes). The two planes are independent pipelines: they seal, ship,
-- and retire on their own. seq is drawn from one shared sequence, so it
-- is globally unique across planes; the `plane` column discriminates.
--
-- segments
--   seq        — globally-unique segment id (filename stem `seg-<seq>`)
--   plane      — 'data' or 'catalog'
--   state      — 'open' or 'sealed'
--   sealed_at  — unix seconds when seal completed; NULL while open
--   size_bytes — CAR size at seal
--   sha256     — sha256 of the CAR at seal
--   shipped_at — unix seconds the CAR shipped to Forge; NULL otherwise
--
-- segment_op_roots (catalog-plane segments only)
--   seq, seq_within — composite ordering of S3 ops within a segment
--   bucket          — the bucket whose root advanced for this op
--   root_cid        — the new MST root the op produced
--
-- The on-disk `<plane>/seg-<seq>.idx` sidecars are the source of truth at
-- recovery; these tables are rehydrated from them when rows are missing.
-- Shipping the CATALOG plane advances per-bucket forge_root_cid in
-- `ingot.buckets` (catalog roots are the MST roots durable on Forge);
-- shipping the data plane does not.

CREATE SEQUENCE ingot.segment_seq;

CREATE TABLE ingot.segments (
    seq        BIGINT PRIMARY KEY,
    plane      TEXT   NOT NULL CHECK (plane IN ('data', 'catalog')),
    state      TEXT   NOT NULL CHECK (state IN ('open', 'sealed')),
    sealed_at  BIGINT,
    size_bytes BIGINT NOT NULL DEFAULT 0,
    sha256     BYTEA,
    shipped_at BIGINT
);
CREATE INDEX segments_plane_seq_idx ON ingot.segments (plane, seq);

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
