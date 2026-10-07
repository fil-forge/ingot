-- +goose Up
-- ingot log segments (LSM-style write log) and the per-segment record of
-- bucket-root advances that landed in each segment.
--
-- Segments hold the catalog log: dag-cbor MST nodes, manifests and
-- indexes. Object bodies are not in the log; they live in local blob
-- storage and on Forge. seq is drawn from one sequence, so it is globally
-- unique. The `plane` column predates the data plane's removal and is
-- always 'catalog'.
--
-- segments
--   seq        — globally-unique segment id (filename stem `seg-<seq>`)
--   plane      — always 'catalog'
--   state      — 'open' or 'sealed'
--   sealed_at  — unix seconds when seal completed; NULL while open
--   size_bytes — CAR size at seal
--   sha256     — sha256 of the CAR at seal
--   shipped_at — unix seconds the CAR shipped to Forge; NULL otherwise
--
-- segment_op_roots
--   seq, seq_within — composite ordering of S3 ops within a segment
--   bucket          — the bucket whose root advanced for this op
--   root_cid        — the new MST root the op produced
--
-- The on-disk `<plane>/seg-<seq>.idx` sidecars are the source of truth at
-- recovery; these tables are rehydrated from them when rows are missing.
-- Shipping a segment advances per-bucket forge_root_cid in `ingot.buckets`
-- (catalog roots are the MST roots durable on Forge).

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
