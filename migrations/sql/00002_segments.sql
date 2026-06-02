-- +goose Up
-- ingot log segments (LSM-style write log) and the per-segment
-- record of bucket-root advances that landed in each segment.
--
-- segments
--   seq         — monotonic segment id (matches the on-disk filename
--                 stem `seg-<seq>.car`)
--   state       — one of 'open', 'sealed', 'flushed'
--   sealed_at   — unix seconds when seal was completed; NULL while open
--   flushed_at  — unix seconds when the Forge ship completed; NULL otherwise
--   size_bytes  — final size of the CAR file at seal
--   car_sha256  — sha256 of the CAR file at seal (used to detect torn
--                 sidecars during recovery)
--
-- segment_op_roots
--   seq, seq_within — composite ordering of S3 ops within a segment
--   bucket          — the bucket whose root advanced for this op
--   root_cid        — the new MST root the op produced
--
-- The on-disk `seg-<seq>.idx` sidecar is the source of truth at
-- recovery time; these tables are rehydrated from sidecars when rows
-- are missing. The flusher uses `segment_op_roots` (joined with
-- `segments.state = 'flushed'`) to advance per-bucket forge_root_cid
-- in `ingot.buckets` atomically with the state transition.

CREATE SEQUENCE ingot.segment_seq;

CREATE TABLE ingot.segments (
    seq         BIGINT PRIMARY KEY,
    state       TEXT   NOT NULL CHECK (state IN ('open', 'sealed', 'flushed')),
    sealed_at   BIGINT,
    flushed_at  BIGINT,
    size_bytes  BIGINT NOT NULL DEFAULT 0,
    car_sha256  BYTEA
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
