-- +goose Up
-- The catalog journal: every catalog block (MST node or manifest) a commit
-- appended to the on-disk log, kept until the segment it went into ships to
-- Forge. A commit writes its rows in the same transaction as its root swap,
-- so a copy of Postgres that holds a root also holds every block of it that
-- exists nowhere but this host's disk. Shipping a segment deletes its rows
-- in the transaction that stamps it shipped.
--
--   seq        — the log segment the block was appended to
--   bucket     — the bucket whose commit wrote it
--   cid        — the block's CID
--   data       — the block's bytes
--   created_at — when the commit wrote the row (the journal's age gauge)
--
-- seq deliberately has no foreign key to ingot.segments. Logstore recovery
-- deletes the row of any segment whose CAR is missing from disk, and after a
-- lost disk these rows are what the segment is rebuilt from, so they must
-- outlive it. The rows go with their bucket instead.
--
-- A block can be appended to one segment more than once (two commits that
-- both write a node already in the segment); the key keeps one row.
CREATE TABLE ingot.catalog_journal (
    seq        BIGINT      NOT NULL,
    bucket     TEXT        NOT NULL REFERENCES ingot.buckets(name) ON DELETE CASCADE,
    cid        BYTEA       NOT NULL,
    data       BYTEA       NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (seq, cid)
);
CREATE INDEX catalog_journal_bucket_seq_idx ON ingot.catalog_journal (bucket, seq);

-- +goose Down
DROP TABLE ingot.catalog_journal;
