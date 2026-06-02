-- +goose Up
-- ingot bucket registry. Mirrors the columns of the previous SQLite
-- schema (pkg/ingot/registry/sqlite.go's `buckets` table) but in the
-- `ingot` schema so the same Postgres database can host both sprue's
-- and ingot's tables without collision.
--
-- name           — S3 bucket name (PK)
-- root_cid       — current MST root CID, bytes form; NULL for empty bucket
-- forge_root_cid — last MST root whose DAG has been shipped to Forge
-- created_at     — unix seconds at create time

CREATE TABLE ingot.buckets (
    name           TEXT   PRIMARY KEY,
    root_cid       BYTEA,
    forge_root_cid BYTEA,
    created_at     BIGINT NOT NULL
);

-- +goose Down
DROP TABLE ingot.buckets;
