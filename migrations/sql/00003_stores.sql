-- +goose Up
-- The relational surface the upload/storage/delete architecture relies on
-- (docs/architecture.md §5–§7, Appendix C). Object manifests and MST nodes
-- are NOT rows here — they are content-addressed dag-cbor blocks in the
-- catalog plane. These tables index the local blob store, the reverse
-- reference index that gates deletion, blob locations (the appliance-topology
-- local location table, in place of the indexing-service for body reads),
-- and in-flight multipart uploads.
--
-- version_id columns carry each version's id — a ULID token, or 'null' for
-- null versions (docs/s3-versioning.md §8); buckets.versioning and
-- next_version_seq drive the S3 versioning state (§4.1). CIDs and blob
-- digests (sha256 multihashes) are stored as bytea.

-- buckets gains: the Forge space it lives in, the S3 versioning state, and
-- the per-bucket version ordinal. Existing columns are untouched. space
-- defaults to '' so existing inserts keep working until the write path
-- threads the space DID through bucket creation.
ALTER TABLE ingot.buckets
    ADD COLUMN space            text   NOT NULL DEFAULT '',
    ADD COLUMN versioning       text   NOT NULL DEFAULT 'unversioned'
                                    CHECK (versioning IN ('unversioned','enabled','suspended')),
    ADD COLUMN next_version_seq bigint NOT NULL DEFAULT 0;
CREATE INDEX buckets_space_idx ON ingot.buckets (space);

-- Reverse index (§5, §6): which object versions reference each blob. One row
-- per (digest, version). A blob's space-claim is released when no rows remain
-- for (space, digest); Piri then deletes the bytes once no space claims it.
-- space is denormalized from buckets for a direct claim query.
CREATE TABLE ingot.blob_refs (
    digest      bytea NOT NULL,
    bucket      text  NOT NULL,
    object_key  text  NOT NULL,
    version_id  text  NOT NULL,
    space       text  NOT NULL,
    PRIMARY KEY (digest, bucket, object_key, version_id)
);
-- Drives "is (space, digest) still claimed?" — the gate on remove(digest).
CREATE INDEX blob_refs_claim_idx ON ingot.blob_refs (space, digest);

-- The local-store index (§5): every blob Ingot holds on disk, in-flight or
-- retained as cache. Drives read-after-write, cache lookup, and crash
-- recovery. state advances spooled → parked → accepted → published; cache
-- eviction deletes the row and the file. Keyed by digest (global; the spool
-- is shared across whatever objects reference the same bytes).
CREATE TABLE ingot.upload_intents (
    digest      bytea PRIMARY KEY,
    local_path  text   NOT NULL,
    size        bigint NOT NULL,
    state       text   NOT NULL
                    CHECK (state IN ('spooled','parked','accepted','published')),
    bucket      text,
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now()
);

-- Local blob location table (§8, appliance topology): the (space, digest) →
-- provider/URL mapping captured at accept, in place of the indexing-service
-- for body-blob reads. A Locator impl resolves a read against this table.
CREATE TABLE ingot.blob_locations (
    space      text   NOT NULL,
    digest     bytea  NOT NULL,
    provider   text   NOT NULL,                 -- provider/node DID that issued the commitment
    url        text   NOT NULL,                 -- retrieval URL for the blob
    size       bigint NOT NULL,                 -- whole-blob byte length (range = [0,size))
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (space, digest)
);

-- One row per in-flight multipart upload (§7.2). state is the single-winner
-- latch (§7.3): Complete and Abort race to move it off 'open'. content_type
-- and metadata are carried from CreateMultipartUpload so Complete can write the
-- manifest without the client resupplying them.
CREATE TABLE ingot.multipart_sessions (
    upload_id     text PRIMARY KEY,
    bucket        text NOT NULL,
    object_key    text NOT NULL,
    state         text NOT NULL DEFAULT 'open'
                      CHECK (state IN ('open','completing','aborting')),
    content_type  text,
    metadata      jsonb,
    created_at    timestamptz NOT NULL DEFAULT now()
);

-- One row per uploaded part. A part larger than max_blob_size maps to several
-- blobs, so blob_digests is an ordered array. state is 'parked' until Complete
-- accepts the part's blobs (or 'accepted' immediately on a dedup hit, §5).
CREATE TABLE ingot.multipart_parts (
    upload_id     text    NOT NULL REFERENCES ingot.multipart_sessions(upload_id) ON DELETE CASCADE,
    part_number   int     NOT NULL,
    etag_md5      bytea   NOT NULL,
    size          bigint  NOT NULL,
    blob_digests  bytea[] NOT NULL,
    state         text    NOT NULL DEFAULT 'parked'
                      CHECK (state IN ('parked','accepted')),
    PRIMARY KEY (upload_id, part_number)
);

-- Superseded MST node CIDs, recorded on overwrite/delete (§4, §9). Write-only
-- this iteration — no catalog GC yet; a future collector consumes it.
CREATE TABLE ingot.gc_candidates (
    cid         bytea PRIMARY KEY,
    bucket      text,
    created_at  timestamptz NOT NULL DEFAULT now()
);

-- +goose Down
DROP TABLE ingot.gc_candidates;
DROP TABLE ingot.multipart_parts;
DROP TABLE ingot.multipart_sessions;
DROP TABLE ingot.blob_locations;
DROP TABLE ingot.upload_intents;
DROP TABLE ingot.blob_refs;
DROP INDEX ingot.buckets_space_idx;
ALTER TABLE ingot.buckets
    DROP COLUMN next_version_seq,
    DROP COLUMN versioning,
    DROP COLUMN space;
