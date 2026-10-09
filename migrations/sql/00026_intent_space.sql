-- +goose Up
-- A blob stored as received (a bucket with encryption "none") has the digest
-- of its bytes, so the same bytes written to two spaces share a digest: one
-- spool file, but an upload, a park and a release per space. The upload
-- intent and the park therefore carry the space, as the location, claim and
-- release rows do, and are keyed by (space, digest).

ALTER TABLE ingot.upload_intents ADD COLUMN space text;
-- An intent's space is its bucket's. A row whose bucket is gone takes the
-- space of whatever else names the digest, which is the one space that can
-- have written it while every write encrypted under a key of its own.
UPDATE ingot.upload_intents i SET space = COALESCE(
    (SELECT b.space FROM ingot.buckets b WHERE b.name = i.bucket),
    (SELECT l.space FROM ingot.blob_locations l WHERE l.digest = i.digest LIMIT 1),
    (SELECT r.space FROM ingot.blob_refs r WHERE r.digest = i.digest LIMIT 1),
    (SELECT p.space FROM ingot.blob_release_intents p WHERE p.digest = i.digest LIMIT 1));
-- An intent nothing can place belongs to no space: no bucket, location,
-- claim or release names its blob, so no release or eviction would ever
-- reach it. Its spool file, if any, is left to the orphan pass.
DELETE FROM ingot.upload_intents WHERE space IS NULL;
ALTER TABLE ingot.upload_intents ALTER COLUMN space SET NOT NULL;
ALTER TABLE ingot.upload_intents
    DROP CONSTRAINT upload_intents_pkey,
    ADD PRIMARY KEY (space, digest);
-- MarkEvicted, MissingIntents and CountOtherIntents ask about a digest in
-- every space; the primary key leads with space.
CREATE INDEX upload_intents_digest_idx ON ingot.upload_intents (digest);
-- The eviction scan's order and cursor gain the space.
DROP INDEX ingot.upload_intents_evictable_idx;
CREATE INDEX upload_intents_evictable_idx
    ON ingot.upload_intents (updated_at, digest, space)
    WHERE evicted_at IS NULL AND state IN ('parked', 'accepted', 'published');

ALTER TABLE ingot.blob_parks ADD COLUMN space text;
-- A park's space is its session's. A park whose session is gone takes the
-- space of its pending release or of its intent.
UPDATE ingot.blob_parks p SET space = COALESCE(
    (SELECT s.space FROM ingot.multipart_parts mp
       JOIN ingot.multipart_sessions s ON s.upload_id = mp.upload_id
      WHERE mp.blob_digests @> ARRAY[p.digest] LIMIT 1),
    (SELECT r.space FROM ingot.blob_release_intents r WHERE r.digest = p.digest LIMIT 1),
    (SELECT i.space FROM ingot.upload_intents i WHERE i.digest = p.digest LIMIT 1));
-- A park nothing names has no release to run it: the provider expires the
-- allocation on its own.
DELETE FROM ingot.blob_parks WHERE space IS NULL;
ALTER TABLE ingot.blob_parks ALTER COLUMN space SET NOT NULL;
ALTER TABLE ingot.blob_parks
    DROP CONSTRAINT blob_parks_pkey,
    ADD PRIMARY KEY (space, digest);

-- +goose Down
-- One row per digest survives; the duplicates a shared digest produced are
-- indistinguishable without the space.
DELETE FROM ingot.blob_parks a USING ingot.blob_parks b
 WHERE a.digest = b.digest AND a.space > b.space;
ALTER TABLE ingot.blob_parks
    DROP CONSTRAINT blob_parks_pkey,
    ADD PRIMARY KEY (digest);
ALTER TABLE ingot.blob_parks DROP COLUMN space;

DROP INDEX ingot.upload_intents_evictable_idx;
CREATE INDEX upload_intents_evictable_idx
    ON ingot.upload_intents (updated_at, digest)
    WHERE evicted_at IS NULL AND state IN ('parked', 'accepted', 'published');
DROP INDEX ingot.upload_intents_digest_idx;
DELETE FROM ingot.upload_intents a USING ingot.upload_intents b
 WHERE a.digest = b.digest AND a.space > b.space;
ALTER TABLE ingot.upload_intents
    DROP CONSTRAINT upload_intents_pkey,
    ADD PRIMARY KEY (digest);
ALTER TABLE ingot.upload_intents DROP COLUMN space;
