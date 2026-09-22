-- +goose Up
-- Multipart session teardown and the blob releases it records: a session
-- carries the space its parts were parked in, the sweeper measures staleness
-- from the last state change, a committed blob's intent is marked published,
-- an intent can say a blob may have reached the network, and the digest
-- lookup over the part rows is indexed. The code reads all of it.

-- A multipart session records its bucket's space, so tearing the session down
-- never depends on the bucket row: a session created while its bucket was
-- being deleted can outlive the bucket, and its part rows are the only index
-- to blobs the space still holds. Rows that predate the column take their
-- bucket's space where the bucket row is still there.
ALTER TABLE ingot.multipart_sessions
    ADD COLUMN space text NOT NULL DEFAULT '';
UPDATE ingot.multipart_sessions s
    SET space = b.space
    FROM ingot.buckets b
    WHERE s.bucket = b.name;

-- A row left empty by that backfill has no space to release its blobs
-- against, and resolving its bucket name instead is unsafe: a name recreated
-- later names another space and another tenant. Such a row is a dead index
-- and goes, its parts cascading with it. The column is then required, so no
-- session can be created or read without a space.
DELETE FROM ingot.multipart_sessions WHERE space = '';
ALTER TABLE ingot.multipart_sessions
    ADD CONSTRAINT multipart_sessions_space_check CHECK (space <> '');

-- The session sweeper measures staleness from the last state change rather
-- than from creation. A Complete latches an open session to 'completing' as it
-- starts, so a live Complete on an old session holds the row for a TTL of its
-- own instead of racing a sweep that would abort the parts it is concluding.
-- Existing rows count from their creation, as before.
ALTER TABLE ingot.multipart_sessions
    ADD COLUMN state_changed_at timestamptz NOT NULL DEFAULT now();
UPDATE ingot.multipart_sessions SET state_changed_at = created_at;

-- A committed blob's upload intent is 'published', written with its first
-- reference claim; a release recognises a committed blob by it once the claims
-- are gone and keeps the spool copy (the insurance copy until eviction) and the
-- intent. Blobs committed before the state existed carry 'accepted' intents and
-- their claims are the only trace: mark them, so their eventual release does not
-- take them for never-committed part blobs and remove the spool copy.
UPDATE ingot.upload_intents i
   SET state = 'published', updated_at = now()
 WHERE i.state <> 'published'
   AND EXISTS (SELECT 1 FROM ingot.blob_refs r WHERE r.digest = i.digest);

-- 'uploading' is set before a blob's first network call and stands until a row
-- records the outcome (a park or a location). A release of a blob with neither
-- row reads it to tell a blob that never left this node (still 'spooled',
-- cleaned up locally) from one whose acceptance may have landed while its
-- location did not (removed on the network, idempotently).
ALTER TABLE ingot.upload_intents
    DROP CONSTRAINT upload_intents_state_check,
    ADD CONSTRAINT upload_intents_state_check
        CHECK (state IN ('spooled','uploading','parked','accepted','published'));

-- A release asks whether any part of an in-flight session still references
-- its digest (CountLivePartRefs, CountPartRefs). The parts table has only its
-- (upload_id, part_number) key, so a digest lookup scanned every retained
-- part row, once per release. The GIN index answers the array-containment
-- predicate (blob_digests @> ARRAY[digest]) those queries use.
CREATE INDEX multipart_parts_blob_digests_gin
    ON ingot.multipart_parts USING gin (blob_digests);

-- +goose Down
-- The published intents and the deleted sessions stay as they are: those
-- intents named committed blobs before and after, and those sessions had no
-- space to be torn down against.
DROP INDEX ingot.multipart_parts_blob_digests_gin;
UPDATE ingot.upload_intents SET state = 'spooled' WHERE state = 'uploading';
ALTER TABLE ingot.upload_intents
    DROP CONSTRAINT upload_intents_state_check,
    ADD CONSTRAINT upload_intents_state_check
        CHECK (state IN ('spooled','parked','accepted','published'));
ALTER TABLE ingot.multipart_sessions DROP CONSTRAINT multipart_sessions_space_check;
ALTER TABLE ingot.multipart_sessions DROP COLUMN state_changed_at;
ALTER TABLE ingot.multipart_sessions DROP COLUMN space;
