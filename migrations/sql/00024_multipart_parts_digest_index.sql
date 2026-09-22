-- +goose Up
-- A release asks whether any part of an in-flight session still references
-- its digest (CountLivePartRefs, CountPartRefs). The parts table has only its
-- (upload_id, part_number) key, so a digest lookup scanned every retained
-- part row, once per release. The GIN index answers the array-containment
-- predicate (blob_digests @> ARRAY[digest]) those queries use.
CREATE INDEX multipart_parts_blob_digests_gin
    ON ingot.multipart_parts USING gin (blob_digests);

-- +goose Down
DROP INDEX ingot.multipart_parts_blob_digests_gin;
