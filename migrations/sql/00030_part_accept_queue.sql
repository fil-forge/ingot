-- +goose Up
-- The part rows are the queue the upload's acceptor works through while the
-- client is still uploading. A part is written 'spooled' before its blobs
-- park, moves to 'parked' once every blob has a park (or a location) and is
-- ready to conclude, and to 'accepted' once its blobs are accepted. A part
-- still 'spooled' at Complete is one whose request died mid-park. Existing
-- rows keep 'parked', which is what the acceptor picks up.
ALTER TABLE ingot.multipart_parts
    DROP CONSTRAINT multipart_parts_state_check,
    ADD CONSTRAINT multipart_parts_state_check
        CHECK (state IN ('spooled','parked','accepted')),
    ALTER COLUMN state SET DEFAULT 'spooled';

-- The length of the span each blob covers in the object, parallel to
-- blob_digests: how many of the part's bytes it carries, before encryption,
-- so Complete lays out the object's spans from the part rows alone.
-- blob_digests name the blobs as stored (the FEE envelope when the session
-- encrypts), so for an encrypted part these lengths are smaller than the
-- stored blobs. Rows written before the column leave it NULL and Complete
-- derives their lengths per blob instead.
ALTER TABLE ingot.multipart_parts
    ADD COLUMN span_lengths bigint[],
    ADD CONSTRAINT multipart_parts_span_lengths_check
        CHECK (span_lengths IS NULL OR cardinality(span_lengths) = cardinality(blob_digests));

-- The acceptor lists an upload's parked parts in part order on every pass;
-- the part number in the key lets it read them in that order, past none of
-- the parts already accepted. The restart sweep and the oldest-wait query use
-- the upload_id prefix.
CREATE INDEX multipart_parts_parked
    ON ingot.multipart_parts (upload_id, part_number) WHERE state = 'parked';

-- +goose Down
-- A spooled part reads as parked to the earlier code, whose Complete concludes
-- what has a park and uploads what has none.
DROP INDEX ingot.multipart_parts_parked;
ALTER TABLE ingot.multipart_parts
    DROP CONSTRAINT multipart_parts_span_lengths_check,
    DROP COLUMN span_lengths;
UPDATE ingot.multipart_parts SET state = 'parked' WHERE state = 'spooled';
ALTER TABLE ingot.multipart_parts
    DROP CONSTRAINT multipart_parts_state_check,
    ADD CONSTRAINT multipart_parts_state_check
        CHECK (state IN ('parked','accepted')),
    ALTER COLUMN state SET DEFAULT 'parked';
