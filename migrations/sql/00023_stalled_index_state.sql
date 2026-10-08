-- +goose Up
-- StalledBytes sums stalled uploads by state, so its index carries state as
-- well as size and the sums still come from the index alone.
--
-- IF EXISTS because 00021 gained this index after it first shipped, so a
-- database that applied the earlier 00021 has no index to drop.
DROP INDEX IF EXISTS ingot.upload_intents_stalled_idx;
CREATE INDEX upload_intents_stalled_idx
    ON ingot.upload_intents (updated_at) INCLUDE (size, state)
    WHERE state IN ('spooled', 'uploading');

-- +goose Down
DROP INDEX ingot.upload_intents_stalled_idx;
CREATE INDEX upload_intents_stalled_idx
    ON ingot.upload_intents (updated_at) INCLUDE (size)
    WHERE state IN ('spooled', 'uploading');
