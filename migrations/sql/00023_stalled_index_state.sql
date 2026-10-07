-- +goose Up
-- StalledBytes sums stalled uploads by state, so its index carries state as
-- well as size and the sums still come from the index alone.
DROP INDEX ingot.upload_intents_stalled_idx;
CREATE INDEX upload_intents_stalled_idx
    ON ingot.upload_intents (updated_at) INCLUDE (size, state)
    WHERE state IN ('spooled', 'uploading');

-- +goose Down
DROP INDEX ingot.upload_intents_stalled_idx;
CREATE INDEX upload_intents_stalled_idx
    ON ingot.upload_intents (updated_at) INCLUDE (size)
    WHERE state IN ('spooled', 'uploading');
