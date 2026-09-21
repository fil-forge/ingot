-- +goose Up
-- 'uploading' is set before a blob's first network call and stands until a row
-- records the outcome (a park or a location). A release of a blob with neither
-- row reads it to tell a blob that never left this node (still 'spooled',
-- cleaned up locally) from one whose acceptance may have landed while its
-- location did not (removed on the network, idempotently).
ALTER TABLE ingot.upload_intents
    DROP CONSTRAINT upload_intents_state_check,
    ADD CONSTRAINT upload_intents_state_check
        CHECK (state IN ('spooled','uploading','parked','accepted','published'));

-- +goose Down
UPDATE ingot.upload_intents SET state = 'spooled' WHERE state = 'uploading';
ALTER TABLE ingot.upload_intents
    DROP CONSTRAINT upload_intents_state_check,
    ADD CONSTRAINT upload_intents_state_check
        CHECK (state IN ('spooled','parked','accepted','published'));
