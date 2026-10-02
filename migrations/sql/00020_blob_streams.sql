-- +goose Up
-- A body blob can be allocated before its digest is known and its bytes sent
-- to the provider as they are encrypted. Until the upload is concluded or
-- parked, the /blob/add task link is the only name the provider knows it by,
-- so each such upload has a row here from allocation until its park or
-- acceptance is recorded. The request that owns a row renews touched_at while
-- it runs; a row whose lease has lapsed was left behind by a request that
-- died, and is aborted on the provider by that task link and then deleted,
-- unless its blob was parked.
CREATE TABLE ingot.blob_streams (
    add_task   bytea PRIMARY KEY,  -- /blob/add task CID (the abort cause)
    space      text        NOT NULL,
    bucket     text        NOT NULL,
    size       bigint      NOT NULL,  -- envelope bytes allocated
    created_at timestamptz NOT NULL DEFAULT now(),
    touched_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX blob_streams_touched_at ON ingot.blob_streams (touched_at);

-- The stream sweeper asks whether a stream's blob was parked by its add task.
CREATE INDEX blob_parks_add_task ON ingot.blob_parks (add_task);

-- +goose Down
DROP INDEX ingot.blob_parks_add_task;
DROP TABLE ingot.blob_streams;
