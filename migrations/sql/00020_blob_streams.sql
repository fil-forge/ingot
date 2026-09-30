-- +goose Up
-- A body blob can be allocated before its digest is known and its bytes sent
-- to the provider as they are encrypted. Until the upload is concluded or
-- parked, the /blob/add task link is the only name the provider knows it by,
-- so each such upload has a row here from allocation until its park or
-- acceptance is recorded. A row left behind by a request that died is aborted
-- on the provider by that task link and then deleted.
CREATE TABLE ingot.blob_streams (
    add_task   bytea PRIMARY KEY,  -- /blob/add task CID (the abort cause)
    space      text        NOT NULL,
    bucket     text        NOT NULL,
    size       bigint      NOT NULL,  -- envelope bytes allocated
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX blob_streams_created_at ON ingot.blob_streams (created_at);

-- +goose Down
DROP TABLE ingot.blob_streams;
