-- +goose Up
-- The outbox for the upload service's content-entry list: one row per object
-- version committed or retired, drained by the registration sweeper.
--
-- It exists so the object count survives what the request path cannot carry.
-- The row is written in the same transaction as the bucket-root CAS, so it
-- exists exactly when the version committed; the sweeper retries until the
-- upload service takes it, so a Sprue outage delays the count instead of
-- losing it; and seq replays the changes to one key in commit order, which a
-- per-request call could not guarantee once the bucket lock was released.
--
-- proofs is the delegation chain authorizing the invocation, captured from the
-- request that committed the version. The sweeper has no request to borrow
-- authority from and a space may never be written again, so the row carries
-- its own. It is bearer authority for one space, narrowed to this command, and
-- it is deleted with the row as soon as the registration lands.
--
-- That chain is short lived: hilt expires its delegation to the gateway at the
-- next UTC midnight, so a row queued late in the day may hold only minutes of
-- authority. The sweeper renews it from the space's live authority when it can.
-- When it cannot, the row is dead-lettered rather than retried forever:
-- dead_lettered_at takes it out of the sweep and, just as importantly, stops it
-- holding back the later rows for its key, which a permanently failing row
-- would otherwise freeze for good. dead_letter_reason says why, for whoever
-- comes looking.
CREATE TABLE ingot.upload_registrations (
    seq         bigserial PRIMARY KEY,
    bucket      text        NOT NULL,
    object_key  text        NOT NULL,
    space       text        NOT NULL,
    root        bytea       NOT NULL,   -- the version's manifest CID
    op          text        NOT NULL CHECK (op IN ('add', 'remove')),
    proofs      bytea       NOT NULL,   -- encoded UCAN container
    attempts    integer     NOT NULL DEFAULT 0,
    next_at     timestamptz NOT NULL DEFAULT now(),
    created_at  timestamptz NOT NULL DEFAULT now(),
    dead_lettered_at   timestamptz,
    dead_letter_reason text
);

-- The sweeper's claim order: due first, then commit order. Dead-lettered rows
-- are never claimed, so they stay out of the index.
CREATE INDEX upload_registrations_due_idx ON ingot.upload_registrations (next_at, seq)
    WHERE dead_lettered_at IS NULL;

-- +goose Down
DROP TABLE ingot.upload_registrations;
