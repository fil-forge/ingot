-- +goose Up
-- The bytes of object data GetObject sent to clients, by bucket space and by
-- hour. Each instance adds what it sent every few seconds, and the usage
-- reporter sends the auth service every row whose count has changed since
-- the service last acknowledged it.
--
-- bytes_reported is the count the service acknowledged. A report carries an
-- hour's total so far and the service replaces what it holds, so a row is due
-- whenever the two differ, the hour in progress included. dirty_since is when
-- the row last became due, for the age the reporter's gauge shows; it is NULL
-- while the row is up to date, and reported_at is when it last got there.
CREATE TABLE ingot.egress_usage (
    space          text        NOT NULL,
    hour           timestamptz NOT NULL,
    bytes_sent     bigint      NOT NULL CHECK (bytes_sent > 0),
    bytes_reported bigint      NOT NULL DEFAULT 0 CHECK (bytes_reported >= 0),
    dirty_since    timestamptz,
    reported_at    timestamptz,
    PRIMARY KEY (space, hour),
    CHECK ((dirty_since IS NULL) = (bytes_sent = bytes_reported))
);

-- The reporter reads the rows that are due, oldest hour first.
CREATE INDEX egress_usage_due_idx ON ingot.egress_usage (hour, space)
    WHERE bytes_sent <> bytes_reported;

-- The last batch of counts each instance added. An instance numbers its
-- batches and a batch is added only if its number is above the one here, so an
-- instance that cannot tell whether an add took effect repeats it without
-- counting twice. An instance is a process: a restart starts a new row, and
-- rows untouched for the retention are deleted with the settled counts.
CREATE TABLE ingot.egress_flush (
    instance   text        PRIMARY KEY,
    seq        bigint      NOT NULL,
    flushed_at timestamptz NOT NULL
);

-- +goose Down
DROP TABLE ingot.egress_flush;
DROP TABLE ingot.egress_usage;
