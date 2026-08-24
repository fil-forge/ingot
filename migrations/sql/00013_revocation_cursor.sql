-- +goose Up
-- The revocation-firehose cursor: a single row holding the resume point in
-- the revocation service's (Swarf's) revocation stream. recorded_at is the
-- recorded_at of the last processed revocation record and becomes the
-- {since} path segment when (re)connecting to GET /revocations/{since}; an
-- absent row means "connect from now" (nothing revoked before the process
-- first subscribed can be sitting in its in-memory caches). revoke is the
-- CID of the delegation that record revoked, for observability.
CREATE TABLE ingot.revocation_cursor (
    id          boolean PRIMARY KEY DEFAULT true CHECK (id), -- single-row latch
    recorded_at timestamptz NOT NULL,
    revoke      bytea NOT NULL,     -- revoked delegation CID (bytes)
    updated_at  timestamptz NOT NULL DEFAULT now()
);

-- +goose Down
DROP TABLE ingot.revocation_cursor;
