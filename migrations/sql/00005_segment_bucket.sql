-- +goose Up
-- The log is segregated per bucket: every segment now belongs to exactly one
-- bucket, whose Forge space its CAR ships to. Adding NOT NULL without a
-- default requires an empty table — the schema is dev-only; reset any
-- persistent dev database.
ALTER TABLE ingot.segments
    ADD COLUMN bucket text NOT NULL;
CREATE INDEX segments_bucket_idx ON ingot.segments (bucket);

-- +goose Down
DROP INDEX ingot.segments_bucket_idx;
ALTER TABLE ingot.segments
    DROP COLUMN bucket;
