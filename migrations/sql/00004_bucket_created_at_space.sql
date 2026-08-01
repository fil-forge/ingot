-- +goose Up
-- Bucket columns tighten up now that creation goes through Hilt:
--
--   created_at — becomes a database-owned timestamp: timestamptz with a
--       now() default (previously unix seconds supplied by the caller).
--   space      — loses its '' default. The default existed so inserts kept
--       working "until the write path threads the space DID through bucket
--       creation" (00003); Create now always supplies the space Hilt returns.
ALTER TABLE ingot.buckets
    ALTER COLUMN created_at TYPE timestamptz USING to_timestamp(created_at),
    ALTER COLUMN created_at SET DEFAULT now(),
    ALTER COLUMN created_at SET NOT NULL,
    ALTER COLUMN space DROP DEFAULT;

-- +goose Down
ALTER TABLE ingot.buckets
    ALTER COLUMN created_at DROP DEFAULT,
    ALTER COLUMN created_at TYPE bigint USING extract(epoch FROM created_at)::bigint,
    ALTER COLUMN space SET DEFAULT '';
