-- +goose Up
-- Per-bucket CORS configuration (docs/s3-cors.md). A bucket's own document
-- replaces the gateway-wide one rendered from cors_allowed_origins, so it
-- rides the registry beside the object-lock configuration and the tag set.
--
--   cors_config — the <CORSConfiguration> XML versitygw's PutBucketCors
--       controller validated, stored verbatim. NULL = no document of the
--       bucket's own (GetBucketCors answers the gateway-wide one, or
--       NoSuchCORSConfiguration when CORS is disabled); DeleteBucketCors
--       restores NULL.
ALTER TABLE ingot.buckets
    ADD COLUMN cors_config bytea;

-- +goose Down
ALTER TABLE ingot.buckets
    DROP COLUMN cors_config;
