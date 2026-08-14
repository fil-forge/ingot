-- +goose Up
-- Object lock (docs/s3-object-lock.md §4.2). Per-version retention and legal
-- holds live in the catalog (the per-key version-state tree); the registry
-- carries only the bucket-level configuration and the CreateMultipartUpload
-- lock headers on their way to Complete.
--
--   object_lock_config — auth.BucketLockConfig JSON, stored verbatim. NULL =
--       never configured (GetObjectLockConfiguration answers
--       ObjectLockConfigurationNotFound).
--   lock_mode / lock_retain_until / lock_legal_hold — the x-amz-object-lock-*
--       headers of CreateMultipartUpload, stamped onto the version Complete
--       commits exactly as a single-shot PUT would have.
ALTER TABLE ingot.buckets
    ADD COLUMN object_lock_config bytea;

ALTER TABLE ingot.multipart_sessions
    ADD COLUMN lock_mode         text,
    ADD COLUMN lock_retain_until timestamptz,
    ADD COLUMN lock_legal_hold   text;

-- +goose Down
ALTER TABLE ingot.multipart_sessions
    DROP COLUMN lock_legal_hold,
    DROP COLUMN lock_retain_until,
    DROP COLUMN lock_mode;

ALTER TABLE ingot.buckets
    DROP COLUMN object_lock_config;
