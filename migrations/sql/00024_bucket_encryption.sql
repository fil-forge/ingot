-- +goose Up
-- Bucket default encryption: the SSEAlgorithm of the bucket's S3
-- ServerSideEncryptionConfiguration, as PutBucketEncryption stored it.
--
--   buckets.encryption — 'AES256' (every body blob stored as a FEE envelope,
--       which is also what an unconfigured bucket does) or 'none' (body blobs
--       stored as received: no envelope, no blob_encryption_params row, a
--       non-standard value the deployment opts into with
--       encryption.allow_none). NULL = never configured, or deleted since
--       (GetBucketEncryption answers ServerSideEncryptionConfigurationNotFound).
--   multipart_sessions.plaintext — whether the session's object is stored as
--       received, decided at CreateMultipartUpload from the bucket's
--       encryption and the request's x-amz-server-side-encryption header,
--       applied to every part and stamped onto the manifest at Complete.
ALTER TABLE ingot.buckets
    ADD COLUMN encryption text
        CHECK (encryption IN ('AES256', 'none'));

ALTER TABLE ingot.multipart_sessions
    ADD COLUMN plaintext boolean NOT NULL DEFAULT false;

-- +goose Down
ALTER TABLE ingot.multipart_sessions
    DROP COLUMN plaintext;

ALTER TABLE ingot.buckets
    DROP COLUMN encryption;
