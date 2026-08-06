-- +goose Up
-- Per-part checksums (FIL-620): the base64 checksum of the part's bytes. When
-- the session declares a checksum algorithm this is that algorithm's value
-- (client-validated or server-computed); when it declares none this is the
-- internal full-object CRC64NVME used to derive the default final checksum at
-- Complete, and is never echoed by ListParts.
ALTER TABLE ingot.multipart_parts ADD COLUMN checksum text NOT NULL DEFAULT '';

-- +goose Down
ALTER TABLE ingot.multipart_parts DROP COLUMN checksum;
