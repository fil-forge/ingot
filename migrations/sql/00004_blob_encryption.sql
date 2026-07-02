-- +goose Up
-- FEE (FilOne encryption envelope) per-blob wrap material, added to
-- ingot.blob_locations. When Ingot encrypts an object's body, each body blob is
-- stored as an independent COSE/STREAM ciphertext envelope; a range GET must be
-- able to decrypt any byte span of that envelope WITHOUT first fetching and
-- parsing its header. These columns cache exactly the inputs the read path's
-- aesstream decryptor needs, so a read unwraps the CEK (under the region KEK)
-- and goes straight to a body-range fetch — no envelope-header round-trip.
--
-- All columns are nullable. An unencrypted blob carries a location row with
-- these columns NULL. Raw CEK bytes are never stored — only the CEK wrapped
-- under the region KEK (region_wrapped_cek) plus the key-version / recipient
-- identifiers needed to unwrap it. Per-blob crypto-shred is nulling these
-- columns (or deleting the row) — no separate mechanism. Re-wrap under a
-- rotated region key updates region_wrapped_cek and region_key_version in
-- place.
ALTER TABLE ingot.blob_locations
    ADD COLUMN region_wrapped_cek   bytea,   -- CEK wrapped under the region KEK (A256KW)
    ADD COLUMN region_key_version   text,    -- opaque id of the region KEK version used (rotation re-wrap)
    ADD COLUMN tenant_recipient_kid text,    -- opaque id of the Hilt wrap key (insurance-recovery unwrap)
    ADD COLUMN base_nonce           bytea,   -- COSE iv: the STREAM nonce seed for this blob's ciphertext
    ADD COLUMN chunk_size           bigint,  -- FEE chunk size from the COSE protected header
    ADD COLUMN protected_header     bytea;   -- raw COSE protected header bytes (Enc_structure/AAD reconstruction)

-- +goose Down
ALTER TABLE ingot.blob_locations
    DROP COLUMN protected_header,
    DROP COLUMN chunk_size,
    DROP COLUMN base_nonce,
    DROP COLUMN tenant_recipient_kid,
    DROP COLUMN region_key_version,
    DROP COLUMN region_wrapped_cek;
