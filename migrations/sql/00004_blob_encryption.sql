-- +goose Up
-- FEE (FilOne encryption envelope) per-blob wrap material, added to
-- ingot.blob_locations. When Ingot encrypts an object's body, each body blob
-- is stored as an independent COSE/STREAM ciphertext envelope; a (range) GET
-- must be able to decrypt any byte span of that envelope WITHOUT first fetching
-- and parsing its header. These columns cache exactly the inputs the read
-- path's aesstream decryptor needs, so a read unwraps the CEK (under the region
-- KEK) and goes straight to a body-range fetch — no envelope-header round-trip.
-- See FIL-480; the aesstream.Config inputs are FIL-569 / FIL-472 / FIL-487.
--
-- WHY blob_locations rather than a new table. blob_locations is keyed by
-- (space, digest) — exactly the granularity FEE context lives at. A fresh CEK
-- is generated per encryption event, so every ciphertext digest is unique to
-- one encryption (never shared across objects, even for identical plaintext).
-- The wrap material is therefore a 1:1 fact about the row, just like the
-- existing provider/url/size columns. This broadens blob_locations from "where
-- the bytes are" toward "the full per-blob record" — a rename (e.g.
-- blob_records) is worth discussing but out of scope here (see the FIL-480
-- reviewer note).
--
-- Object-level values (plaintext size, ETag, object identity) are NOT stored
-- here: they live in the content-addressed ObjectManifest / MST leaf, computed
-- before encryption in the PUT pipeline, and an overwrite already swaps the MST
-- leaf to a new manifest CID atomically — so no plaintext_* columns and no
-- generation counter are needed. No region column either: one Ingot instance is
-- one region, so region_key_version alone names which region KEK version wrapped
-- the CEK.
--
-- All columns are nullable. An unencrypted blob (the appliance topology today)
-- carries a location row with these columns NULL. Raw CEK bytes are never
-- stored — only the CEK wrapped under the region KEK (region_wrapped_cek) plus
-- the key-version / recipient identifiers needed to unwrap it. Per-blob
-- crypto-shred is nulling these columns (or deleting the row) — no separate
-- mechanism. Re-wrap under a rotated region key updates region_wrapped_cek and
-- region_key_version in place. Because region_key_version / tenant_recipient_kid
-- are opaque identifiers, no schema change is needed if the region-key or Hilt
-- wrap-key cardinality decisions (FIL-572 / FIL-574) later go multi-key.
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
