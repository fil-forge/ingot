-- +goose Up
-- FEE (FilOne encryption envelope) per-blob encryption parameters. When Ingot
-- encrypts an object's body, each body blob is stored as an independent
-- COSE/STREAM ciphertext envelope; a range GET must be able to decrypt any byte
-- span of that envelope WITHOUT first fetching and parsing its header. A row
-- here caches exactly the inputs the read path's decryptor needs, so a read
-- unwraps the CEK (under the region KEK) and goes straight to a body-range
-- fetch — no envelope-header round-trip.
--
-- The existence of a row is what marks a blob as encrypted, so every column is
-- NOT NULL: there is no such thing as a half-populated parameter set the
-- decrypt path could not use. An unencrypted blob simply has no row.
--
-- Deliberately a separate table from ingot.blob_locations, and deliberately
-- WITHOUT a foreign key to it. blob_locations is a reconstructible cache of the
-- indexing-service contract — every row can be re-derived from the indexer or
-- the accept receipt, and the table goes away when the topology moves to a real
-- indexer. A wrapped CEK is not reconstructible: lose the row and the
-- ciphertext is permanently unreadable. The two therefore have independent
-- lifecycles, and an FK would additionally force location-before-parameters
-- write ordering.
--
-- aad holds the whole COSE Enc_structure rather than just the protected header,
-- because the structure's context string differs between a COSE_Encrypt and a
-- COSE_Encrypt0 and a bare row cannot record which form was used. The protected
-- header stays recoverable from it as element 1.
--
-- Raw CEK bytes are never stored — only the CEK wrapped under the region KEK
-- (region_wrapped_cek) plus the opaque key-version / recipient identifiers
-- needed to unwrap it. Per-blob crypto-shred is deleting the row; because there
-- is no cascade, a caller removing a blob must delete here as well as from
-- blob_locations. Re-wrap under a rotated region key is an upsert that replaces
-- region_wrapped_cek and region_key_version in place.
CREATE TABLE ingot.blob_encryption_params (
    space                text   NOT NULL,
    digest               bytea  NOT NULL,                 -- ciphertext blob multihash, as in blob_locations
    region_wrapped_cek   bytea  NOT NULL                  -- CEK wrapped under the region KEK (A256KW)
                             CHECK (octet_length(region_wrapped_cek) > 0),
    region_key_version   text   NOT NULL                  -- opaque id of the region KEK version used (rotation re-wrap)
                             CHECK (region_key_version <> ''),
    tenant_recipient_kid text   NOT NULL                  -- opaque id of the Hilt wrap key (insurance-recovery unwrap)
                             CHECK (tenant_recipient_kid <> ''),
    header_len           bigint NOT NULL                  -- encoded envelope length; the ciphertext starts at this offset
                             CHECK (header_len > 0),
    base_nonce           bytea  NOT NULL                  -- COSE iv: the STREAM nonce seed for this blob's ciphertext
                             CHECK (octet_length(base_nonce) > 0),
    chunk_size           bigint NOT NULL                  -- FEE chunk size from the COSE protected header
                             CHECK (chunk_size > 0),
    aad                  bytea  NOT NULL                  -- COSE Enc_structure, bound into every chunk's GCM tag
                             CHECK (octet_length(aad) > 0),
    created_at           timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (space, digest)
);

-- +goose Down
DROP TABLE ingot.blob_encryption_params;
