-- +goose Up
-- FEE (Filecoin Encryption Envelope) per-blob encryption parameters. When Ingot
-- encrypts an object's body, each body blob is stored as an independent
-- COSE/STREAM ciphertext envelope; a range GET must be able to decrypt any byte
-- span of that envelope WITHOUT first fetching and parsing its header. A row
-- here holds exactly the inputs the read path's decryptor needs, so a read
-- unwraps the CEK (via the region's secrets manager, which custodies the region
-- KEK) and goes straight to a body-range fetch — no envelope-header round-trip.
--
-- Every body blob is written encrypted, so every body blob has a row (written
-- before its manifest commits), and every column is NOT NULL: there is no such
-- thing as a half-populated parameter set the decrypt path could not use. A
-- referenced blob with a missing row is a read error (fail closed), never
-- "stored as plaintext".
--
-- Deliberately a separate table from ingot.blob_locations, and deliberately
-- WITHOUT a foreign key to it. blob_locations is a reconstructible cache of the
-- indexing-service contract — every row can be re-derived from the indexer or
-- the accept receipt, and the table goes away when the topology moves to a real
-- indexer. A row here is instead the marker that a blob is encrypted, so the
-- two have independent lifecycles, and an FK would additionally force
-- location-before-parameters write ordering.
--
-- aad holds the whole COSE Enc_structure rather than just the protected header,
-- because the structure's context string differs between a COSE_Encrypt and a
-- COSE_Encrypt0 and a bare row cannot record which form was used. The protected
-- header stays recoverable from it as element 1.
--
-- Only what the region-KEK read path needs is stored. The COSE recipients,
-- including the tenant wrap key the insurance-recovery unwrap uses, stay in the
-- envelope header: that path is rare and out-of-band, and it reads the header
-- anyway. Raw key bytes never touch this table: region_wrapped_cek is
-- ciphertext, unwrappable only by the region KEK, which lives in the region's
-- secrets manager and never leaves it. Deleting a row is therefore the per-blob
-- crypto-shred — without the wrapped CEK the region has no path to the
-- plaintext (only the tenant recipient in the envelope survives, by design).
-- Because there is no cascade, a caller removing a blob must delete here as
-- well as from blob_locations.
CREATE TABLE ingot.blob_encryption_params (
    space                text   NOT NULL,
    digest               bytea  NOT NULL,                 -- ciphertext blob multihash, as in blob_locations
    region_wrapped_cek   bytea  NOT NULL                  -- CEK wrapped by the region key provider (e.g. transit ciphertext); never the raw CEK
                             CHECK (octet_length(region_wrapped_cek) > 0),
    region_key_version   text   NOT NULL                  -- opaque id of the region KEK version that produced the wrap
                             CHECK (region_key_version <> ''),
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
