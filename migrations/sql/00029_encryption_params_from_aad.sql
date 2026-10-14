-- +goose Up
-- A blob's FEE base nonce and chunk size are in its protected header, which aad
-- already holds whole (element 1 of the COSE Enc_structure). The read path
-- reads them from there, so the separate columns only duplicated it.
ALTER TABLE ingot.blob_encryption_params
    DROP COLUMN base_nonce,
    DROP COLUMN chunk_size;

-- +goose Down
-- The values cannot be restored from this table alone, so the columns come
-- back nullable; each row's aad still carries them.
ALTER TABLE ingot.blob_encryption_params
    ADD COLUMN base_nonce bytea,
    ADD COLUMN chunk_size bigint;
