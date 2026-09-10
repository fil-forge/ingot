-- +goose Up
-- The tenant that owns each bucket: the did:plc hilt names as the caller's
-- tenant in its authorize response, recorded by CreateBucket. Copy paths read
-- it to refuse a copy source owned by another tenant, which hilt cannot see
-- (it authorizes a copy against the destination bucket only).
--
-- Rows that predate the column are backfilled with a sentinel that is a valid
-- DID (so the registry parses it like any other row) and can never equal a
-- hilt tenant, which is always did:plc. The Go home of the sentinel is
-- registry.UnknownTenant.
ALTER TABLE ingot.buckets ADD COLUMN tenant text;
UPDATE ingot.buckets SET tenant = 'did:web:unknown-tenant.invalid' WHERE tenant IS NULL;
ALTER TABLE ingot.buckets
    ALTER COLUMN tenant SET NOT NULL,
    ADD CONSTRAINT buckets_tenant_nonempty CHECK (tenant <> '');

-- +goose Down
ALTER TABLE ingot.buckets DROP COLUMN tenant;
