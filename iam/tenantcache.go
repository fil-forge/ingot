package iam

import (
	"time"

	"github.com/fil-forge/ucantone/did"
	gocache "github.com/patrickmn/go-cache"
)

// TenantCache is a TTL cache of the tenant DID Hilt returns from
// /s3/request/authorize, keyed by access key id. An access key belongs to
// exactly one tenant for its whole life, so the entry is as stable as the
// verification key it is cached alongside (same TTL, cleared together by
// the Revoker). The local fast path reads it to stash the tenant on the
// request — the write path encrypts every object to the tenant's wrap key
// and refuses to write without one — and falls through to Hilt when it is
// missing, so a cache gap never yields a tenant-less write.
type TenantCache struct {
	data *gocache.Cache
}

// NewTenantCache returns an empty cache.
func NewTenantCache() *TenantCache {
	return &TenantCache{data: gocache.New(gocache.NoExpiration, cacheJanitorInterval)}
}

// Put caches tenant under access for ttl. Undefined tenants and non-positive
// TTLs are skipped.
func (c *TenantCache) Put(access string, ttl time.Duration, tenant did.DID) {
	if ttl <= 0 || !tenant.Defined() {
		return
	}
	c.data.Set(access, tenant, ttl)
}

// Get returns the cached tenant for access, if present and unexpired.
func (c *TenantCache) Get(access string) (did.DID, bool) {
	v, ok := c.data.Get(access)
	if !ok {
		return did.Undef, false
	}
	tenant, ok := v.(did.DID)
	return tenant, ok
}

// Delete drops the cached tenant for access.
func (c *TenantCache) Delete(access string) {
	c.data.Delete(access)
}
