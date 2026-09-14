package iam

import (
	"slices"
	"time"

	gocache "github.com/patrickmn/go-cache"
)

// PermissionCache remembers, per access key, the S3 permissions Hilt reports
// for it in an authorize response, to the same horizon as the key's derived
// signing key. The local fast path requires the permission each bucket of a
// request needs to be present here: a delegation chain alone cannot tell
// permissions apart (s3:PutObject's Forge commands include the retrieve a
// write's cleanup needs, which is also all s3:GetObject maps to), so without
// this a key granted only PutObject could copy from a bucket it may not read.
// A missing entry falls through to Hilt, so a cache gap never widens access.
type PermissionCache struct {
	data *gocache.Cache
}

// NewPermissionCache returns an empty cache.
func NewPermissionCache() *PermissionCache {
	return &PermissionCache{data: gocache.New(gocache.NoExpiration, cacheJanitorInterval)}
}

// Put caches permissions under access for ttl. Non-positive TTLs are skipped;
// an empty permission list is cached as such (the key may do nothing locally).
func (c *PermissionCache) Put(access string, ttl time.Duration, permissions []string) {
	if ttl <= 0 {
		return
	}
	c.data.Set(access, slices.Clone(permissions), ttl)
}

// Has reports whether access is cached and holds permission.
func (c *PermissionCache) Has(access, permission string) bool {
	v, ok := c.data.Get(access)
	if !ok {
		return false
	}
	perms, ok := v.([]string)
	return ok && slices.Contains(perms, permission)
}

// Delete drops the cached permissions for access.
func (c *PermissionCache) Delete(access string) {
	c.data.Delete(access)
}
