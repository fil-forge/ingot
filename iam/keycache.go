package iam

import (
	"path"
	"time"

	s3 "github.com/fil-forge/libforge/commands/s3"
	gocache "github.com/patrickmn/go-cache"
)

// VerificationKeyCache is a TTL cache of the derived SigV4 verification keys
// Hilt returns from /s3/request/authorize, keyed by (access key id, key
// kind). A cached key lets the Service verify a request signature locally
// (the fast path) without a Hilt round-trip.
//
// The cache is deliberately dumb: the caller supplies the TTL (the Service
// uses until-next-UTC-midnight, mirroring Hilt's own key expiry — a SigV4
// derived key dies at the credential-scope date rollover regardless). A
// stale key is harmless: it fails verification and the request falls back
// to Hilt, which returns a fresh key.
type VerificationKeyCache struct {
	data *gocache.Cache
}

// NewVerificationKeyCache returns an empty cache.
func NewVerificationKeyCache() *VerificationKeyCache {
	return &VerificationKeyCache{data: gocache.New(gocache.NoExpiration, cacheJanitorInterval)}
}

// Put caches each key under (access, key.Kind) for ttl. Keys with no data
// are skipped.
func (k *VerificationKeyCache) Put(access string, ttl time.Duration, keys ...s3.VerificationKey) {
	if ttl <= 0 {
		return
	}
	for _, key := range keys {
		if len(key.Data) == 0 {
			continue
		}
		k.data.Set(path.Join(access, key.Kind), key.Data, ttl)
	}
}

// Get returns the cached key bytes for (access, kind), if present and
// unexpired.
func (k *VerificationKeyCache) Get(access, kind string) ([]byte, bool) {
	v, ok := k.data.Get(path.Join(access, kind))
	if !ok {
		return nil, false
	}
	data, ok := v.([]byte)
	return data, ok
}
