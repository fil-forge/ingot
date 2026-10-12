package s3frontend

import (
	"sync"

	"github.com/fil-forge/ucantone/did"
	"github.com/multiformats/go-multihash"
)

// blobKeyLock serialises, within this process, the writes that make one
// (space, digest) durable on its provider: the check for a location or a
// park, the upload or park, and the row that records it. Two writes of the
// same bytes to one space, which a bucket stored as received allows, would
// otherwise both miss the row and both allocate; with the lock the second
// finds the first's row and reuses it. The registry stays the guard between
// instances (ParkStore.PutPark refuses to overwrite a park); this lock is
// the cheaper one that closes the race where it happens.
type blobKeyLock struct {
	mu    sync.Mutex
	locks map[string]*blobKeyEntry
}

type blobKeyEntry struct {
	mu      sync.Mutex
	waiters int
}

// lock takes the key and returns the function that releases it.
func (l *blobKeyLock) lock(space did.DID, digest multihash.Multihash) func() {
	key := space.String() + "\x00" + string(digest)
	l.mu.Lock()
	if l.locks == nil {
		l.locks = map[string]*blobKeyEntry{}
	}
	e, ok := l.locks[key]
	if !ok {
		e = &blobKeyEntry{}
		l.locks[key] = e
	}
	e.waiters++
	l.mu.Unlock()
	e.mu.Lock()
	return func() {
		e.mu.Unlock()
		l.mu.Lock()
		e.waiters--
		if e.waiters == 0 {
			delete(l.locks, key)
		}
		l.mu.Unlock()
	}
}
