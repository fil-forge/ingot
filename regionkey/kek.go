package regionkey

import (
	"fmt"
	"os"
	"unsafe"
)

// KEK holds raw region key-encryption-key bytes in a locked, non-swappable
// memory buffer. It is the in-process home of a KEK for the brief window a
// [SoftwareProvider] needs it: the backing memory is pinned with mlock (or the
// platform equivalent) so it cannot be paged to disk, and [KEK.Destroy] zeroes
// and unlocks it.
//
// Ownership of a KEK passes to whoever receives it from a [KEKSource]. That
// owner MUST Destroy it as soon as its single wrap or unwrap completes, and
// should arrange the Destroy with a defer immediately after obtaining the KEK
// so the bytes are wiped on every path, including panics and errors. A KEK is
// not safe for concurrent use.
//
// mlock acts at page granularity: locking a slice locks the whole page(s) it
// spans, and unlocking it unlocks them. If two KEKs shared a page, destroying
// one would unlock the other. A KEK therefore takes sole ownership of the
// page(s) it locks by over-allocating and locking only a page-aligned window
// that lies entirely within its own allocation — so no other Go object can sit
// on a locked page, and Destroy's unlock touches nothing but this KEK.
type KEK struct {
	// backing is the raw over-allocated slice; the KEK retains it so the locked
	// window stays alive (Go's GC does not move heap allocations) and to keep
	// the page exclusively its own.
	backing []byte
	// page is the page-aligned, page-sized window within backing that is
	// mlocked; it is what Destroy zeroes and unlocks.
	page []byte
	// key is page[:n], the n bytes that hold the KEK itself.
	key       []byte
	destroyed bool
}

// NewKEK returns a KEK holding a copy of key in freshly locked memory. It is
// how a [KEKSource] implementation outside this package (for example a
// Vault-backed source) hands a fetched key to a [Provider]: read the raw bytes,
// copy them in with NewKEK, then zero the caller's own copy. The key slice is
// neither retained nor modified.
//
// It returns an error if the memory cannot be locked; see newKEK.
func NewKEK(key []byte) (*KEK, error) {
	k, err := newKEK(len(key))
	if err != nil {
		return nil, err
	}
	copy(k.key, key)
	return k, nil
}

// newKEK allocates a KEK of size bytes whose backing memory is locked. It
// returns an error if the memory cannot be locked (see mlock_unix.go and
// mlock_other.go); callers treat that as fatal rather than proceeding with an
// unlocked key, so a misconfigured host fails closed instead of silently
// weakening the exposure guarantee.
func newKEK(size int) (*KEK, error) {
	if size <= 0 {
		return nil, fmt.Errorf("region: KEK size must be positive, got %d", size)
	}

	pageSize := os.Getpagesize()
	pages := (size + pageSize - 1) / pageSize
	lockLen := pages * pageSize

	// Over-allocate by a page so a full lockLen-byte, page-aligned window is
	// guaranteed to fit inside this single allocation, giving the KEK exclusive
	// ownership of every page it locks.
	backing := make([]byte, lockLen+pageSize)
	addr := uintptr(unsafe.Pointer(&backing[0]))
	off := int((uintptr(pageSize) - addr%uintptr(pageSize)) % uintptr(pageSize))
	page := backing[off : off+lockLen]

	if err := lock(page); err != nil {
		zero(backing)
		return nil, fmt.Errorf("region: locking KEK memory: %w", err)
	}
	return &KEK{backing: backing, page: page, key: page[:size]}, nil
}

// Bytes returns the raw KEK bytes for use in a single operation. The slice
// aliases the locked buffer and is valid only until [KEK.Destroy]; callers must
// not retain it past the operation. After Destroy the slice is all zeroes.
func (k *KEK) Bytes() []byte {
	return k.key
}

// Destroy zeroes the KEK bytes and unlocks the backing memory. It is idempotent
// and safe to defer. After Destroy the KEK must not be used to wrap or unwrap.
func (k *KEK) Destroy() {
	if k.destroyed {
		return
	}
	k.destroyed = true
	// Zero the whole locked page (the key plus any slack) while it is still
	// guaranteed resident, then unlock. Any unlock error is unrecoverable here
	// and the bytes are already zeroed, so it is deliberately ignored.
	zero(k.page)
	_ = unlock(k.page)
}

// zero overwrites b with zeroes, a best-effort wipe of key material.
func zero(b []byte) {
	for i := range b {
		b[i] = 0
	}
}
