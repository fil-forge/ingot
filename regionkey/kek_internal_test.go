package regionkey

import (
	"bytes"
	"os"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"
)

// TestKEKDestroyZeroesAndIsIdempotent checks the core hygiene contract of the
// locked buffer: Destroy wipes the bytes, and a second Destroy is a harmless
// no-op. It is an internal test because it inspects the buffer directly.
func TestKEKDestroyZeroesAndIsIdempotent(t *testing.T) {
	k, err := newKEK(KEKLen)
	require.NoError(t, err, "newKEK")

	for i := range k.page {
		k.page[i] = 0xAB
	}
	require.NotEqual(t, make([]byte, KEKLen), k.Bytes(), "key should be non-zero before Destroy")

	k.Destroy()
	require.Equal(t, make([]byte, len(k.page)), k.page, "Destroy must zero the whole locked page")
	require.Equal(t, make([]byte, KEKLen), k.Bytes(), "Destroy must zero the key")

	// Idempotent: a second Destroy neither panics nor changes anything.
	require.NotPanics(t, k.Destroy)
	require.Equal(t, make([]byte, KEKLen), k.Bytes())
}

// TestNewKEKCopiesAndLocks verifies NewKEK copies the caller's key into an
// independent locked buffer, so zeroing the caller's slice does not disturb the
// KEK.
func TestNewKEKCopiesAndLocks(t *testing.T) {
	key := bytes.Repeat([]byte{0xCD}, KEKLen)
	k, err := NewKEK(key)
	require.NoError(t, err, "NewKEK")
	defer k.Destroy()

	require.Equal(t, key, k.Bytes(), "NewKEK must copy the key bytes")

	// Zeroing the caller's slice must not affect the KEK's own copy.
	zero(key)
	require.Equal(t, bytes.Repeat([]byte{0xCD}, KEKLen), k.Bytes(), "KEK must hold an independent copy")
}

// TestKEKLocksAnExclusivePage checks the anti-page-sharing invariant: the
// locked window is page-aligned, a whole number of pages long, and lies
// entirely within the KEK's own backing allocation, so no other object can
// occupy a page this KEK locks and Destroy's unlock affects nothing else.
func TestKEKLocksAnExclusivePage(t *testing.T) {
	pageSize := os.Getpagesize()
	k, err := newKEK(KEKLen)
	require.NoError(t, err, "newKEK")
	defer k.Destroy()

	pageAddr := uintptr(unsafe.Pointer(&k.page[0]))
	require.Zero(t, int(pageAddr)%pageSize, "locked window must start on a page boundary")
	require.Equal(t, pageSize, len(k.page), "a KEKLen key locks exactly one page")

	backingStart := uintptr(unsafe.Pointer(&k.backing[0]))
	require.GreaterOrEqual(t, pageAddr, backingStart, "locked window must sit inside backing")
	require.LessOrEqual(t, pageAddr+uintptr(len(k.page)), backingStart+uintptr(len(k.backing)),
		"locked window must end inside backing")
}

func TestNewKEKRejectsNonPositiveSize(t *testing.T) {
	_, err := newKEK(0)
	require.Error(t, err)
	_, err = newKEK(-1)
	require.Error(t, err)
}
