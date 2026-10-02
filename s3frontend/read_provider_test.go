package s3frontend

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestReadsNeedOnlyTheProvider pins that a GET depends on what the provider
// holds: the write leaves no local copy, and whole and ranged reads of a
// multi-blob object, across blob and chunk boundaries, still return the
// object.
func TestReadsNeedOnlyTheProvider(t *testing.T) {
	const blobCeiling = 300 << 10
	b, _, _ := newRefTestBackend(t, blobCeiling)
	data := testBody(700 << 10) // three blobs

	putObj(t, b, "k1", data)
	digests := blobDigestsOf(t, b, "k1", "")
	require.Len(t, digests, 3)

	for _, d := range digests {
		_, err := os.Stat(b.spool.Path(d))
		require.ErrorIs(t, err, os.ErrNotExist, "the write left a local copy")
	}

	_, got, err := getObjV(t, b, "k1", "")
	require.NoError(t, err)
	require.Equal(t, data, got)

	// A range that starts in one blob and ends in the next.
	require.Equal(t, data[blobCeiling-10:blobCeiling+10], getRange(t, b, "k1", "bytes=307190-307209"))
}
