package bucketqueue

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestVersionID_Classify(t *testing.T) {
	kind, _ := ClassifyVersionID("")
	require.Equal(t, VersionKindCurrent, kind, "empty")
	kind, _ = ClassifyVersionID("null")
	require.Equal(t, VersionKindNull, kind, "null")
	// A minted token round-trips to its seq.
	tok := mintVersionID(42)
	kind, seq := ClassifyVersionID(tok)
	require.Equal(t, VersionKindToken, kind, "minted token")
	require.Equal(t, uint64(42), seq, "minted token")
	// A foreign-but-well-formed ULID is a token (resolution rejects it later).
	kind, _ = ClassifyVersionID("01G65Z755AFWAKHE12NY0CQ9FH")
	require.Equal(t, VersionKindToken, kind, "foreign ULID")
	// Malformed ids are invalid.
	for _, bad := range []string{"invalid_version_id", "../../secret.txt", "abc", "NULL "} {
		kind, _ = ClassifyVersionID(bad)
		require.Equal(t, VersionKindInvalid, kind, "%q", bad)
	}
}

func TestRevSeqKey_NewestFirst(t *testing.T) {
	// Larger seq must sort lexicographically smaller (newest-first walk).
	keys := []string{RevSeqKey(1), RevSeqKey(2), RevSeqKey(10), RevSeqKey(1 << 40)}
	sorted := append([]string(nil), keys...)
	sort.Strings(sorted)
	for i := range keys {
		require.Equal(t, sorted[i], keys[len(keys)-1-i], "RevSeqKey ordering broken: %v (sorted %v)", keys, sorted)
	}
}
