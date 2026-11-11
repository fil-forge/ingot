package bucketqueue

import (
	"sort"
	"testing"
)

func TestVersionID_Classify(t *testing.T) {
	if kind, _ := ClassifyVersionID(""); kind != VersionKindCurrent {
		t.Fatalf("empty → %v, want current", kind)
	}
	if kind, _ := ClassifyVersionID("null"); kind != VersionKindNull {
		t.Fatalf("null → %v, want null", kind)
	}
	// A minted token round-trips to its seq.
	tok := mintVersionID(42)
	kind, seq := ClassifyVersionID(tok)
	if kind != VersionKindToken || seq != 42 {
		t.Fatalf("minted token → (%v, %d), want (token, 42)", kind, seq)
	}
	// A foreign-but-well-formed ULID is a token (resolution rejects it later).
	if kind, _ := ClassifyVersionID("01G65Z755AFWAKHE12NY0CQ9FH"); kind != VersionKindToken {
		t.Fatalf("foreign ULID → %v, want token", kind)
	}
	// Malformed ids are invalid.
	for _, bad := range []string{"invalid_version_id", "../../secret.txt", "abc", "NULL "} {
		if kind, _ := ClassifyVersionID(bad); kind != VersionKindInvalid {
			t.Fatalf("%q → %v, want invalid", bad, kind)
		}
	}
}

func TestRevSeqKey_NewestFirst(t *testing.T) {
	// Larger seq must sort lexicographically smaller (newest-first walk).
	keys := []string{RevSeqKey(1), RevSeqKey(2), RevSeqKey(10), RevSeqKey(1 << 40)}
	sorted := append([]string(nil), keys...)
	sort.Strings(sorted)
	for i := range keys {
		if keys[len(keys)-1-i] != sorted[i] {
			t.Fatalf("RevSeqKey ordering broken: %v (sorted %v)", keys, sorted)
		}
	}
}
