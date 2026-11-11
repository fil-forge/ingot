package spaceshardqueue

import (
	"bytes"
	"strings"
	"testing"

	"github.com/fil-forge/libforge/testutil"
	"github.com/multiformats/go-multihash"
)

// A subject parses back to the space and digest it was made from, and falls
// under its space's pattern.
func TestSubjectRoundTrips(t *testing.T) {
	space := testutil.RandomDID(t)
	digest := multihash.Multihash([]byte{0x12, 0x02, 0xab, 0xcd})
	subject := Subject(space, digest)
	gotSpace, gotDigest, err := ParseSubject(subject)
	if err != nil {
		t.Fatalf("ParseSubject(%q): %v", subject, err)
	}
	if gotSpace != space || !bytes.Equal(gotDigest, digest) {
		t.Fatalf("ParseSubject(%q) = %s, %x; want %s, %x", subject, gotSpace, gotDigest, space, digest)
	}
	if pattern := SpaceSubjects(space); !strings.HasPrefix(subject, strings.TrimSuffix(pattern, "*")) {
		t.Fatalf("subject %q is not under its space's pattern %q", subject, pattern)
	}
	for _, bad := range []string{"no-digest", space.String() + "/not-hex", "not-a-did/abcd"} {
		if _, _, err := ParseSubject(bad); err == nil {
			t.Errorf("ParseSubject(%q) succeeded, want an error", bad)
		}
	}
}
