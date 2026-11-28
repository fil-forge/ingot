package spaceshardqueue

import (
	"strings"
	"testing"

	"github.com/fil-forge/libforge/testutil"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A subject parses back to the space and digest it was made from, and falls
// under its space's pattern.
func TestSubjectRoundTrips(t *testing.T) {
	space := testutil.RandomDID(t)
	digest := multihash.Multihash([]byte{0x12, 0x02, 0xab, 0xcd})
	subject := Subject(space, digest)
	gotSpace, gotDigest, err := ParseSubject(subject)
	require.NoError(t, err, "ParseSubject(%q)", subject)
	require.Equal(t, space, gotSpace, "ParseSubject(%q) space", subject)
	require.Equal(t, digest, gotDigest, "ParseSubject(%q) digest", subject)
	pattern := SpaceSubjects(space)
	require.True(t, strings.HasPrefix(subject, strings.TrimSuffix(pattern, "*")), "subject %q is not under its space's pattern %q", subject, pattern)
	for _, bad := range []string{"no-digest", space.String() + "/not-hex", "not-a-did/abcd"} {
		_, _, err = ParseSubject(bad)
		assert.Error(t, err, "ParseSubject(%q)", bad)
	}
}
