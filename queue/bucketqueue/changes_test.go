package bucketqueue

import (
	"bytes"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	msbucket "github.com/fil-forge/ingot/bucket"
)

// A change carries a body of more blobs than the codec's default cap, such as
// a 5 GiB PUT cut into 512 KiB blobs.
func TestPutChangeCarriesALargeBody(t *testing.T) {
	const n = 10240
	in := PutChange{HasBlobs: true, Blobs: make([]msbucket.BlobRef, n), Refs: make([]BlobRefTask, n)}
	for i := range n {
		d, err := multihash.Sum([]byte(strconv.Itoa(i)), multihash.SHA2_256, -1)
		require.NoError(t, err)
		in.Blobs[i] = msbucket.BlobRef{Digest: d, Start: int64(i) << 19, End: int64(i+1) << 19}
		in.Refs[i] = BlobRefTask{Digest: d, Ref: d[:16]}
	}
	var buf bytes.Buffer
	require.NoError(t, in.MarshalCBOR(&buf), "encode a change of %d blobs", n)
	var out PutChange
	require.NoError(t, out.UnmarshalCBOR(&buf), "decode a change of %d blobs", n)
	require.Len(t, out.Blobs, n, "decoded %d blobs and %d references", len(out.Blobs), len(out.Refs))
	require.Len(t, out.Refs, n, "decoded %d blobs and %d references", len(out.Blobs), len(out.Refs))
}

// The codec caps on a change's blob lists match the most blobs a body holds.
func TestPutChangeCapsMatchTheBodyCapacity(t *testing.T) {
	typ := reflect.TypeOf(PutChange{})
	for _, name := range []string{"Blobs", "Refs"} {
		f, _ := typ.FieldByName(name)
		var max int
		for _, opt := range strings.Split(f.Tag.Get("cborgen"), ",") {
			if v, ok := strings.CutPrefix(opt, "maxlen="); ok {
				max, _ = strconv.Atoi(v)
			}
		}
		assert.Equal(t, msbucket.MaxBodyBlobs, max, "%s maxlen", name)
	}
}
