package s3frontend

import (
	"bytes"
	"context"
	"encoding/base64"
	"io"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fasthttp"
	"lukechampine.com/blake3"
	"lukechampine.com/blake3/bao"

	"github.com/fil-forge/ingot/blake3tree"
)

// TestCIDHeader checks that GET and HEAD return the object's CID, a raw-codec
// CIDv1 over the body's BLAKE3 hash, when the backend is handed the request
// as its context the way versitygw does, and that a plain context sets
// nothing.
func TestCIDHeader(t *testing.T) {
	b, _, _ := newRefTestBackend(t, 4096)
	data := bytes.Repeat([]byte("cid header "), 2000) // three blobs at a 4 KiB split
	putObj(t, b, "k", data)

	sum := blake3.Sum256(data)
	digest, err := mh.Encode(sum[:], mh.BLAKE3)
	require.NoError(t, err)
	want := cid.NewCidV1(cid.Raw, digest).String()

	bucket, key := "bk", "k"
	t.Run("head", func(t *testing.T) {
		rc := newRequestCtx()
		_, err := b.HeadObject(rc, &s3.HeadObjectInput{Bucket: &bucket, Key: &key})
		require.NoError(t, err, "HeadObject")
		got := string(rc.Response.Header.Peek(cidHeader))
		require.Equal(t, want, got, "%s", cidHeader)
	})
	t.Run("get", func(t *testing.T) {
		rc := newRequestCtx()
		out, err := b.GetObject(rc, &s3.GetObjectInput{Bucket: &bucket, Key: &key})
		require.NoError(t, err, "GetObject")
		got, err := io.ReadAll(out.Body)
		require.NoError(t, err, "read body")
		require.Equal(t, data, got, "body")
		require.Equal(t, want, string(rc.Response.Header.Peek(cidHeader)), "%s", cidHeader)
	})
	t.Run("ranged get", func(t *testing.T) {
		rc := newRequestCtx()
		out, err := b.GetObject(rc, &s3.GetObjectInput{Bucket: &bucket, Key: &key, Range: aws.String("bytes=4000-4200")})
		require.NoError(t, err, "GetObject")
		_, _ = io.ReadAll(out.Body)
		got := string(rc.Response.Header.Peek(cidHeader))
		require.Equal(t, want, got, "%s", cidHeader)
	})
	t.Run("plain context", func(t *testing.T) {
		_, err := b.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: &bucket, Key: &key})
		require.NoError(t, err, "HeadObject")
	})
}

// TestBlake3Attribute checks GetObjectAttributes returns the Blake3 element:
// the object's CID, the chunk log for its size, and a Bao outboard the Bao
// library verifies the object's blocks against. The controller filters by
// requested attribute; the backend always populates it.
func TestBlake3Attribute(t *testing.T) {
	b, _, _ := newRefTestBackend(t, 4096)
	data := bytes.Repeat([]byte("attribute "), 30_000) // 300 KB, 19 blocks of 16 KiB
	putObj(t, b, "attr", data)
	bucket, key := "bk", "attr"
	res, err := b.GetObjectAttributes(context.Background(), &s3.GetObjectAttributesInput{
		Bucket: &bucket, Key: &key, ObjectAttributes: []types.ObjectAttributes{"Blake3"},
	})
	require.NoError(t, err, "GetObjectAttributes")
	require.NotNil(t, res.Blake3, "no Blake3 attribute")
	root := blake3.Sum256(data)
	digest, _ := mh.Encode(root[:], mh.BLAKE3)
	want := cid.NewCidV1(cid.Raw, digest).String()
	require.Equal(t, want, res.Blake3.CID, "CID")
	require.Equal(t, blake3tree.ChunkLog(int64(len(data))), res.Blake3.ChunkLog, "ChunkLog")
	outboard, err := base64.StdEncoding.DecodeString(res.Blake3.Outboard)
	require.NoError(t, err, "Outboard is not base64")
	// What a client does: load root, chunk log and outboard into a Bao
	// library and verify the blocks it read.
	group := int(res.Blake3.ChunkLog)
	encoded, _ := bao.EncodeBuf(data, group, true)
	require.True(t, bytes.Equal(outboard, encoded), "outboard differs from the Bao library's encoding")
	block := blake3tree.BlockSize(res.Blake3.ChunkLog)
	for off := int64(0); off < int64(len(data)); off += block {
		require.True(t, bao.VerifyChunk(data[off:min(off+block, int64(len(data)))], outboard, group, uint64(off), root), "block at %d not verified", off)
	}
}

// TestBlake3AttributeGating checks that the attribute is built only when
// requested, and that GetObjectAttributes sets no x-cid header: the header
// belongs to GET and HEAD, and an attributes response must not change for
// a client that did not ask for Blake3.
func TestBlake3AttributeGating(t *testing.T) {
	b, _, _ := newRefTestBackend(t, 4096)
	putObj(t, b, "gate", bytes.Repeat([]byte("g"), 50_000))
	bucket, key := "bk", "gate"
	rc := newRequestCtx()
	res, err := b.GetObjectAttributes(rc, &s3.GetObjectAttributesInput{
		Bucket: &bucket, Key: &key, ObjectAttributes: []types.ObjectAttributes{types.ObjectAttributesEtag},
	})
	require.NoError(t, err)
	require.Nil(t, res.Blake3, "Blake3 built although only ETag was requested")
	require.NotNil(t, res.ETag, "ETag missing")
	got := rc.Response.Header.Peek(cidHeader)
	require.Empty(t, got, "GetObjectAttributes set %s = %q", cidHeader, got)
	res, err = b.GetObjectAttributes(context.Background(), &s3.GetObjectAttributesInput{Bucket: &bucket, Key: &key})
	require.NoError(t, err, "with no attribute list the attribute must be built")
	require.NotNil(t, res.Blake3, "with no attribute list the attribute must be built")
}

// newRequestCtx returns a request context as a server hands one to a handler:
// one whose Done works, which the registry's queries wait on.
func newRequestCtx() *fasthttp.RequestCtx {
	rc := &fasthttp.RequestCtx{}
	rc.Init(&fasthttp.Request{}, nil, nil)
	return rc
}
