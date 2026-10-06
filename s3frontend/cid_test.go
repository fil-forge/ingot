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
	if err != nil {
		t.Fatal(err)
	}
	want := cid.NewCidV1(cid.Raw, digest).String()

	bucket, key := "bk", "k"
	t.Run("head", func(t *testing.T) {
		rc := &fasthttp.RequestCtx{}
		if _, err := b.HeadObject(rc, &s3.HeadObjectInput{Bucket: &bucket, Key: &key}); err != nil {
			t.Fatalf("HeadObject: %v", err)
		}
		if got := string(rc.Response.Header.Peek(cidHeader)); got != want {
			t.Fatalf("%s = %q, want %q", cidHeader, got, want)
		}
	})
	t.Run("get", func(t *testing.T) {
		rc := &fasthttp.RequestCtx{}
		out, err := b.GetObject(rc, &s3.GetObjectInput{Bucket: &bucket, Key: &key})
		if err != nil {
			t.Fatalf("GetObject: %v", err)
		}
		got, err := io.ReadAll(out.Body)
		if err != nil || !bytes.Equal(got, data) {
			t.Fatalf("body mismatch (%v)", err)
		}
		if got := string(rc.Response.Header.Peek(cidHeader)); got != want {
			t.Fatalf("%s = %q, want %q", cidHeader, got, want)
		}
	})
	t.Run("ranged get", func(t *testing.T) {
		rc := &fasthttp.RequestCtx{}
		out, err := b.GetObject(rc, &s3.GetObjectInput{Bucket: &bucket, Key: &key, Range: aws.String("bytes=4000-4200")})
		if err != nil {
			t.Fatalf("GetObject: %v", err)
		}
		_, _ = io.ReadAll(out.Body)
		if got := string(rc.Response.Header.Peek(cidHeader)); got != want {
			t.Fatalf("%s = %q, want %q", cidHeader, got, want)
		}
	})
	t.Run("plain context", func(t *testing.T) {
		if _, err := b.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: &bucket, Key: &key}); err != nil {
			t.Fatalf("HeadObject: %v", err)
		}
	})
}

// TestBlake3Attribute checks GetObjectAttributes returns the Blake3 element:
// the object's CID, the group for its size, and a Bao outboard the Bao
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
	if err != nil {
		t.Fatalf("GetObjectAttributes: %v", err)
	}
	if res.Blake3 == nil {
		t.Fatal("no Blake3 attribute")
	}
	root := blake3.Sum256(data)
	digest, _ := mh.Encode(root[:], mh.BLAKE3)
	if want := cid.NewCidV1(cid.Raw, digest).String(); res.Blake3.CID != want {
		t.Fatalf("CID = %s, want %s", res.Blake3.CID, want)
	}
	if want := blake3tree.GroupLog(int64(len(data))); res.Blake3.Group != want {
		t.Fatalf("Group = %d, want %d", res.Blake3.Group, want)
	}
	outboard, err := base64.StdEncoding.DecodeString(res.Blake3.Outboard)
	if err != nil {
		t.Fatalf("Outboard is not base64: %v", err)
	}
	// What a client does: load root, block size and outboard into a Bao
	// library and verify the blocks it read.
	group := int(res.Blake3.Group) - 10
	if want, _ := bao.EncodeBuf(data, group, true); !bytes.Equal(outboard, want) {
		t.Fatalf("outboard differs from the Bao library's encoding")
	}
	block := blake3tree.GroupSize(res.Blake3.Group)
	for off := int64(0); off < int64(len(data)); off += block {
		if !bao.VerifyChunk(data[off:min(off+block, int64(len(data)))], outboard, group, uint64(off), root) {
			t.Fatalf("block at %d not verified", off)
		}
	}
}
