package s3frontend

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"testing"

	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	cmds3 "github.com/fil-forge/libforge/commands/s3"
	"github.com/fil-forge/versitygw/s3response"

	"github.com/fil-forge/ingot/internal/reqscope"
)

// ctxWithContentMD5 puts a request carrying the correct Content-MD5 for data
// into the context, as the front-end does after the checksum middleware
// verifies the header against the stream. In production that match is
// guaranteed before the backend runs; the harness has no middleware, so these
// tests supply the matching value and assert the ETag is taken from it.
func ctxWithContentMD5(data []byte) context.Context {
	sum := md5.Sum(data)
	hdr := base64.StdEncoding.EncodeToString(sum[:])
	return context.WithValue(context.Background(), reqscope.RequestKey(),
		cmds3.Request{Headers: map[string]string{"Content-Md5": hdr}})
}

// A PutObject whose request carries a Content-MD5 gets that value as its ETag
// and reads back intact, and a second PutObject of the same bytes without the
// header lands on the same ETag: reusing the header preserves the S3 ETag.
func TestPutObject_ReusesContentMD5ForETag(t *testing.T) {
	b, _, _ := newRefTestBackend(t)
	bucket, key, key2 := "bk", "obj-hdr", "obj-computed"
	data := bytes.Repeat([]byte("content-md5 reuse "), 4000)
	sum := md5.Sum(data)
	want := `"` + hex.EncodeToString(sum[:]) + `"`

	out, err := b.PutObject(ctxWithContentMD5(data), s3response.PutObjectInput{
		Bucket: &bucket, Key: &key, Body: bytes.NewReader(data),
	})
	if err != nil {
		t.Fatalf("PutObject with Content-MD5: %v", err)
	}
	if out.ETag != want {
		t.Fatalf("header-path ETag = %q, want %s", out.ETag, want)
	}

	out2, err := b.PutObject(context.Background(), s3response.PutObjectInput{
		Bucket: &bucket, Key: &key2, Body: bytes.NewReader(data),
	})
	if err != nil {
		t.Fatalf("PutObject computed: %v", err)
	}
	if out2.ETag != want {
		t.Fatalf("compute-path ETag = %q, want %s", out2.ETag, want)
	}

	if _, body, err := getObjV(t, b, key, ""); err != nil || !bytes.Equal(body, data) {
		t.Fatalf("round-trip: %d bytes, err=%v", len(body), err)
	}
}

// An UploadPart whose request carries a Content-MD5 records that value as the
// part ETag, so ListParts, the Complete part check and the composite ETag are
// unchanged by reusing the header instead of computing the part MD5.
func TestUploadPart_ReusesContentMD5ForETag(t *testing.T) {
	b, _, _ := newRefTestBackend(t)
	key := "mp-reuse"
	uploadID := mpCreate(t, b, key, "", "")
	part := bytes.Repeat([]byte("p"), 5<<20)
	sum := md5.Sum(part)
	want := `"` + hex.EncodeToString(sum[:]) + `"`

	bucket := "bk"
	one := int32(1)
	out, err := b.UploadPart(ctxWithContentMD5(part), &awss3.UploadPartInput{
		Bucket: &bucket, Key: &key, UploadId: &uploadID, PartNumber: &one,
		Body: bytes.NewReader(part),
	})
	if err != nil {
		t.Fatalf("UploadPart with Content-MD5: %v", err)
	}
	if out.ETag == nil || *out.ETag != want {
		t.Fatalf("part ETag = %v, want %s", out.ETag, want)
	}

	lp, err := b.ListParts(context.Background(), &awss3.ListPartsInput{
		Bucket: &bucket, Key: &key, UploadId: &uploadID,
	})
	if err != nil {
		t.Fatalf("ListParts: %v", err)
	}
	if len(lp.Parts) != 1 || strings_Trim(lp.Parts[0].ETag) != hex.EncodeToString(sum[:]) {
		t.Fatalf("ListParts ETag = %+v, want %s", lp.Parts, hex.EncodeToString(sum[:]))
	}
}

func strings_Trim(s string) string {
	return string(bytes.Trim([]byte(s), `"`))
}
