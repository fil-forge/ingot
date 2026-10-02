package s3frontend

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"testing"

	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
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
		Bucket: &bucket, Key: &key, Body: bytes.NewReader(data), ContentLength: sizeOf(data),
	})
	if err != nil {
		t.Fatalf("PutObject with Content-MD5: %v", err)
	}
	if out.ETag != want {
		t.Fatalf("header-path ETag = %q, want %s", out.ETag, want)
	}

	out2, err := b.PutObject(context.Background(), s3response.PutObjectInput{
		Bucket: &bucket, Key: &key2, Body: bytes.NewReader(data), ContentLength: sizeOf(data),
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
		Body: bytes.NewReader(part), ContentLength: sizeOf(part),
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

// A PutObject naming an MD5 checksum, either an explicit x-amz-checksum-md5
// value or x-amz-checksum-algorithm: MD5 alone, takes its ETag from the
// checksum reader's digest and echoes the checksum, so neither form pays a
// second MD5 pass.
func TestPutObject_MD5ChecksumSpecSuppliesETag(t *testing.T) {
	b, _, _ := newRefTestBackend(t)
	bucket := "bk"
	data := bytes.Repeat([]byte("md5 checksum spec "), 4000)
	sum := md5.Sum(data)
	wantETag := `"` + hex.EncodeToString(sum[:]) + `"`
	wantB64 := base64.StdEncoding.EncodeToString(sum[:])

	for name, mod := range map[string]func(*s3response.PutObjectInput){
		"explicit value": func(in *s3response.PutObjectInput) { in.ChecksumMD5 = &wantB64 },
		"algorithm only": func(in *s3response.PutObjectInput) { in.ChecksumAlgorithm = types.ChecksumAlgorithmMd5 },
	} {
		t.Run(name, func(t *testing.T) {
			key := "obj-" + name
			in := s3response.PutObjectInput{Bucket: &bucket, Key: &key, Body: bytes.NewReader(data), ContentLength: sizeOf(data)}
			mod(&in)
			out, err := b.PutObject(context.Background(), in)
			if err != nil {
				t.Fatalf("PutObject: %v", err)
			}
			if out.ETag != wantETag {
				t.Fatalf("ETag = %q, want %s", out.ETag, wantETag)
			}
			if out.ChecksumMD5 == nil || *out.ChecksumMD5 != wantB64 {
				t.Fatalf("ChecksumMD5 echo = %v, want %s", out.ChecksumMD5, wantB64)
			}
			if _, body, err := getObjV(t, b, key, ""); err != nil || !bytes.Equal(body, data) {
				t.Fatalf("round-trip: %d bytes, err=%v", len(body), err)
			}
		})
	}
}

// An UploadPart whose checksum reader already computes MD5, on a session
// that declared MD5 or as an MD5 part checksum on a session that declared
// nothing, takes its part ETag from that reader.
func TestUploadPart_MD5ChecksumReaderSuppliesETag(t *testing.T) {
	b, _, _ := newRefTestBackend(t)
	part := bytes.Repeat([]byte("q"), 5<<20)
	sum := md5.Sum(part)
	want := `"` + hex.EncodeToString(sum[:]) + `"`
	wantB64 := base64.StdEncoding.EncodeToString(sum[:])

	t.Run("MD5 session", func(t *testing.T) {
		key := "mp-md5-session"
		uploadID := mpCreate(t, b, key, types.ChecksumAlgorithmMd5, types.ChecksumTypeFullObject)
		out, err := mpUploadPart(t, b, key, uploadID, 1, part, nil)
		if err != nil {
			t.Fatalf("UploadPart: %v", err)
		}
		if out.ETag == nil || *out.ETag != want {
			t.Fatalf("part ETag = %v, want %s", out.ETag, want)
		}
		if out.ChecksumMD5 == nil || *out.ChecksumMD5 != wantB64 {
			t.Fatalf("ChecksumMD5 echo = %v, want %s", out.ChecksumMD5, wantB64)
		}
	})
	t.Run("MD5 part checksum, undeclared session", func(t *testing.T) {
		key := "mp-md5-part"
		uploadID := mpCreate(t, b, key, "", "")
		out, err := mpUploadPart(t, b, key, uploadID, 1, part, func(in *awss3.UploadPartInput) {
			in.ChecksumAlgorithm = types.ChecksumAlgorithmMd5
		})
		if err != nil {
			t.Fatalf("UploadPart: %v", err)
		}
		if out.ETag == nil || *out.ETag != want {
			t.Fatalf("part ETag = %v, want %s", out.ETag, want)
		}
		if out.ChecksumMD5 == nil || *out.ChecksumMD5 != wantB64 {
			t.Fatalf("ChecksumMD5 echo = %v, want %s", out.ChecksumMD5, wantB64)
		}
	})
}

// An UploadPartCopy whose explicit range covers the whole single-part source
// is a whole-object copy: the part ETag is the source's MD5, as with no
// range header.
func TestUploadPartCopy_ExplicitFullRangeIsWholeObject(t *testing.T) {
	b, _, _ := newRefTestBackend(t)
	src := bytes.Repeat([]byte("whole-range "), 6<<20/12)
	putObjV(t, b, "src", src)
	sum := md5.Sum(src)
	want := `"` + hex.EncodeToString(sum[:]) + `"`

	id := mpCreate(t, b, "full-range", "", "")
	rng := fmt.Sprintf("bytes=0-%d", len(src)-1)
	res, err := upc(t, b, "full-range", id, 1, "bk/src", func(in *awss3.UploadPartCopyInput) { in.CopySourceRange = &rng })
	if err != nil {
		t.Fatalf("UploadPartCopy full range: %v", err)
	}
	if res.ETag == nil || *res.ETag != want {
		t.Fatalf("part ETag = %v, want %s", res.ETag, want)
	}
	if _, err := mpComplete(t, b, "full-range", id, []types.CompletedPart{completed(res, 1)}, nil); err != nil {
		t.Fatalf("complete: %v", err)
	}
	if _, got, err := getObjV(t, b, "full-range", ""); err != nil || !bytes.Equal(got, src) {
		t.Fatalf("GET after full-range copy: %d bytes, %v; want the source", len(got), err)
	}
}

// A zero-byte PutObject carries the constant empty MD5 as its ETag.
func TestPutObject_ZeroByteETag(t *testing.T) {
	b, _, _ := newRefTestBackend(t)
	bucket, key := "bk", "empty"
	out, err := b.PutObject(context.Background(), s3response.PutObjectInput{Bucket: &bucket, Key: &key, Body: bytes.NewReader(nil), ContentLength: sizeOf(nil)})
	if err != nil {
		t.Fatalf("PutObject: %v", err)
	}
	if want := `"d41d8cd98f00b204e9800998ecf8427e"`; out.ETag != want {
		t.Fatalf("ETag = %q, want %s", out.ETag, want)
	}
}
