package s3frontend

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	hiltclient "github.com/fil-forge/hilt/pkg/client"
	usagecmds "github.com/fil-forge/libforge/commands/s3/usage"
	"github.com/fil-forge/versitygw/backend"
	"github.com/fil-forge/versitygw/s3response"

	"github.com/fil-forge/ingot/usage"
)

// nopUsageReporter is a usage reporter no test here reaches: these tests read
// the counts from the store.
type nopUsageReporter struct{}

func (nopUsageReporter) ReportUsage(context.Context, *usagecmds.ReportArguments, ...hiltclient.MethodOption) (*usagecmds.ReportOK, error) {
	return &usagecmds.ReportOK{}, nil
}

// TestGetObjectCountsEgressForTheBucketsSpace: the bytes GetObject sends are
// counted under the bucket's space, whether the read was whole, ranged or
// abandoned part way. They are the object's bytes as the client receives
// them, not the encrypted bytes read to produce them, and a copy, which sends
// nothing to a client, adds none.
func TestGetObjectCountsEgressForTheBucketsSpace(t *testing.T) {
	counter := usage.NewCounter()
	b, mem := newSweepBackend(t, func(d *Deps) { d.Usage = counter })
	svc, err := usage.New(usage.Config{Counter: counter, Store: mem.Postgres, Reporter: nopUsageReporter{}})
	if err != nil {
		t.Fatal(err)
	}
	ctx := t.Context()
	space := sweepSpace(t, mem)
	started := time.Now().UTC().Truncate(time.Hour)

	bucket, key := sweepBucket, "obj"
	body := bytes.Repeat([]byte{'a'}, 1000)
	if _, err := b.PutObject(ctx, s3response.PutObjectInput{Bucket: &bucket, Key: &key, Body: bytes.NewReader(body), ContentLength: byteLen(body)}); err != nil {
		t.Fatalf("PutObject: %v", err)
	}
	// The object is stored encrypted, so what is read from storage is more
	// than its 1000 bytes.
	rv, err := b.resolveVersion(ctx, bucket, key, "")
	if err != nil {
		t.Fatalf("resolveVersion: %v", err)
	}
	var stored int64
	for _, blob := range storedBlobs(t, b, rv) {
		stored += blobSize(t, b, blob.Digest)
	}
	if stored <= 1000 {
		t.Fatalf("the object's stored bytes are %d: this test needs an encrypted object, which stores more than its 1000", stored)
	}

	get := func(rng string, read int64) {
		t.Helper()
		out, err := b.GetObject(ctx, &s3.GetObjectInput{Bucket: &bucket, Key: &key, Range: backend.GetPtrFromString(rng)})
		if err != nil {
			t.Fatalf("GetObject %q: %v", rng, err)
		}
		if _, err := io.CopyN(io.Discard, out.Body, read); err != nil {
			t.Fatalf("read %q: %v", rng, err)
		}
		if err := out.Body.Close(); err != nil {
			t.Fatalf("close %q: %v", rng, err)
		}
	}
	get("", 1000)
	get("bytes=0-99", 100)
	get("bytes=500-500", 1)
	get("", 10) // the client went away after 10 bytes

	dst, src := "copy", bucket+"/"+key
	if _, err := b.CopyObject(ctx, s3response.CopyObjectInput{Bucket: &bucket, Key: &dst, CopySource: &src}); err != nil {
		t.Fatalf("CopyObject: %v", err)
	}

	if err := svc.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	due, err := mem.DueEgress(ctx, 10)
	if err != nil {
		t.Fatalf("DueEgress: %v", err)
	}
	// One count, unless the test ran across the top of an hour.
	var sent int64
	for _, c := range due {
		if c.Space != space {
			t.Fatalf("egress counted under %s, want the bucket's space %s", c.Space, space)
		}
		if c.Hour.Before(started) || c.Hour.After(time.Now()) {
			t.Fatalf("egress counted in hour %s, want the hour it was sent in", c.Hour)
		}
		sent += c.Bytes
	}
	if sent != 1111 {
		t.Fatalf("egress counted: %d bytes, want the 1111 the GETs read", sent)
	}
}

// TestGetObjectCountsNothingWithoutACounter: a backend built with no usage
// counter serves reads as before.
func TestGetObjectCountsNothingWithoutACounter(t *testing.T) {
	b, mem := newSweepBackend(t)
	ctx := t.Context()
	bucket, key := sweepBucket, "obj"
	body := bytes.Repeat([]byte{'a'}, 100)
	if _, err := b.PutObject(ctx, s3response.PutObjectInput{Bucket: &bucket, Key: &key, Body: bytes.NewReader(body), ContentLength: byteLen(body)}); err != nil {
		t.Fatalf("PutObject: %v", err)
	}
	out, err := b.GetObject(ctx, &s3.GetObjectInput{Bucket: &bucket, Key: &key})
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	got, err := io.ReadAll(out.Body)
	if err != nil || !bytes.Equal(got, body) {
		t.Fatalf("GetObject body: %d bytes, err %v", len(got), err)
	}
	_ = out.Body.Close()
	due, err := mem.DueEgress(ctx, 10)
	if err != nil {
		t.Fatalf("DueEgress: %v", err)
	}
	if len(due) != 0 {
		t.Fatalf("egress was counted with no counter: %v", due)
	}
}
