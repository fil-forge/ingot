package bucket

import (
	"bytes"
	"strings"
	"testing"

	"github.com/ipfs/go-cid"
	cbg "github.com/whyrusleeping/cbor-gen"
)

func testCid(t *testing.T, s string) cid.Cid {
	t.Helper()
	pref := cid.V1Builder{Codec: cid.DagCBOR, MhType: 0x12}
	c, err := pref.Sum([]byte(s))
	if err != nil {
		t.Fatalf("cid: %v", err)
	}
	return c
}

// TestObjectValue_LeafRoundTrip pins the §2.1 envelope: a leaf written through
// EnvelopedLeaf decodes back as ObjectValue.Leaf with its fields intact.
func TestObjectValue_LeafRoundTrip(t *testing.T) {
	prev := testCid(t, "prev-root")
	leaf := &ObjectLeaf{
		Current: VersionNode{Seq: 9, VersionID: "01ARZ3NDEKTSV4RRFFQ69G5FAV", Manifest: testCid(t, "mf")},
		Prev:    &prev,
		NullSeq: 4,
	}
	var buf bytes.Buffer
	if err := (&EnvelopedLeaf{Leaf: leaf}).MarshalCBOR(&buf); err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var val ObjectValue
	if err := val.UnmarshalCBOR(bytes.NewReader(buf.Bytes())); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if val.Manifest != nil || val.Leaf == nil {
		t.Fatalf("value = {Manifest:%v Leaf:%v}, want leaf", val.Manifest, val.Leaf)
	}
	got := val.Leaf
	if got.Current != leaf.Current || got.NullSeq != leaf.NullSeq ||
		got.Prev == nil || !got.Prev.Equals(prev) {
		t.Fatalf("leaf round-trip = %+v, want %+v", got, leaf)
	}

	var env EnvelopedLeaf
	if err := env.UnmarshalCBOR(bytes.NewReader(buf.Bytes())); err != nil {
		t.Fatalf("enveloped unmarshal: %v", err)
	}
	if env.Leaf.Current != leaf.Current {
		t.Fatalf("EnvelopedLeaf round-trip = %+v, want %+v", env.Leaf, leaf)
	}
}

// TestObjectValue_BareManifest pins the other half of the union: a manifest
// block decodes as ObjectValue.Manifest, never as a leaf.
func TestObjectValue_BareManifest(t *testing.T) {
	mf := &ObjectManifest{Key: "photos/cat.jpg", Seq: 7, VersionID: "null", ETag: "abc"}
	var buf bytes.Buffer
	if err := mf.MarshalCBOR(&buf); err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var val ObjectValue
	if err := val.UnmarshalCBOR(bytes.NewReader(buf.Bytes())); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if val.Leaf != nil || val.Manifest == nil {
		t.Fatalf("value = {Manifest:%v Leaf:%v}, want bare manifest", val.Manifest, val.Leaf)
	}
	if val.Manifest.Key != mf.Key || val.Manifest.Seq != mf.Seq || val.Manifest.VersionID != mf.VersionID {
		t.Fatalf("manifest round-trip = %+v, want %+v", val.Manifest, mf)
	}

	// A manifest is not an enveloped leaf.
	var env EnvelopedLeaf
	if err := env.UnmarshalCBOR(bytes.NewReader(buf.Bytes())); err == nil {
		t.Fatal("EnvelopedLeaf.UnmarshalCBOR(manifest) = nil error, want rejection")
	}
}

// TestObjectValue_UnknownEnvelope pins §10's upgrade safety: an envelope key
// the reader does not know is an error, never a garbage decode.
func TestObjectValue_UnknownEnvelope(t *testing.T) {
	encodeEnvelope := func(key string) []byte {
		var buf bytes.Buffer
		cw := cbg.NewCborWriter(&buf)
		if err := cw.WriteMajorTypeHeader(cbg.MajMap, 1); err != nil {
			t.Fatalf("header: %v", err)
		}
		if err := cw.WriteMajorTypeHeader(cbg.MajTextString, uint64(len(key))); err != nil {
			t.Fatalf("key header: %v", err)
		}
		if _, err := cw.WriteString(key); err != nil {
			t.Fatalf("key: %v", err)
		}
		if err := (&ObjectLeaf{Current: VersionNode{Manifest: testCid(t, "payload")}}).MarshalCBOR(cw); err != nil {
			t.Fatalf("payload: %v", err)
		}
		return buf.Bytes()
	}

	var val ObjectValue
	err := val.UnmarshalCBOR(bytes.NewReader(encodeEnvelope("/objectleaf/1")))
	if err == nil || !strings.Contains(err.Error(), "unknown value envelope") {
		t.Fatalf("future envelope: err = %v, want unknown-envelope rejection", err)
	}

	// A single-entry map that is not an envelope is malformed, not a manifest.
	err = val.UnmarshalCBOR(bytes.NewReader(encodeEnvelope("oops")))
	if err == nil {
		t.Fatal("non-envelope single-entry map: err = nil, want rejection")
	}
}
