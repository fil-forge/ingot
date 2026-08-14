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

// TestObjectValue_LeafRoundTrip pins the §2.1 union: a leaf written through
// LeafValue/EnvelopedLeaf is a single-entry map keyed "/objectleaf/0" and
// decodes back with its fields intact.
func TestObjectValue_LeafRoundTrip(t *testing.T) {
	prev := testCid(t, "prev-root")
	state := testCid(t, "state-root")
	leaf := &ObjectLeaf{
		Current: VersionNode{Seq: 9, VersionID: "01ARZ3NDEKTSV4RRFFQ69G5FAV", Manifest: testCid(t, "mf")},
		Prev:    &prev,
		NullSeq: 4,
		State:   &state,
	}
	var buf bytes.Buffer
	if err := LeafValue(leaf).MarshalCBOR(&buf); err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if buf.Bytes()[0] != 0xa1 {
		t.Fatalf("header = %#x, want 0xa1 (single-entry map)", buf.Bytes()[0])
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
		got.Prev == nil || !got.Prev.Equals(prev) ||
		got.State == nil || !got.State.Equals(state) {
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

// TestObjectValue_ManifestRoundTrip pins the other arm: a manifest written
// through ManifestValue/EnvelopedManifest round-trips, and the two strict
// single-arm decoders reject each other's blocks.
func TestObjectValue_ManifestRoundTrip(t *testing.T) {
	mf := &ObjectManifest{Key: "photos/cat.jpg", Seq: 7, VersionID: "null", ETag: "abc"}
	var buf bytes.Buffer
	if err := (&EnvelopedManifest{Manifest: mf}).MarshalCBOR(&buf); err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if buf.Bytes()[0] != 0xa1 {
		t.Fatalf("header = %#x, want 0xa1 (single-entry map)", buf.Bytes()[0])
	}

	var val ObjectValue
	if err := val.UnmarshalCBOR(bytes.NewReader(buf.Bytes())); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if val.Leaf != nil || val.Manifest == nil {
		t.Fatalf("value = {Manifest:%v Leaf:%v}, want manifest", val.Manifest, val.Leaf)
	}
	if val.Manifest.Key != mf.Key || val.Manifest.Seq != mf.Seq || val.Manifest.VersionID != mf.VersionID {
		t.Fatalf("manifest round-trip = %+v, want %+v", val.Manifest, mf)
	}

	var em EnvelopedManifest
	if err := em.UnmarshalCBOR(bytes.NewReader(buf.Bytes())); err != nil {
		t.Fatalf("enveloped manifest unmarshal: %v", err)
	}
	if em.Manifest.Key != mf.Key {
		t.Fatalf("EnvelopedManifest round-trip = %+v, want %+v", em.Manifest, mf)
	}

	// The strict single-arm decoders reject the other arm.
	var el EnvelopedLeaf
	if err := el.UnmarshalCBOR(bytes.NewReader(buf.Bytes())); err == nil {
		t.Fatal("EnvelopedLeaf.UnmarshalCBOR(manifest block) = nil error, want rejection")
	}
	var lbuf bytes.Buffer
	if err := LeafValue(&ObjectLeaf{Current: VersionNode{Manifest: testCid(t, "m")}}).MarshalCBOR(&lbuf); err != nil {
		t.Fatalf("leaf marshal: %v", err)
	}
	if err := em.UnmarshalCBOR(bytes.NewReader(lbuf.Bytes())); err == nil {
		t.Fatal("EnvelopedManifest.UnmarshalCBOR(leaf block) = nil error, want rejection")
	}
}

// TestObjectValue_RejectsNonUnionBlocks pins §10's upgrade safety: a block
// carrying no known union key — a future format's key, or a pre-union bare
// manifest — is an error, never a zero-filled decode.
func TestObjectValue_RejectsNonUnionBlocks(t *testing.T) {
	encodeUnknown := func(key string) []byte {
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
		if err := (&ObjectManifest{Key: "k"}).MarshalCBOR(cw); err != nil {
			t.Fatalf("payload: %v", err)
		}
		return buf.Bytes()
	}

	var val ObjectValue
	err := val.UnmarshalCBOR(bytes.NewReader(encodeUnknown("/objectmanifest/1")))
	if err == nil || !strings.Contains(err.Error(), "no known value-union key") {
		t.Fatalf("future union key: err = %v, want no-known-key rejection", err)
	}

	// A pre-union bare manifest block is not a value block.
	var mbuf bytes.Buffer
	if err := (&ObjectManifest{Key: "k"}).MarshalCBOR(&mbuf); err != nil {
		t.Fatalf("bare manifest marshal: %v", err)
	}
	if err := val.UnmarshalCBOR(bytes.NewReader(mbuf.Bytes())); err == nil {
		t.Fatal("bare manifest block: err = nil, want rejection")
	}

	// Marshalling zero or two arms is refused.
	var empty ObjectValue
	if err := empty.MarshalCBOR(&bytes.Buffer{}); err == nil {
		t.Fatal("marshal of zero-arm value: err = nil, want rejection")
	}
}

// TestVersionState_RoundTrip pins the version-state block: the
// "/versionstate/0" union key, the tri-valued hold, the verbatim retention
// bytes, and the reserved Tags field surviving a decode/re-encode cycle (the
// §4.1 merge rule depends on unowned fields being carried).
func TestVersionState_RoundTrip(t *testing.T) {
	st := &VersionState{
		Retention: []byte(`{"Mode":"GOVERNANCE","RetainUntilDate":"2027-01-02T15:04:05Z"}`),
		LegalHold: LegalHoldOff,
		Tags:      map[string]string{"team": "forge", "env": "dev"},
	}
	var buf bytes.Buffer
	if err := (&EnvelopedVersionState{State: st}).MarshalCBOR(&buf); err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if buf.Bytes()[0] != 0xa1 {
		t.Fatalf("header = %#x, want 0xa1 (single-entry map)", buf.Bytes()[0])
	}

	var env EnvelopedVersionState
	if err := env.UnmarshalCBOR(bytes.NewReader(buf.Bytes())); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	got := env.State
	if !bytes.Equal(got.Retention, st.Retention) || got.LegalHold != LegalHoldOff ||
		got.Tags["team"] != "forge" || got.Tags["env"] != "dev" || len(got.Tags) != 2 {
		t.Fatalf("round-trip = %+v, want %+v", got, st)
	}
	if got.Empty() {
		t.Fatal("populated state reports Empty")
	}
	if !(&VersionState{}).Empty() {
		t.Fatal("zero state does not report Empty")
	}

	// The strict decoders reject cross-type blocks: a value block is not a
	// state block, and a state block is not a value block.
	var lbuf bytes.Buffer
	if err := LeafValue(&ObjectLeaf{Current: VersionNode{Manifest: testCid(t, "m")}}).MarshalCBOR(&lbuf); err != nil {
		t.Fatalf("leaf marshal: %v", err)
	}
	if err := env.UnmarshalCBOR(bytes.NewReader(lbuf.Bytes())); err == nil {
		t.Fatal("EnvelopedVersionState.UnmarshalCBOR(leaf block) = nil error, want rejection")
	}
	var val ObjectValue
	if err := val.UnmarshalCBOR(bytes.NewReader(buf.Bytes())); err == nil {
		t.Fatal("ObjectValue.UnmarshalCBOR(state block) = nil error, want rejection")
	}
}
