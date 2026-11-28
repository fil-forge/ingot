package bucket

import (
	"bytes"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/require"
	cbg "github.com/whyrusleeping/cbor-gen"
)

func testCid(t *testing.T, s string) cid.Cid {
	t.Helper()
	pref := cid.V1Builder{Codec: cid.DagCBOR, MhType: 0x12}
	c, err := pref.Sum([]byte(s))
	require.NoError(t, err, "cid")
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
	require.NoError(t, LeafValue(leaf).MarshalCBOR(&buf), "marshal")
	require.Equal(t, byte(0xa1), buf.Bytes()[0], "header, want a single-entry map")

	var val ObjectValue
	require.NoError(t, val.UnmarshalCBOR(bytes.NewReader(buf.Bytes())), "unmarshal")
	require.Nil(t, val.Manifest, "value = {Manifest:%v Leaf:%v}", val.Manifest, val.Leaf)
	require.NotNil(t, val.Leaf, "value = {Manifest:%v Leaf:%v}", val.Manifest, val.Leaf)
	got := val.Leaf
	require.Equal(t, leaf.Current, got.Current, "leaf round-trip: Current")
	require.Equal(t, leaf.NullSeq, got.NullSeq, "leaf round-trip: NullSeq")
	require.NotNil(t, got.Prev, "leaf round-trip: Prev")
	require.True(t, got.Prev.Equals(prev), "leaf round-trip: Prev = %s, want %s", got.Prev, prev)
	require.NotNil(t, got.State, "leaf round-trip: State")
	require.True(t, got.State.Equals(state), "leaf round-trip: State = %s, want %s", got.State, state)

	var env EnvelopedLeaf
	require.NoError(t, env.UnmarshalCBOR(bytes.NewReader(buf.Bytes())), "enveloped unmarshal")
	require.Equal(t, leaf.Current, env.Leaf.Current, "EnvelopedLeaf round-trip")
}

// TestObjectValue_ManifestRoundTrip pins the other arm: a manifest written
// through ManifestValue/EnvelopedManifest round-trips, and the two strict
// single-arm decoders reject each other's blocks.
func TestObjectValue_ManifestRoundTrip(t *testing.T) {
	mf := &ObjectManifest{Key: "photos/cat.jpg", Seq: 7, VersionID: "null", ETag: "abc"}
	var buf bytes.Buffer
	require.NoError(t, (&EnvelopedManifest{Manifest: mf}).MarshalCBOR(&buf), "marshal")
	require.Equal(t, byte(0xa1), buf.Bytes()[0], "header, want a single-entry map")

	var val ObjectValue
	require.NoError(t, val.UnmarshalCBOR(bytes.NewReader(buf.Bytes())), "unmarshal")
	require.Nil(t, val.Leaf, "value = {Manifest:%v Leaf:%v}", val.Manifest, val.Leaf)
	require.NotNil(t, val.Manifest, "value = {Manifest:%v Leaf:%v}", val.Manifest, val.Leaf)
	require.Equal(t, mf.Key, val.Manifest.Key, "manifest round-trip")
	require.Equal(t, mf.Seq, val.Manifest.Seq, "manifest round-trip")
	require.Equal(t, mf.VersionID, val.Manifest.VersionID, "manifest round-trip")

	var em EnvelopedManifest
	require.NoError(t, em.UnmarshalCBOR(bytes.NewReader(buf.Bytes())), "enveloped manifest unmarshal")
	require.Equal(t, mf.Key, em.Manifest.Key, "EnvelopedManifest round-trip")

	// The strict single-arm decoders reject the other arm.
	var el EnvelopedLeaf
	require.Error(t, el.UnmarshalCBOR(bytes.NewReader(buf.Bytes())), "EnvelopedLeaf.UnmarshalCBOR(manifest block) = nil error, want rejection")
	var lbuf bytes.Buffer
	require.NoError(t, LeafValue(&ObjectLeaf{Current: VersionNode{Manifest: testCid(t, "m")}}).MarshalCBOR(&lbuf), "leaf marshal")
	require.Error(t, em.UnmarshalCBOR(bytes.NewReader(lbuf.Bytes())), "EnvelopedManifest.UnmarshalCBOR(leaf block) = nil error, want rejection")
}

// TestObjectValue_RejectsNonUnionBlocks pins §10's upgrade safety: a block
// carrying no known union key — a future format's key, or a pre-union bare
// manifest — is an error, never a zero-filled decode.
func TestObjectValue_RejectsNonUnionBlocks(t *testing.T) {
	encodeUnknown := func(key string) []byte {
		var buf bytes.Buffer
		cw := cbg.NewCborWriter(&buf)
		require.NoError(t, cw.WriteMajorTypeHeader(cbg.MajMap, 1), "header")
		require.NoError(t, cw.WriteMajorTypeHeader(cbg.MajTextString, uint64(len(key))), "key header")
		_, err := cw.WriteString(key)
		require.NoError(t, err, "key")
		require.NoError(t, (&ObjectManifest{Key: "k"}).MarshalCBOR(cw), "payload")
		return buf.Bytes()
	}

	var val ObjectValue
	err := val.UnmarshalCBOR(bytes.NewReader(encodeUnknown("/objectmanifest/2")))
	require.Error(t, err, "future union key: err = %v, want no-known-key rejection", err)
	require.Contains(t, err.Error(), "no known value-union key", "future union key: err = %v, want no-known-key rejection", err)

	// A pre-union bare manifest block is not a value block.
	var mbuf bytes.Buffer
	require.NoError(t, (&ObjectManifest{Key: "k"}).MarshalCBOR(&mbuf), "bare manifest marshal")
	require.Error(t, val.UnmarshalCBOR(bytes.NewReader(mbuf.Bytes())), "bare manifest block: want rejection")

	// Marshalling zero or two arms is refused.
	var empty ObjectValue
	require.Error(t, empty.MarshalCBOR(&bytes.Buffer{}), "marshal of zero-arm value: want rejection")
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
	require.NoError(t, (&EnvelopedVersionState{State: st}).MarshalCBOR(&buf), "marshal")
	require.Equal(t, byte(0xa1), buf.Bytes()[0], "header, want a single-entry map")

	var env EnvelopedVersionState
	require.NoError(t, env.UnmarshalCBOR(bytes.NewReader(buf.Bytes())), "unmarshal")
	got := env.State
	require.Equal(t, st.Retention, got.Retention, "round-trip")
	require.Equal(t, LegalHoldOff, got.LegalHold, "round-trip")
	require.Equal(t, "forge", got.Tags["team"], "round-trip")
	require.Equal(t, "dev", got.Tags["env"], "round-trip")
	require.Len(t, got.Tags, 2, "round-trip")
	require.False(t, got.Empty(), "populated state reports Empty")
	require.True(t, (&VersionState{}).Empty(), "zero state does not report Empty")

	// The strict decoders reject cross-type blocks: a value block is not a
	// state block, and a state block is not a value block.
	var lbuf bytes.Buffer
	require.NoError(t, LeafValue(&ObjectLeaf{Current: VersionNode{Manifest: testCid(t, "m")}}).MarshalCBOR(&lbuf), "leaf marshal")
	require.Error(t, env.UnmarshalCBOR(bytes.NewReader(lbuf.Bytes())), "EnvelopedVersionState.UnmarshalCBOR(leaf block) = nil error, want rejection")
	var val ObjectValue
	require.Error(t, val.UnmarshalCBOR(bytes.NewReader(buf.Bytes())), "ObjectValue.UnmarshalCBOR(state block) = nil error, want rejection")
}
