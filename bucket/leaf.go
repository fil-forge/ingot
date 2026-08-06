package bucket

import (
	"bytes"
	"fmt"
	"io"

	"github.com/ipfs/go-cid"
	cbg "github.com/whyrusleeping/cbor-gen"
)

// VersionNode identifies one object version: its per-bucket ordinal (Seq,
// ordering only — never exposed to clients), its S3 client handle
// (VersionID, "null" or a ULID token), and its manifest. See
// docs/s3-versioning.md §2.
type VersionNode struct {
	Seq       uint64  `cborgen:"s"`
	VersionID string  `cborgen:"v"`
	Manifest  cid.Cid `cborgen:"m"`
}

// ObjectLeaf is the per-key version group, stored under the LeafEnvelopeKey
// envelope (docs/s3-versioning.md §2.1). A key's top-MST value points at one
// once the key has been superseded; until then the value is the bare
// ObjectManifest.
type ObjectLeaf struct {
	// Current is the head version — what GET/HEAD/ListObjects resolve with
	// a single leaf read, no descent into Prev.
	Current VersionNode `cborgen:"c"`

	// Prev is the root of the per-key MST of noncurrent versions, keyed
	// newest-first by the fixed-width hex of the bitwise-inverted Seq
	// (value = the version's manifest CID), or nil when there are none.
	Prev *cid.Cid `cborgen:"p"`

	// NullSeq is the Seq of the null version when it is noncurrent (an
	// entry in Prev); 0 when the null version is current or absent. Needed
	// because Prev is keyed by Seq and the null version's id ("null") does
	// not encode its Seq.
	NullSeq uint64 `cborgen:"n"`
}

// LeafEnvelopeKey is the keyed-union discriminator under which ObjectLeaf
// blocks are serialized (docs/s3-versioning.md §2.1). A revised leaf format
// takes a new key ("/objectleaf/1"); readers reject keys they do not know
// instead of decoding garbage.
const LeafEnvelopeKey = "/objectleaf/0"

// maxEnvelopeKeyLen bounds a value block's envelope key; real discriminators
// are short, so anything longer is malformed rather than a huge alloc.
const maxEnvelopeKeyLen = 128

// EnvelopedLeaf serializes its Leaf under the LeafEnvelopeKey envelope:
// {"/objectleaf/0": <leaf>}. Leaves must be written through it — a bare
// ObjectLeaf block would decode as an ObjectManifest at read time.
type EnvelopedLeaf struct {
	Leaf *ObjectLeaf
}

func (e *EnvelopedLeaf) MarshalCBOR(w io.Writer) error {
	if e == nil || e.Leaf == nil {
		_, err := w.Write(cbg.CborNull)
		return err
	}
	cw := cbg.NewCborWriter(w)
	if err := cw.WriteMajorTypeHeader(cbg.MajMap, 1); err != nil {
		return err
	}
	if err := cw.WriteMajorTypeHeader(cbg.MajTextString, uint64(len(LeafEnvelopeKey))); err != nil {
		return err
	}
	if _, err := cw.WriteString(LeafEnvelopeKey); err != nil {
		return err
	}
	return e.Leaf.MarshalCBOR(cw)
}

func (e *EnvelopedLeaf) UnmarshalCBOR(r io.Reader) error {
	var v ObjectValue
	if err := v.UnmarshalCBOR(r); err != nil {
		return err
	}
	if v.Leaf == nil {
		return fmt.Errorf("bucket: value block is not an enveloped leaf")
	}
	e.Leaf = v.Leaf
	return nil
}

// ObjectValue decodes a top-MST value block, which is either a bare
// ObjectManifest (a single-version key) or an enveloped ObjectLeaf
// (docs/s3-versioning.md §2.1). Exactly one of Manifest/Leaf is non-nil
// after a successful decode.
//
// The discrimination is exact, not duck typing: a manifest always encodes as
// a multi-entry map, so only envelopes present as a single-entry map with a
// "/"-prefixed key — and an unknown envelope key is an error (a block
// written by a newer format), never a zero-filled garbage decode.
type ObjectValue struct {
	Manifest *ObjectManifest
	Leaf     *ObjectLeaf
}

func (v *ObjectValue) UnmarshalCBOR(r io.Reader) error {
	*v = ObjectValue{}
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	cr := cbg.NewCborReader(bytes.NewReader(data))
	maj, extra, err := cr.ReadHeader()
	if err != nil {
		return err
	}
	if maj == cbg.MajMap && extra == 1 {
		kmaj, klen, err := cr.ReadHeader()
		if err != nil {
			return err
		}
		if kmaj != cbg.MajTextString || klen > maxEnvelopeKeyLen {
			return fmt.Errorf("bucket: malformed value block: single-entry map without a short text key")
		}
		key := make([]byte, klen)
		if _, err := io.ReadFull(cr, key); err != nil {
			return err
		}
		switch {
		case string(key) == LeafEnvelopeKey:
			v.Leaf = new(ObjectLeaf)
			return v.Leaf.UnmarshalCBOR(cr)
		case len(key) > 0 && key[0] == '/':
			return fmt.Errorf("bucket: unknown value envelope %q (written by a newer format?)", key)
		default:
			return fmt.Errorf("bucket: malformed value block: unexpected single-entry map key %q", key)
		}
	}
	v.Manifest = new(ObjectManifest)
	return v.Manifest.UnmarshalCBOR(bytes.NewReader(data))
}
