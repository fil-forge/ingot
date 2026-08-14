package bucket

import (
	"fmt"
	"io"

	"github.com/ipfs/go-cid"
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

// ObjectLeaf is the per-key version group, stored under the "/objectleaf/0"
// union key (docs/s3-versioning.md §2.1). A key's top-MST value points at one
// once the key has been superseded or has received version state
// (docs/s3-object-lock.md §4.1); until then the value is its manifest (under
// "/objectmanifest/0").
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

	// State is the root of the per-key version-state MST: revSeqKey(seq) →
	// the version's VersionState block CID (docs/s3-object-lock.md §4.1).
	// Nil when no version of this key carries explicit state, which is what
	// makes state-free keys free to read.
	State *cid.Cid `cborgen:"st"`
}

// LegalHold values for VersionState.LegalHold. S3 distinguishes never-set
// (a 400 from GetObjectLegalHold) from an explicit OFF (a 200), so the
// stored state is tri-valued.
const (
	LegalHoldUnset uint8 = 0
	LegalHoldOff   uint8 = 1
	LegalHoldOn    uint8 = 2
)

// VersionState is one version's mutable service state, stored as its own
// catalog block under the "/versionstate/0" union key (docs/s3-object-lock.md
// §4.1). Object lock is its first tenant; the block is the home for
// per-version state that mutates after the version is created. Mutations
// merge: each operation replaces the fields it owns and carries every other
// field verbatim; a mutation that leaves every field absent deletes the
// version's state-tree entry.
type VersionState struct {
	// Retention is the stored retention document (docs/s3-object-lock.md §2),
	// verbatim; nil when retention has never been set.
	Retention []byte `cborgen:"r"`
	// LegalHold is tri-valued: LegalHoldUnset, LegalHoldOff, LegalHoldOn.
	LegalHold uint8 `cborgen:"h"`
	// Tags is reserved for object tagging (planned follow-up). Declared now
	// so every rewrite round-trips it under the merge rule; the tagging
	// feature adds handlers, not format. Nil until it lands.
	Tags map[string]string `cborgen:"t"`
}

// Empty reports whether every field is absent — the elision condition: an
// empty state block is removed from the tree rather than stored.
func (s *VersionState) Empty() bool {
	return s.Retention == nil && s.LegalHold == LegalHoldUnset && len(s.Tags) == 0
}

// ValueUnion is the keyed union every catalog value block is encoded as
// (docs/s3-versioning.md §2.1): a single-entry map whose key names the
// payload's format. cbor-gen generates its codec (the omitempty arms encode
// only the one that is set), but the generated decoder skips unknown keys
// silently — zero arms set — so raw ValueUnion use is a bug: decode through
// ObjectValue (either form), EnvelopedManifest, or EnvelopedLeaf, which turn
// that into the loud failure the format requires. A format revision adds an
// arm under a new key ("/objectmanifest/1") and old blocks keep decoding.
type ValueUnion struct {
	Manifest *ObjectManifest `cborgen:"/objectmanifest/0,omitempty"`
	Leaf     *ObjectLeaf     `cborgen:"/objectleaf/0,omitempty"`
}

// arms counts the set arms; a well-formed block has exactly one.
func (u *ValueUnion) arms() int {
	n := 0
	if u.Manifest != nil {
		n++
	}
	if u.Leaf != nil {
		n++
	}
	return n
}

// ObjectValue decodes a top-MST value block, which is either a manifest (a
// single-version key) or a leaf (docs/s3-versioning.md §2.1). Exactly one of
// Manifest/Leaf is non-nil after a successful decode; a block carrying no
// known union key — a newer format, or not a value block at all — is an
// error, never a zero-filled decode.
type ObjectValue struct {
	ValueUnion
}

// ManifestValue wraps a manifest for storage as a value block.
func ManifestValue(mf *ObjectManifest) *ObjectValue {
	return &ObjectValue{ValueUnion{Manifest: mf}}
}

// LeafValue wraps a leaf for storage as a value block.
func LeafValue(l *ObjectLeaf) *ObjectValue {
	return &ObjectValue{ValueUnion{Leaf: l}}
}

func (v *ObjectValue) MarshalCBOR(w io.Writer) error {
	if v.arms() != 1 {
		return fmt.Errorf("bucket: value block must carry exactly one union arm")
	}
	return v.ValueUnion.MarshalCBOR(w)
}

func (v *ObjectValue) UnmarshalCBOR(r io.Reader) error {
	if err := v.ValueUnion.UnmarshalCBOR(r); err != nil {
		return err
	}
	if v.arms() != 1 {
		return fmt.Errorf("bucket: block carries no known value-union key (written by a newer format?)")
	}
	return nil
}

// EnvelopedManifest reads/writes one manifest block under its
// "/objectmanifest/0" union key — the form every manifest is stored in
// (§2.3), as a value block and as a prev-tree entry alike. Decoding a leaf
// block through it is an error.
type EnvelopedManifest struct {
	Manifest *ObjectManifest
}

func (e *EnvelopedManifest) MarshalCBOR(w io.Writer) error {
	return ManifestValue(e.Manifest).MarshalCBOR(w)
}

func (e *EnvelopedManifest) UnmarshalCBOR(r io.Reader) error {
	var v ObjectValue
	if err := v.UnmarshalCBOR(r); err != nil {
		return err
	}
	if v.Manifest == nil {
		return fmt.Errorf("bucket: block is not an enveloped manifest")
	}
	e.Manifest = v.Manifest
	return nil
}

// EnvelopedLeaf reads/writes one leaf block under its "/objectleaf/0" union
// key. Decoding a manifest block through it is an error.
type EnvelopedLeaf struct {
	Leaf *ObjectLeaf
}

func (e *EnvelopedLeaf) MarshalCBOR(w io.Writer) error {
	return LeafValue(e.Leaf).MarshalCBOR(w)
}

func (e *EnvelopedLeaf) UnmarshalCBOR(r io.Reader) error {
	var v ObjectValue
	if err := v.UnmarshalCBOR(r); err != nil {
		return err
	}
	if v.Leaf == nil {
		return fmt.Errorf("bucket: block is not an enveloped leaf")
	}
	e.Leaf = v.Leaf
	return nil
}

// StateUnion is the keyed union for version-state blocks — the values of a
// leaf's state tree (docs/s3-object-lock.md §4.1). A separate union from
// ValueUnion so a state block decoded through ObjectValue (or vice versa)
// fails loudly with no arm matched. cbor-gen generates its codec; decode
// through EnvelopedVersionState, which requires the arm.
type StateUnion struct {
	State *VersionState `cborgen:"/versionstate/0,omitempty"`
}

// EnvelopedVersionState reads/writes one version-state block under its
// "/versionstate/0" union key.
type EnvelopedVersionState struct {
	State *VersionState
}

func (e *EnvelopedVersionState) MarshalCBOR(w io.Writer) error {
	if e.State == nil {
		return fmt.Errorf("bucket: version-state block must carry a state")
	}
	u := StateUnion{State: e.State}
	return u.MarshalCBOR(w)
}

func (e *EnvelopedVersionState) UnmarshalCBOR(r io.Reader) error {
	var u StateUnion
	if err := u.UnmarshalCBOR(r); err != nil {
		return err
	}
	if u.State == nil {
		return fmt.Errorf("bucket: block is not an enveloped version state")
	}
	e.State = u.State
	return nil
}
