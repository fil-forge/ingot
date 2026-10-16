package bucket

import (
	"context"
	"fmt"
	"io"
	"sort"

	"github.com/fil-forge/ucantone/did"
	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"

	"github.com/fil-forge/ingot/blockstore"
)

// BlobList is a stored body's blobs in byte order: the first entries inline,
// the rest in extent blocks (see BlobExtent) that ExtentRefs point to. A
// ranged read loads the manifest and each extent its range covers, once.
// Read it through Body.Index; write it with BuildBlobList.
//
// A blob ends one byte before the next blob's Offset, or before the next
// extent's Offset, or at the body's Size-1, so no entry stores an end.
type BlobList struct {
	// Head holds the first blobs, starting at offset 0. Empty only for a
	// zero-byte body.
	Head []BlobEntry `cborgen:"h"`
	// Extents points to the blobs after Head, in offset order. Empty when
	// Head holds them all.
	Extents []ExtentRef `cborgen:"x"`
}

// BlobEntry names one body blob, tuple-encoded as [digest, offset].
type BlobEntry struct {
	// Digest is the sha256 multihash Piri stores the blob under.
	Digest mh.Multihash
	// Offset is the absolute plaintext offset of the blob's first byte.
	Offset int64
}

// ExtentRef points to one extent block, tuple-encoded as [offset, link].
type ExtentRef struct {
	// Offset equals the Offset of the extent's first entry.
	Offset int64
	// Link is the extent block's CID.
	Link cid.Cid
}

// BlobExtent is one block of a body's blobs after its Head, stored as a
// catalog block under the "/blobextent/0" union key and encoded as a bare
// array of entries.
type BlobExtent struct {
	Entries []BlobEntry `cborgen:"transparent"`
}

// BlobExtentUnion is the keyed union for extent blocks. A separate union
// from ValueUnion, as StateUnion is, so an extent decoded as a value block
// (or a value block decoded as an extent) fails with no arm matched. Decode
// through EnvelopedBlobExtent, which requires the arm.
type BlobExtentUnion struct {
	Extent *BlobExtent `cborgen:"/blobextent/0,omitempty"`
}

// EnvelopedBlobExtent reads/writes one extent block under its
// "/blobextent/0" union key.
type EnvelopedBlobExtent struct {
	Extent *BlobExtent
}

func (e *EnvelopedBlobExtent) MarshalCBOR(w io.Writer) error {
	if e.Extent == nil {
		return fmt.Errorf("bucket: extent block must carry an extent")
	}
	u := BlobExtentUnion{Extent: e.Extent}
	return u.MarshalCBOR(w)
}

func (e *EnvelopedBlobExtent) UnmarshalCBOR(r io.Reader) error {
	var u BlobExtentUnion
	if err := u.UnmarshalCBOR(r); err != nil {
		return err
	}
	if u.Extent == nil {
		return fmt.Errorf("bucket: block is not an enveloped blob extent")
	}
	e.Extent = u.Extent
	return nil
}

// headEntries and extentEntries are how many blobs BuildBlobList puts in a
// body's Head and in each extent. Readers accept any split that validates;
// tests shrink these to cross extent boundaries with small bodies.
var (
	headEntries   = 1024
	extentEntries = 1024
)

// maxExtentRefs is how many extents a body can point to: cbor-gen's default
// cap on a slice, which BlobList.Extents keeps.
const maxExtentRefs = 8192

// MaxBodyBlobs is the most blobs one body can hold: a full Head and every
// extent full. At the default blob size that is far past S3's largest
// object; it binds only a deployment whose max_blob_size is under about
// 6.1 MiB.
const MaxBodyBlobs = 1024 + maxExtentRefs*1024

// blobCapacity is MaxBodyBlobs at the current writer constants.
func blobCapacity() int { return headEntries + maxExtentRefs*extentEntries }

// BlobRef is one blob of a body in memory: its digest and the inclusive
// plaintext span [Start, End] it covers within the object (HTTP Range
// semantics, like every range in the read path). Readers get one per blob
// from a BlobIndex; writers build a body's list as BlobRefs.
//
// It is also the element of a /0 manifest's blob list, which stored spans.
type BlobRef struct {
	Digest mh.Multihash `cborgen:"d"`
	Start  int64        `cborgen:"s"`
	End    int64        `cborgen:"e"`
}

// Len returns the number of plaintext bytes the blob covers.
func (b BlobRef) Len() int64 { return b.End - b.Start + 1 }

// BlobIndex reads a body's blobs. A stored body's index loads extents on
// demand; it is for one request and not safe for concurrent use.
type BlobIndex interface {
	// Size is the body's byte count.
	Size() int64
	// Locate returns the blob covering off, loading at most one extent.
	Locate(ctx context.Context, off int64) (BlobRef, error)
	// Extent returns every blob stored in the same block as the one
	// covering off: the manifest's Head, or one extent. A BlobRefs list is
	// one extent.
	Extent(ctx context.Context, off int64) ([]BlobRef, error)
	// Digests returns every blob's digest in order, loading every extent.
	Digests(ctx context.Context) ([]mh.Multihash, error)
}

// BlobRefs is a body's blob list while the body is being written: contiguous
// from offset 0, in order. Append keeps it that way. It is a BlobIndex, so
// the write path reads a body it has not committed the same way the read
// path reads a stored one.
type BlobRefs []BlobRef

// Append adds the body's next blob, length plaintext bytes starting where
// the list ends.
func (r *BlobRefs) Append(digest mh.Multihash, length int64) {
	start := r.Size()
	*r = append(*r, BlobRef{Digest: digest, Start: start, End: start + length - 1})
}

// Size is the byte count the list covers.
func (r BlobRefs) Size() int64 {
	if len(r) == 0 {
		return 0
	}
	return r[len(r)-1].End + 1
}

// Locate returns the blob covering off.
func (r BlobRefs) Locate(_ context.Context, off int64) (BlobRef, error) {
	i := sort.Search(len(r), func(i int) bool { return r[i].End >= off })
	if off < 0 || i == len(r) || r[i].Start > off {
		return BlobRef{}, fmt.Errorf("bucket: no blob covers offset %d of a %d-byte body", off, r.Size())
	}
	return r[i], nil
}

// Extent returns the whole list.
func (r BlobRefs) Extent(context.Context, int64) ([]BlobRef, error) { return r, nil }

// Digests returns every blob's digest in order.
func (r BlobRefs) Digests(context.Context) ([]mh.Multihash, error) { return r.DigestList(), nil }

// DigestList is Digests without the context and error a stored list needs.
func (r BlobRefs) DigestList() []mh.Multihash {
	out := make([]mh.Multihash, len(r))
	for i, b := range r {
		out[i] = b.Digest
	}
	return out
}

// ReadBlobRefs reads every blob of idx into a BlobRefs list, one extent at a
// time: what a write that reuses a stored body (a copy) hands its commit, so
// the destination writes extents of its own.
func ReadBlobRefs(ctx context.Context, idx BlobIndex) (BlobRefs, error) {
	var out BlobRefs
	for off := int64(0); off < idx.Size(); off = out.Size() {
		ext, err := idx.Extent(ctx, off)
		if err != nil {
			return nil, err
		}
		out = append(out, ext...)
	}
	return out, nil
}

// BuildBlobList cuts refs into a Head and extents, writing each extent
// through w, and returns the list for the manifest. It runs inside the
// commit's transaction, as the leaf's version and state trees are built, so
// the extents are logged and shipped with the manifest that names them.
// Identical refs give identical CIDs.
func BuildBlobList(ctx context.Context, w blockstore.Writer, refs BlobRefs) (BlobList, error) {
	if len(refs) > blobCapacity() {
		return BlobList{}, fmt.Errorf("bucket: a body holds at most %d blobs, this one has %d", blobCapacity(), len(refs))
	}
	var next int64
	for i, b := range refs {
		if len(b.Digest) == 0 {
			return BlobList{}, fmt.Errorf("bucket: blob %d has no digest", i)
		}
		if b.Start != next || b.End < b.Start {
			return BlobList{}, fmt.Errorf("bucket: blob %d spans [%d,%d], want a non-empty span from %d", i, b.Start, b.End, next)
		}
		next = b.End + 1
	}
	head := min(len(refs), headEntries)
	list := BlobList{Head: blobEntries(refs[:head])}
	for i := head; i < len(refs); i += extentEntries {
		chunk := refs[i:min(i+extentEntries, len(refs))]
		c, err := w.Put(ctx, &EnvelopedBlobExtent{Extent: &BlobExtent{Entries: blobEntries(chunk)}})
		if err != nil {
			return BlobList{}, fmt.Errorf("bucket: put blob extent: %w", err)
		}
		list.Extents = append(list.Extents, ExtentRef{Offset: chunk[0].Start, Link: c})
	}
	return list, nil
}

func blobEntries(refs []BlobRef) []BlobEntry {
	out := make([]BlobEntry, len(refs))
	for i, b := range refs {
		out[i] = BlobEntry{Digest: b.Digest, Offset: b.Start}
	}
	return out
}

// Index reads the body's stored blob list, fetching extents through store
// under space (the bucket's read store, or the commit's transaction).
func (b *Body) Index(store blockstore.Reader, space did.DID) BlobIndex {
	return &storedIndex{store: store, space: space, list: b.Blobs, size: b.Size, loaded: -1}
}

// storedIndex is the BlobIndex over a stored BlobList. Extent e (0 is Head,
// e is Extents[e-1]) covers [extentStart(e), extentStart(e+1)). The last
// extent it loaded stays cached, so a sequential read loads each extent once.
type storedIndex struct {
	store blockstore.Reader
	space did.DID
	list  BlobList
	size  int64

	checked bool
	err     error // the list's shape, checked on first use
	loaded  int   // the extent refs holds; -1 for none
	refs    []BlobRef
}

func (x *storedIndex) Size() int64 { return x.size }

// check validates what can be checked without loading an extent.
func (x *storedIndex) check() error {
	if x.checked {
		return x.err
	}
	x.checked = true
	l := x.list
	switch {
	case x.size < 0:
		x.err = fmt.Errorf("bucket: body size %d is negative", x.size)
	case x.size == 0:
		if len(l.Head) > 0 || len(l.Extents) > 0 {
			x.err = fmt.Errorf("bucket: a zero-byte body lists blobs")
		}
	case len(l.Head) == 0:
		x.err = fmt.Errorf("bucket: a %d-byte body lists no blobs", x.size)
	default:
		prev := l.Head[len(l.Head)-1].Offset
		for i, e := range l.Extents {
			if e.Offset <= prev || e.Offset >= x.size {
				x.err = fmt.Errorf("bucket: extent %d starts at %d, want after %d and before %d", i, e.Offset, prev, x.size)
				break
			}
			prev = e.Offset
		}
	}
	return x.err
}

// extentStart is where extent e starts; extentStart(len(Extents)+1) is Size.
func (x *storedIndex) extentStart(e int) int64 {
	switch {
	case e == 0:
		return 0
	case e > len(x.list.Extents):
		return x.size
	default:
		return x.list.Extents[e-1].Offset
	}
}

// extentFor is the extent covering off.
func (x *storedIndex) extentFor(off int64) int {
	return sort.Search(len(x.list.Extents), func(i int) bool { return x.list.Extents[i].Offset > off })
}

// load returns extent e's blobs, fetching the block unless it is cached.
func (x *storedIndex) load(ctx context.Context, e int) ([]BlobRef, error) {
	if e == x.loaded {
		return x.refs, nil
	}
	entries := x.list.Head
	if e > 0 {
		ref := x.list.Extents[e-1]
		var env EnvelopedBlobExtent
		if err := x.store.Get(ctx, x.space, ref.Link, &env); err != nil {
			return nil, fmt.Errorf("bucket: load blob extent %d (%s): %w", e-1, ref.Link, err)
		}
		entries = env.Extent.Entries
	}
	refs, err := extentRefs(entries, x.extentStart(e), x.extentStart(e+1))
	if err != nil {
		return nil, fmt.Errorf("bucket: blob extent %d: %w", e-1, err)
	}
	x.loaded, x.refs = e, refs
	return refs, nil
}

// extentRefs turns one extent's entries, covering [start, end), into spans,
// checking that they start at start, increase, and end before end.
func extentRefs(entries []BlobEntry, start, end int64) ([]BlobRef, error) {
	if len(entries) == 0 {
		return nil, fmt.Errorf("no entries")
	}
	if entries[0].Offset != start {
		return nil, fmt.Errorf("first blob at %d, want %d", entries[0].Offset, start)
	}
	refs := make([]BlobRef, len(entries))
	for i, e := range entries {
		next := end
		if i+1 < len(entries) {
			next = entries[i+1].Offset
		}
		if next <= e.Offset {
			return nil, fmt.Errorf("blob %d at %d ends before it starts (next at %d)", i, e.Offset, next)
		}
		if len(e.Digest) == 0 {
			return nil, fmt.Errorf("blob %d has no digest", i)
		}
		refs[i] = BlobRef{Digest: e.Digest, Start: e.Offset, End: next - 1}
	}
	return refs, nil
}

func (x *storedIndex) Locate(ctx context.Context, off int64) (BlobRef, error) {
	refs, err := x.Extent(ctx, off)
	if err != nil {
		return BlobRef{}, err
	}
	return BlobRefs(refs).Locate(ctx, off)
}

func (x *storedIndex) Extent(ctx context.Context, off int64) ([]BlobRef, error) {
	if err := x.check(); err != nil {
		return nil, err
	}
	if off < 0 || off >= x.size {
		return nil, fmt.Errorf("bucket: offset %d is outside a %d-byte body", off, x.size)
	}
	return x.load(ctx, x.extentFor(off))
}

func (x *storedIndex) Digests(ctx context.Context) ([]mh.Multihash, error) {
	if err := x.check(); err != nil {
		return nil, err
	}
	out := make([]mh.Multihash, 0, len(x.list.Head))
	for e := 0; x.size > 0 && e <= len(x.list.Extents); e++ {
		refs, err := x.load(ctx, e)
		if err != nil {
			return nil, err
		}
		for _, r := range refs {
			out = append(out, r.Digest)
		}
	}
	return out, nil
}
