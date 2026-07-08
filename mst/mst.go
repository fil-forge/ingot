// Package mst is a fork of github.com/bluesky-social/indigo/mst with the
// atproto-specific key validation relaxed for use as a generic ordered
// content-addressed key/value map. Keys may be any non-empty UTF-8 string up
// to 1024 bytes, with the only forbidden bytes being NUL.
//
// On-disk format is unchanged from the atproto MST: NodeData / TreeEntry CBOR
// blocks with prefix-compressed byte-string keys. Cross-implementation
// compatibility with atproto MSTs is intentionally not preserved.
//
// See https://hal.inria.fr/hal-02303490/document for the underlying data
// structure. SHA-256 is used for key hashing with a 4-bit fanout (~16 entries
// per layer).
package mst

import (
	"context"
	"fmt"
	"reflect"

	"github.com/fil-forge/ucantone/did"
	"github.com/ipfs/go-cid"

	"github.com/fil-forge/ingot/blockstore"
)

// The MST package consumes blockstore.Reader on the load + traversal
// path and blockstore.Store on the materialization (GetPointer)
// path. Mutating operations (Add, Update, Delete) build new
// in-memory tree values without any I/O writes; the only write site
// in the package is GetPointer, which takes a writer argument.

// nodeKind is the type of node in the MST.
type nodeKind uint8

const (
	entryUndefined nodeKind = 0
	entryLeaf      nodeKind = 1
	entryTree      nodeKind = 2
)

// nodeEntry is either a leaf (key/value) or a pointer to a subtree.
type nodeEntry struct {
	Kind nodeKind
	Key  string
	Val  cid.Cid
	Tree *MerkleSearchTree
}

func mkTreeEntry(t *MerkleSearchTree) nodeEntry {
	return nodeEntry{
		Kind: entryTree,
		Tree: t,
	}
}

func (ne nodeEntry) isTree() bool      { return ne.Kind == entryTree }
func (ne nodeEntry) isLeaf() bool      { return ne.Kind == entryLeaf }
func (ne nodeEntry) isUndefined() bool { return ne.Kind == entryUndefined }

// Sanity check: two trees can never be neighbors in an entries slice.
func checkTreeInvariant(ents []nodeEntry) {
	for i := 0; i < len(ents)-1; i++ {
		if ents[i].isTree() && ents[i+1].isTree() {
			panic(fmt.Sprintf("two trees next to each other! %d %d", i, i+1))
		}
	}
}

// CBORTypes returns the types in this package that need to be registered with
// the CBOR codec.
func CBORTypes() []reflect.Type {
	return []reflect.Type{
		reflect.TypeOf(NodeData{}),
		reflect.TypeOf(TreeEntry{}),
	}
}

// NodeData is the CBOR-serialized form of an MST node.
type NodeData struct {
	Left    *cid.Cid    `cborgen:"l"` // [nullable] pointer to lower-level subtree to the "left" of this path/key
	Entries []TreeEntry `cborgen:"e"` // ordered list of entries at this node
}

// TreeEntry is one entry within a NodeData.
type TreeEntry struct {
	PrefixLen int64    `cborgen:"p"` // count of bytes shared with previous key in tree
	KeySuffix []byte   `cborgen:"k"` // remaining part of key (appended to "previous key")
	Val       cid.Cid  `cborgen:"v"` // CID pointer at this path/key
	Tree      *cid.Cid `cborgen:"t"` // [nullable] pointer to lower-level subtree to the "right" of this entry
}

// MerkleSearchTree is an MST tree node. Values are immutable: methods return
// copies with changes applied. Hydration is lazy; a tree loaded by CID has no
// entries until getEntries is called.
//
// The cst field is a blockstore.Reader: traversal and mutation
// (Add/Update/Delete) both stay read-only at the storage level,
// returning new in-memory MerkleSearchTree values rather than
// persisting anything. The only write site is GetPointer, which
// takes its writer as an explicit argument.
type MerkleSearchTree struct {
	cst blockstore.Reader
	// space is the Forge space the tree's blocks belong to (the owning
	// bucket's), passed to every cst.Get so misses can resolve from the
	// network. did.Undef where reads are purely local.
	space    did.DID
	entries  []nodeEntry // non-nil when "hydrated"
	layer    int
	pointer  cid.Cid
	validPtr bool
}

// NewEmptyMST returns a new empty MST using cst as its storage.
func NewEmptyMST(cst blockstore.Reader, space did.DID) *MerkleSearchTree {
	return createMST(cst, space, cid.Undef, []nodeEntry{}, 0)
}

func createMST(cst blockstore.Reader, space did.DID, ptr cid.Cid, entries []nodeEntry, layer int) *MerkleSearchTree {
	mst := &MerkleSearchTree{
		cst:      cst,
		space:    space,
		pointer:  ptr,
		layer:    layer,
		entries:  entries,
		validPtr: ptr.Defined(),
	}
	return mst
}

// LoadMST returns a lazy reference to an MST rooted at the given CID. Entries
// are not loaded until needed.
func LoadMST(cst blockstore.Reader, space did.DID, root cid.Cid) *MerkleSearchTree {
	return createMST(cst, space, root, nil, -1)
}

// === Immutability ===

func (mst *MerkleSearchTree) newTree(entries []nodeEntry) *MerkleSearchTree {
	if entries == nil {
		panic("nil entries passed to newTree")
	}
	return createMST(mst.cst, mst.space, cid.Undef, entries, mst.layer)
}

// === Lazy getters ===

func (mst *MerkleSearchTree) getEntries(ctx context.Context) ([]nodeEntry, error) {
	if mst.entries != nil {
		return mst.entries, nil
	}

	if mst.pointer != cid.Undef {
		var nd NodeData
		if err := mst.cst.Get(ctx, mst.space, mst.pointer, &nd); err != nil {
			return nil, err
		}
		entries, err := entriesFromNodeData(ctx, &nd, mst.cst, mst.space)
		if err != nil {
			return nil, err
		}
		if entries == nil {
			panic("got nil entries from node data decoding")
		}
		mst.entries = entries
		return entries, nil
	}

	return nil, fmt.Errorf("no entries or self-pointer (CID) on MerkleSearchTree")
}

func entriesFromNodeData(ctx context.Context, nd *NodeData, cst blockstore.Reader, space did.DID) ([]nodeEntry, error) {
	layer := -1
	if len(nd.Entries) > 0 {
		// the first entry's KeySuffix is a complete key (PrefixLen=0)
		firstLeaf := nd.Entries[0]
		layer = leadingZerosOnHashBytes(firstLeaf.KeySuffix)
	}

	entries, err := deserializeNodeData(ctx, cst, space, nd, layer)
	if err != nil {
		return nil, err
	}

	return entries, nil
}

// GetPointer returns the CID of this MST root, recomputing it if any subtree
// has been mutated since the last call. writer is the IpldStore that any
// freshly-serialized subtree nodes are Put through; only this method (and the
// cidForEntries / serializeNodeData helpers it drives) ever issues writes
// against it.
func (mst *MerkleSearchTree) GetPointer(ctx context.Context, writer blockstore.Store) (cid.Cid, error) {
	if mst.validPtr {
		return mst.pointer, nil
	}

	if _, err := mst.getEntries(ctx); err != nil {
		return cid.Undef, err
	}

	for i, e := range mst.entries {
		if e.isTree() {
			if !e.Tree.validPtr {
				if _, err := e.Tree.GetPointer(ctx, writer); err != nil {
					return cid.Undef, err
				}
				mst.entries[i] = e
			}
		}
	}

	nptr, err := cidForEntries(ctx, mst.entries, writer)
	if err != nil {
		return cid.Undef, err
	}
	mst.pointer = nptr
	mst.validPtr = true

	return mst.pointer, nil
}

func (mst *MerkleSearchTree) getLayer(ctx context.Context) (int, error) {
	layer, err := mst.attemptGetLayer(ctx)
	if err != nil {
		return -1, err
	}
	if layer < 0 {
		mst.layer = 0
	} else {
		mst.layer = layer
	}
	return mst.layer, nil
}

func (mst *MerkleSearchTree) attemptGetLayer(ctx context.Context) (int, error) {
	if mst.layer >= 0 {
		return mst.layer, nil
	}

	entries, err := mst.getEntries(ctx)
	if err != nil {
		return -1, err
	}

	layer := layerForEntries(entries)
	if layer < 0 {
		for _, e := range entries {
			if e.isTree() {
				childLayer, err := e.Tree.attemptGetLayer(ctx)
				if err != nil {
					return -1, err
				}
				if childLayer >= 0 {
					layer = childLayer + 1
					break
				}
			}
		}
	}

	if layer >= 0 {
		mst.layer = layer
	}
	return mst.layer, nil
}

// === Core operations ===

// Add inserts a new key/value pair. Returns ErrAlreadyExists if the key is
// already present.
func (mst *MerkleSearchTree) Add(ctx context.Context, key string, val cid.Cid, knownZeros int) (*MerkleSearchTree, error) {
	if err := ensureValidKey(key); err != nil {
		return nil, err
	}

	if val == cid.Undef {
		return nil, fmt.Errorf("tried to insert an undef CID")
	}

	keyZeros := knownZeros
	if keyZeros < 0 {
		keyZeros = leadingZerosOnHash(key)
	}

	layer, err := mst.getLayer(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting layer failed: %w", err)
	}

	newLeaf := nodeEntry{
		Kind: entryLeaf,
		Key:  key,
		Val:  val,
	}

	if keyZeros == layer {
		index, err := mst.findGtOrEqualLeafIndex(ctx, key)
		if err != nil {
			return nil, err
		}

		found, err := mst.atIndex(index)
		if err != nil {
			return nil, err
		}

		if found.isLeaf() && found.Key == key {
			return nil, ErrAlreadyExists
		}

		prevNode, err := mst.atIndex(index - 1)
		if err != nil {
			return nil, err
		}

		if prevNode.isUndefined() || prevNode.isLeaf() {
			return mst.spliceIn(ctx, newLeaf, index)
		}

		left, right, err := prevNode.Tree.splitAround(ctx, key)
		if err != nil {
			return nil, err
		}
		return mst.replaceWithSplit(ctx, index-1, left, newLeaf, right)

	} else if keyZeros < layer {
		index, err := mst.findGtOrEqualLeafIndex(ctx, key)
		if err != nil {
			return nil, err
		}

		prevNode, err := mst.atIndex(index - 1)
		if err != nil {
			return nil, err
		}

		if !prevNode.isUndefined() && prevNode.isTree() {
			newSubtree, err := prevNode.Tree.Add(ctx, key, val, keyZeros)
			if err != nil {
				return nil, err
			}
			return mst.updateEntry(ctx, index-1, mkTreeEntry(newSubtree))
		}

		subTree, err := mst.createChild(ctx)
		if err != nil {
			return nil, err
		}

		newSubTree, err := subTree.Add(ctx, key, val, keyZeros)
		if err != nil {
			return nil, fmt.Errorf("subtree add: %w", err)
		}

		return mst.spliceIn(ctx, mkTreeEntry(newSubTree), index)
	}

	// keyZeros > layer: must push the rest of the tree down
	left, right, err := mst.splitAround(ctx, key)
	if err != nil {
		return nil, err
	}

	layer, err = mst.getLayer(ctx)
	if err != nil {
		return nil, fmt.Errorf("get layer in split case failed: %w", err)
	}

	extraLayersToAdd := keyZeros - layer

	for i := 1; i < extraLayersToAdd; i++ {
		if left != nil {
			par, err := left.createParent(ctx)
			if err != nil {
				return nil, fmt.Errorf("create left parent: %w", err)
			}
			left = par
		}

		if right != nil {
			par, err := right.createParent(ctx)
			if err != nil {
				return nil, fmt.Errorf("create right parent: %w", err)
			}
			right = par
		}
	}

	var updated []nodeEntry
	if left != nil {
		updated = append(updated, mkTreeEntry(left))
	}

	updated = append(updated, nodeEntry{
		Kind: entryLeaf,
		Key:  key,
		Val:  val,
	})

	if right != nil {
		updated = append(updated, mkTreeEntry(right))
	}

	checkTreeInvariant(updated)
	newRoot := createMST(mst.cst, mst.space, cid.Undef, updated, keyZeros)
	newRoot.validPtr = false

	return newRoot, nil
}

// ErrNotFound is returned by Get / Update / Delete when the key is absent.
var ErrNotFound = fmt.Errorf("mst: not found")

// ErrAlreadyExists is returned by Add when the key is already present.
var ErrAlreadyExists = fmt.Errorf("mst: key already exists")

// Get returns the CID at the given key, or ErrNotFound.
func (mst *MerkleSearchTree) Get(ctx context.Context, k string) (cid.Cid, error) {
	index, err := mst.findGtOrEqualLeafIndex(ctx, k)
	if err != nil {
		return cid.Undef, err
	}

	found, err := mst.atIndex(index)
	if err != nil {
		return cid.Undef, err
	}

	if !found.isUndefined() && found.isLeaf() && found.Key == k {
		return found.Val, nil
	}

	prev, err := mst.atIndex(index - 1)
	if err != nil {
		return cid.Undef, err
	}

	if !prev.isUndefined() && prev.isTree() {
		return prev.Tree.Get(ctx, k)
	}

	return cid.Undef, ErrNotFound
}

// Update replaces the value at an existing key. Returns ErrNotFound if absent.
func (mst *MerkleSearchTree) Update(ctx context.Context, k string, val cid.Cid) (*MerkleSearchTree, error) {
	if err := ensureValidKey(k); err != nil {
		return nil, err
	}

	if val == cid.Undef {
		return nil, fmt.Errorf("tried to insert an undef CID")
	}

	index, err := mst.findGtOrEqualLeafIndex(ctx, k)
	if err != nil {
		return nil, err
	}

	found, err := mst.atIndex(index)
	if err != nil {
		return nil, err
	}

	if !found.isUndefined() && found.isLeaf() && found.Key == k {
		return mst.updateEntry(ctx, index, nodeEntry{
			Kind: entryLeaf,
			Key:  k,
			Val:  val,
		})
	}

	prev, err := mst.atIndex(index - 1)
	if err != nil {
		return nil, err
	}

	if !prev.isUndefined() && prev.isTree() {
		updatedTree, err := prev.Tree.Update(ctx, k, val)
		if err != nil {
			return nil, err
		}
		return mst.updateEntry(ctx, index-1, mkTreeEntry(updatedTree))
	}

	return nil, ErrNotFound
}

// Delete removes the leaf at the given key.
func (mst *MerkleSearchTree) Delete(ctx context.Context, k string) (*MerkleSearchTree, error) {
	altered, err := mst.deleteRecurse(ctx, k)
	if err != nil {
		return nil, err
	}
	return altered.trimTop(ctx)
}

func (mst *MerkleSearchTree) deleteRecurse(ctx context.Context, k string) (*MerkleSearchTree, error) {
	ix, err := mst.findGtOrEqualLeafIndex(ctx, k)
	if err != nil {
		return nil, err
	}

	found, err := mst.atIndex(ix)
	if err != nil {
		return nil, err
	}

	if found.isLeaf() && found.Key == k {
		prev, err := mst.atIndex(ix - 1)
		if err != nil {
			return nil, err
		}

		next, err := mst.atIndex(ix + 1)
		if err != nil {
			return nil, err
		}

		if prev.isTree() && next.isTree() {
			merged, err := prev.Tree.appendMerge(ctx, next.Tree)
			if err != nil {
				return nil, err
			}
			entries, err := mst.getEntries(ctx)
			if err != nil {
				return nil, err
			}
			return mst.newTree(append(append(entries[:ix-1], mkTreeEntry(merged)), entries[ix+2:]...)), nil
		}
		return mst.removeEntry(ctx, ix)
	}

	prev, err := mst.atIndex(ix - 1)
	if err != nil {
		return nil, err
	}

	if prev.isTree() {
		subtree, err := prev.Tree.deleteRecurse(ctx, k)
		if err != nil {
			return nil, err
		}

		subtreeEntries, err := subtree.getEntries(ctx)
		if err != nil {
			return nil, err
		}

		if len(subtreeEntries) == 0 {
			return mst.removeEntry(ctx, ix-1)
		}
		return mst.updateEntry(ctx, ix-1, mkTreeEntry(subtree))
	}

	return nil, ErrNotFound
}

// === Simple operations ===

func (mst *MerkleSearchTree) updateEntry(ctx context.Context, ix int, entry nodeEntry) (*MerkleSearchTree, error) {
	entries, err := mst.getEntries(ctx)
	if err != nil {
		return nil, err
	}

	nents := make([]nodeEntry, len(entries))
	copy(nents, entries[:ix])
	nents[ix] = entry
	copy(nents[ix+1:], entries[ix+1:])

	checkTreeInvariant(nents)
	return mst.newTree(nents), nil
}

func (mst *MerkleSearchTree) removeEntry(ctx context.Context, ix int) (*MerkleSearchTree, error) {
	entries, err := mst.getEntries(ctx)
	if err != nil {
		return nil, err
	}

	nents := make([]nodeEntry, len(entries)-1)
	copy(nents, entries[:ix])
	copy(nents[ix:], entries[ix+1:])

	checkTreeInvariant(nents)
	return mst.newTree(nents), nil
}

func (mst *MerkleSearchTree) append(ctx context.Context, ent nodeEntry) (*MerkleSearchTree, error) {
	entries, err := mst.getEntries(ctx)
	if err != nil {
		return nil, err
	}

	nents := make([]nodeEntry, len(entries)+1)
	copy(nents, entries)
	nents[len(nents)-1] = ent

	checkTreeInvariant(nents)
	return mst.newTree(nents), nil
}

func (mst *MerkleSearchTree) prepend(ctx context.Context, ent nodeEntry) (*MerkleSearchTree, error) {
	entries, err := mst.getEntries(ctx)
	if err != nil {
		return nil, err
	}

	nents := make([]nodeEntry, len(entries)+1)
	copy(nents[1:], entries)
	nents[0] = ent

	checkTreeInvariant(nents)
	return mst.newTree(nents), nil
}

func (mst *MerkleSearchTree) atIndex(ix int) (nodeEntry, error) {
	entries, err := mst.getEntries(context.TODO())
	if err != nil {
		return nodeEntry{}, err
	}

	if ix < 0 || ix >= len(entries) {
		return nodeEntry{}, nil
	}

	return entries[ix], nil
}

func (mst *MerkleSearchTree) spliceIn(ctx context.Context, entry nodeEntry, ix int) (*MerkleSearchTree, error) {
	entries, err := mst.getEntries(ctx)
	if err != nil {
		return nil, err
	}

	nents := make([]nodeEntry, len(entries)+1)
	copy(nents, entries[:ix])
	nents[ix] = entry
	copy(nents[ix+1:], entries[ix:])

	checkTreeInvariant(nents)
	return mst.newTree(nents), nil
}

func (mst *MerkleSearchTree) replaceWithSplit(ctx context.Context, ix int, left *MerkleSearchTree, nl nodeEntry, right *MerkleSearchTree) (*MerkleSearchTree, error) {
	entries, err := mst.getEntries(ctx)
	if err != nil {
		return nil, err
	}
	checkTreeInvariant(entries)
	var update []nodeEntry
	update = append(update, entries[:ix]...)

	if left != nil {
		update = append(update, nodeEntry{
			Kind: entryTree,
			Tree: left,
		})
	}

	update = append(update, nl)

	if right != nil {
		update = append(update, nodeEntry{
			Kind: entryTree,
			Tree: right,
		})
	}

	update = append(update, entries[ix+1:]...)

	checkTreeInvariant(update)
	return mst.newTree(update), nil
}

func (mst *MerkleSearchTree) trimTop(ctx context.Context) (*MerkleSearchTree, error) {
	entries, err := mst.getEntries(ctx)
	if err != nil {
		return nil, err
	}
	if len(entries) == 1 && entries[0].isTree() {
		return entries[0].Tree.trimTop(ctx)
	}
	return mst, nil
}

// === Subtree splits ===

func (mst *MerkleSearchTree) splitAround(ctx context.Context, key string) (*MerkleSearchTree, *MerkleSearchTree, error) {
	index, err := mst.findGtOrEqualLeafIndex(ctx, key)
	if err != nil {
		return nil, nil, err
	}

	entries, err := mst.getEntries(ctx)
	if err != nil {
		return nil, nil, err
	}

	leftData := entries[:index]
	rightData := entries[index:]
	left := mst.newTree(leftData)
	right := mst.newTree(rightData)

	if len(leftData) > 0 && leftData[len(leftData)-1].isTree() {
		lastInLeft := leftData[len(leftData)-1]

		nleft, err := left.removeEntry(ctx, len(leftData)-1)
		if err != nil {
			return nil, nil, err
		}
		left = nleft

		subl, subr, err := lastInLeft.Tree.splitAround(ctx, key)
		if err != nil {
			return nil, nil, err
		}

		if subl != nil {
			left, err = left.append(ctx, mkTreeEntry(subl))
			if err != nil {
				return nil, nil, err
			}
		}

		if subr != nil {
			right, err = right.prepend(ctx, mkTreeEntry(subr))
			if err != nil {
				return nil, nil, err
			}
		}
	}

	if left.entryCount() == 0 {
		left = nil
	}
	if right.entryCount() == 0 {
		right = nil
	}

	return left, right, nil
}

func (mst *MerkleSearchTree) entryCount() int {
	entries, err := mst.getEntries(context.TODO())
	if err != nil {
		panic(err)
	}
	return len(entries)
}

func (mst *MerkleSearchTree) appendMerge(ctx context.Context, omst *MerkleSearchTree) (*MerkleSearchTree, error) {
	mylayer, err := mst.getLayer(ctx)
	if err != nil {
		return nil, err
	}

	olayer, err := omst.getLayer(ctx)
	if err != nil {
		return nil, err
	}

	if mylayer != olayer {
		return nil, fmt.Errorf("trying to merge two nodes from different layers")
	}

	entries, err := mst.getEntries(ctx)
	if err != nil {
		return nil, err
	}

	tomergeEnts, err := omst.getEntries(ctx)
	if err != nil {
		return nil, err
	}

	lastInLeft := entries[len(entries)-1]
	firstInRight := tomergeEnts[0]

	if lastInLeft.isTree() && firstInRight.isTree() {
		merged, err := lastInLeft.Tree.appendMerge(ctx, firstInRight.Tree)
		if err != nil {
			return nil, err
		}
		return mst.newTree(append(append(entries[:len(entries)-1], mkTreeEntry(merged)), tomergeEnts[1:]...)), nil
	}
	return mst.newTree(append(entries, tomergeEnts...)), nil
}

// === Create relatives ===

func (mst *MerkleSearchTree) createChild(ctx context.Context) (*MerkleSearchTree, error) {
	layer, err := mst.getLayer(ctx)
	if err != nil {
		return nil, err
	}
	return createMST(mst.cst, mst.space, cid.Undef, []nodeEntry{}, layer-1), nil
}

func (mst *MerkleSearchTree) createParent(ctx context.Context) (*MerkleSearchTree, error) {
	layer, err := mst.getLayer(ctx)
	if err != nil {
		return nil, err
	}
	return createMST(mst.cst, mst.space, cid.Undef, []nodeEntry{mkTreeEntry(mst)}, layer+1), nil
}

// === Finding insertion points ===

func (mst *MerkleSearchTree) findGtOrEqualLeafIndex(ctx context.Context, key string) (int, error) {
	entries, err := mst.getEntries(ctx)
	if err != nil {
		return -1, err
	}

	for i, e := range entries {
		if e.isLeaf() && e.Key >= key {
			return i, nil
		}
	}

	return len(entries), nil
}

// === List operations ===

// ErrStopWalk halts a WalkLeavesFrom traversal without surfacing as an error
// to the caller. The walk function returns nil after stopping.
var ErrStopWalk = fmt.Errorf("mst: stop walk")

// WalkLeavesFrom walks leaves in sorted order starting at the first key >=
// from. The callback may return ErrStopWalk to halt early; any other error
// aborts and is returned to the caller.
func (mst *MerkleSearchTree) WalkLeavesFrom(ctx context.Context, from string, cb func(key string, val cid.Cid) error) error {
	err := mst.walkLeavesFrom(ctx, from, false, cb)
	if err == ErrStopWalk {
		return nil
	}
	return err
}

// WalkLeavesFromNocache is like WalkLeavesFrom but does not retain hydrated
// subtree state, intended for one-pass streaming traversals.
func (mst *MerkleSearchTree) WalkLeavesFromNocache(ctx context.Context, from string, cb func(key string, val cid.Cid) error) error {
	err := mst.walkLeavesFrom(ctx, from, true, cb)
	if err == ErrStopWalk {
		return nil
	}
	return err
}

func (mst *MerkleSearchTree) walkLeavesFrom(ctx context.Context, from string, nocache bool, cb func(key string, val cid.Cid) error) error {
	index, err := mst.findGtOrEqualLeafIndex(ctx, from)
	if err != nil {
		return err
	}

	entries, err := mst.getEntries(ctx)
	if err != nil {
		return fmt.Errorf("get entries: %w", err)
	}

	if index > 0 {
		prev := entries[index-1]
		if !prev.isUndefined() && prev.isTree() {
			if err := prev.Tree.walkLeavesFrom(ctx, from, nocache, cb); err != nil {
				return err
			}
		}
	}

	for _, e := range entries[index:] {
		if e.isLeaf() {
			if err := cb(e.Key, e.Val); err != nil {
				return err
			}
		} else {
			if err := e.Tree.walkLeavesFrom(ctx, from, nocache, cb); err != nil {
				return err
			}
			if nocache {
				e.Tree = nil
			}
		}
	}
	return nil
}
