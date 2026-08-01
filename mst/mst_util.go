package mst

import (
	"context"
	"crypto/sha256"
	"fmt"
	"strings"
	"unicode/utf8"
	"unsafe"

	"github.com/fil-forge/ucantone/did"
	"github.com/ipfs/go-cid"

	"github.com/fil-forge/ingot/blockstore"
)

// MaxKeyBytes is the maximum length, in bytes, of a key stored in the MST.
// Matches S3's object key length cap.
const MaxKeyBytes = 1024

// 4-bit fanout: count zero bits in 2-bit chunks. A leading 0x00 byte = 4 zeros.
func leadingZerosOnHash(key string) int {
	var b []byte
	if len(key) > 0 {
		b = unsafe.Slice(unsafe.StringData(key), len(key))
	}
	return leadingZerosOnHashBytes(b)
}

func leadingZerosOnHashBytes(key []byte) (total int) {
	hv := sha256.Sum256(key)
	for _, b := range hv {
		if b&0xC0 != 0 {
			break
		}
		if b == 0x00 {
			total += 4
			continue
		}
		if b&0xFC == 0x00 {
			total += 3
		} else if b&0xF0 == 0x00 {
			total += 2
		} else {
			total += 1
		}
		break
	}
	return total
}

func layerForEntries(entries []nodeEntry) int {
	var firstLeaf nodeEntry
	for _, e := range entries {
		if e.isLeaf() {
			firstLeaf = e
			break
		}
	}

	if firstLeaf.Kind == entryUndefined {
		return -1
	}

	return leadingZerosOnHash(firstLeaf.Key)
}

func deserializeNodeData(ctx context.Context, cst blockstore.Reader, space did.DID, nd *NodeData, layer int) ([]nodeEntry, error) {
	entries := []nodeEntry{}
	if nd.Left != nil {
		entries = append(entries, nodeEntry{
			Kind: entryTree,
			Tree: createMST(cst, space, *nd.Left, nil, layer-1),
		})
	}

	var lastKey string
	var keyb []byte // re-used between entries
	for _, e := range nd.Entries {
		if keyb == nil {
			keyb = make([]byte, 0, int(e.PrefixLen)+len(e.KeySuffix))
		}
		keyb = append(keyb[:0], lastKey[:e.PrefixLen]...)
		keyb = append(keyb, e.KeySuffix...)

		keyStr := string(keyb)
		if err := ensureValidKey(keyStr); err != nil {
			return nil, err
		}

		entries = append(entries, nodeEntry{
			Kind: entryLeaf,
			Key:  keyStr,
			Val:  e.Val,
		})

		if e.Tree != nil {
			entries = append(entries, nodeEntry{
				Kind: entryTree,
				Tree: createMST(cst, space, *e.Tree, nil, layer-1),
				Key:  keyStr,
			})
		}
		lastKey = keyStr
	}

	return entries, nil
}

func serializeNodeData(ctx context.Context, entries []nodeEntry, writer blockstore.Store) (*NodeData, error) {
	var data NodeData

	i := 0
	if len(entries) > 0 && entries[0].isTree() {
		i++

		ptr, err := entries[0].Tree.GetPointer(ctx, writer)
		if err != nil {
			return nil, err
		}
		data.Left = &ptr
	}

	var lastKey string
	for i < len(entries) {
		leaf := entries[i]

		if !leaf.isLeaf() {
			return nil, fmt.Errorf("not a valid node: two subtrees next to each other (%d, %d)", i, len(entries))
		}
		i++

		var subtree *cid.Cid

		if i < len(entries) {
			next := entries[i]

			if next.isTree() {
				ptr, err := next.Tree.GetPointer(ctx, writer)
				if err != nil {
					return nil, fmt.Errorf("getting subtree pointer: %w", err)
				}

				subtree = &ptr
				i++
			}
		}

		if err := ensureValidKey(leaf.Key); err != nil {
			return nil, err
		}

		prefixLen := countPrefixLen(lastKey, leaf.Key)
		data.Entries = append(data.Entries, TreeEntry{
			PrefixLen: int64(prefixLen),
			KeySuffix: []byte(leaf.Key)[prefixLen:],
			Val:       leaf.Val,
			Tree:      subtree,
		})

		lastKey = leaf.Key
	}

	return &data, nil
}

func countPrefixLen(a, b string) int {
	var i int
	for i = 0; i < len(a) && i < len(b); i++ {
		if a[i] != b[i] {
			return i
		}
	}
	return i
}

func cidForEntries(ctx context.Context, entries []nodeEntry, writer blockstore.Store) (cid.Cid, error) {
	nd, err := serializeNodeData(ctx, entries, writer)
	if err != nil {
		return cid.Undef, fmt.Errorf("serializing new entries: %w", err)
	}
	return writer.Put(ctx, nd)
}

// IsValidKey reports whether s is a valid MST key under this fork's relaxed
// rules: non-empty, valid UTF-8, no NUL bytes, at most MaxKeyBytes bytes long.
func IsValidKey(s string) bool {
	if len(s) == 0 || len(s) > MaxKeyBytes {
		return false
	}
	if !utf8.ValidString(s) {
		return false
	}
	if strings.ContainsRune(s, 0) {
		return false
	}
	return true
}

func ensureValidKey(s string) error {
	if !IsValidKey(s) {
		return fmt.Errorf("invalid mst key (len=%d)", len(s))
	}
	return nil
}
