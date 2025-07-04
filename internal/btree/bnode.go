package btree

import (
	"bytes"
	"encoding/binary"

	"kvs/internal/utils"
)

const (
	BNODE_NODE         = 1 // Internal nodes with pointers
	BNODE_LEAF         = 2 // Leaf nodes with values
	BTREE_PAGE_SIZE    = 4096
	BTREE_MAX_KEY_SIZE = 1000
	BTREE_MAX_VAL_SIZE = 3000
)

// BNode represents a B-tree node in memory.
// Structure of a BNode:
//
//	| btype | nkeys |   pointers   |   offsets   | key-values | unused |
//	|  2B   |  2B   |  nkeys × 8B  |  nkeys × 2B |    ...     |        |
//
// Fields:
//   - btype:   2 bytes, node type (internal or leaf)
//   - nkeys:   2 bytes, number of keys
//   - pointers: nkeys × 8 bytes, child pointers (internal nodes only)
//   - offsets:  nkeys × 2 bytes, offsets to key-value data
//   - key-values: variable, actual key and value data
type BNode []byte

// Read the fixed-size header.
func (node BNode) btype() uint16 {
	return binary.LittleEndian.Uint16(node[0:2])
}
func (node BNode) nkeys() uint16 {
	return binary.LittleEndian.Uint16(node[2:4])
}

// Write the fixed-size header.
func (node BNode) setHeader(btype uint16, nkeys uint16) {
	binary.LittleEndian.PutUint16(node[0:2], btype)
	binary.LittleEndian.PutUint16(node[2:4], nkeys)
}

// Read and write the pointer array (for internal nodes).
func (node BNode) getPtr(idx uint16) uint64 {
	utils.Assert(idx < node.nkeys())
	pos := 4 + 8*idx
	return binary.LittleEndian.Uint64(node[pos:])
}
func (node BNode) setPtr(idx uint16, val uint64) {
	utils.Assert(idx < node.nkeys())
	pos := 4 + 8*idx
	binary.LittleEndian.PutUint64(node[pos:], val)
}

// Read the offsets array to locate the nth key in O(1).
func (node BNode) getOffset(idx uint16) uint16 {
	if idx == 0 {
		return 0
	}
	pos := 4 + 8*node.nkeys() + 2*(idx-1)
	return binary.LittleEndian.Uint16(node[pos:])
}
func (node BNode) setOffset(idx uint16, val uint16) {
	utils.Assert(idx <= node.nkeys())
	pos := 4 + 8*node.nkeys() + 2*(idx-1)
	binary.LittleEndian.PutUint16(node[pos:], val)
}

// Return the position of the nth key using GetOffset().
func (node BNode) kvPos(idx uint16) uint16 {
	utils.Assert(idx <= node.nkeys())
	return 4 + 8*node.nkeys() + 2*node.nkeys() + node.getOffset(idx)
}

// Get the nth key data as a slice.
func (node BNode) getKey(idx uint16) []byte {
	utils.Assert(idx < node.nkeys())
	pos := node.kvPos(idx)
	klen := binary.LittleEndian.Uint16(node[pos:])
	return node[pos+4:][:klen]
}

// Get the nth value data as a slice (for leaf nodes).
func (node BNode) getVal(idx uint16) []byte {
	utils.Assert(idx < node.nkeys())
	pos := node.kvPos(idx)
	klen := binary.LittleEndian.Uint16(node[pos+0:])
	vlen := binary.LittleEndian.Uint16(node[pos+2:])
	return node[pos+4+klen:][:vlen]
}

// Node size in bytes using the last key's offset.
func (node BNode) nbytes() uint16 {
	return node.kvPos(node.nkeys())
}

// Create a new BNode with the given type and number of keys.
func nodeAppendKV(new BNode, idx uint16, ptr uint64, key []byte, val []byte) {
	new.setPtr(idx, ptr)
	pos := new.kvPos(idx) // uses the offset value of the previous key
	binary.LittleEndian.PutUint16(new[pos+0:], uint16(len(key)))
	binary.LittleEndian.PutUint16(new[pos+2:], uint16(len(val)))
	// KV data
	copy(new[pos+4:], key)
	copy(new[pos+4+uint16(len(key)):], val)
	// update the offset value for the next key
	new.setOffset(idx+1, new.getOffset(idx)+4+uint16((len(key)+len(val))))
}

// Append a range of keys from an old node to a new node.
func nodeAppendRange(
	new BNode, old BNode, dstNew uint16, srcOld uint16, n uint16,
) {
	for i := uint16(0); i < n; i++ {
		dst, src := dstNew+i, srcOld+i
		nodeAppendKV(new, dst,
			old.getPtr(src), old.getKey(src), old.getVal(src))
	}
}

// Insert a new key at position `idx` in a leaf node.
func leafInsert(
	new BNode, old BNode, idx uint16, key []byte, val []byte,
) {
	new.setHeader(BNODE_LEAF, old.nkeys()+1)
	nodeAppendRange(new, old, 0, 0, idx)                   // copy the keys before `idx`
	nodeAppendKV(new, idx, 0, key, val)                    // the new key
	nodeAppendRange(new, old, idx+1, idx, old.nkeys()-idx) // keys from `idx`
}

// Update an existing key at position `idx` in a leaf node.
func leafUpdate(
	new BNode, old BNode, idx uint16, key []byte, val []byte,
) {
	new.setHeader(BNODE_LEAF, old.nkeys())
	nodeAppendRange(new, old, 0, 0, idx)
	nodeAppendKV(new, idx, 0, key, val)
	nodeAppendRange(new, old, idx+1, idx+1, old.nkeys()-(idx+1))
}

// Find the last postion that is less than or equal to the key
func nodeLookupLE(node BNode, key []byte) uint16 {
	nkeys := node.nkeys()
	if nkeys == 0 {
		return 0
	}
	lo, hi := uint16(0), nkeys-1
	var mid uint16
	for lo <= hi {
		mid = lo + (hi-lo)/2
		cmp := bytes.Compare(node.getKey(mid), key)
		if cmp == 0 {
			return mid
		}
		if cmp < 0 {
			lo = mid + 1
		} else {
			if mid == 0 {
				break
			}
			hi = mid - 1
		}
	}
	if hi < nkeys {
		return hi
	}
	return 0 // fallback, should not happen
}

// Split an oversized node into 2 nodes. The 2nd node always fits.
func nodeSplit2(left BNode, right BNode, old BNode) {
	utils.Assert(old.nkeys() >= 2)
	// the initial guess
	nleft := old.nkeys() / 2
	// try to fit the left half
	left_bytes := func() uint16 {
		return 4 + 8*nleft + 2*nleft + old.getOffset(nleft)
	}
	for left_bytes() > BTREE_PAGE_SIZE {
		nleft--
	}
	utils.Assert(nleft >= 1)
	// try to fit the right half
	right_bytes := func() uint16 {
		return old.nkeys() - left_bytes() + 4
	}
	for right_bytes() > BTREE_PAGE_SIZE {
		nleft++
	}
	utils.Assert(nleft < old.nkeys())
	nright := old.nkeys() - nleft
	// new nodes
	left.setHeader(old.btype(), nleft)
	right.setHeader(old.btype(), nright)
	nodeAppendRange(left, old, 0, 0, nleft)
	nodeAppendRange(right, old, 0, nleft, nright)
	// NOTE: the left half may be still too big
	utils.Assert(right.nbytes() <= BTREE_PAGE_SIZE)
}

// Split a node if it's too big. the results are 1~3 nodes.
func nodeSplit3(old BNode) (uint16, [3]BNode) {
	if old.nbytes() <= BTREE_PAGE_SIZE {
		old = old[:BTREE_PAGE_SIZE]
		return 1, [3]BNode{old} // not split
	}
	left := BNode(make([]byte, 2*BTREE_PAGE_SIZE)) // might be split later
	right := BNode(make([]byte, BTREE_PAGE_SIZE))
	nodeSplit2(left, right, old)
	if left.nbytes() <= BTREE_PAGE_SIZE {
		left = left[:BTREE_PAGE_SIZE]
		return 2, [3]BNode{left, right} // 2 nodes
	}
	leftleft := BNode(make([]byte, BTREE_PAGE_SIZE))
	middle := BNode(make([]byte, BTREE_PAGE_SIZE))
	nodeSplit2(leftleft, middle, left)
	utils.Assert(leftleft.nbytes() <= BTREE_PAGE_SIZE)
	return 3, [3]BNode{leftleft, middle, right} // 3 nodes
}
