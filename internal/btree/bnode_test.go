package btree

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// Helper Functions

// Checks if the key-value pairs in the node match the expected pairs.
func assertNodeKVPairs(t *testing.T, node BNode, expectedPairs []struct{ key, val string }) {
	for i, pair := range expectedPairs {
		assert.Equal(t, []byte(pair.key), node.getKey(uint16(i)), "Key mismatch at index %d", i)
		assert.Equal(t, []byte(pair.val), node.getVal(uint16(i)), "Value mismatch at index %d", i)
	}
}

// Sets up a B-tree node with 3 key-value pairs for testing.
func setupBtreeNode() BNode {
	node := BNode(make([]byte, BTREE_PAGE_SIZE))
	node.setHeader(BNODE_LEAF, 3)

	nodeAppendKV(node, 0, 0, []byte("k1"), []byte("v1"))
	nodeAppendKV(node, 1, 0, []byte("k2"), []byte("v2"))
	nodeAppendKV(node, 2, 0, []byte("k3"), []byte("v3"))

	return node
}

func TestNodeMaxSize(t *testing.T) {
	node1max := 4 + 1*8 + 1*2 + 4 + BTREE_MAX_KEY_SIZE + BTREE_MAX_VAL_SIZE
	assert.LessOrEqual(t, node1max, BTREE_PAGE_SIZE,
		"Maximum node size (%d bytes) exceeds page size (%d bytes)", node1max, BTREE_PAGE_SIZE)
}

func TestNodeAppendKV(t *testing.T) {
	node := setupBtreeNode()

	assert.Equal(t, uint16(3), node.nkeys())
	assert.Equal(t, BNODE_LEAF, int(node.btype()))
	assert.Equal(t, uint16(58), node.nbytes())

	expectedPairs := []struct{ key, val string }{
		{"k1", "v1"},
		{"k2", "v2"},
		{"k3", "v3"},
	}
	assertNodeKVPairs(t, node, expectedPairs)
}

func TestNodeLookupLE(t *testing.T) {
	node := setupBtreeNode()

	// Test exact matches
	assert.Equal(t, uint16(0), nodeLookupLE(node, []byte("k1")))
	assert.Equal(t, uint16(1), nodeLookupLE(node, []byte("k2")))
	assert.Equal(t, uint16(2), nodeLookupLE(node, []byte("k3")))

	// Test key that doesn't exist but should find the last key <= search key
	assert.Equal(t, uint16(0), nodeLookupLE(node, []byte("k1a")))
	assert.Equal(t, uint16(1), nodeLookupLE(node, []byte("k2z")))
}

func TestLeafInsert(t *testing.T) {
	oldNode := setupBtreeNode()
	newNode := BNode(make([]byte, BTREE_PAGE_SIZE))

	leafInsert(newNode, oldNode, 3, []byte("k4"), []byte("v4"))

	assert.Equal(t, uint16(4), newNode.nkeys())
	assert.Equal(t, BNODE_LEAF, int(newNode.btype()))

	expectedPairs := []struct{ key, val string }{
		{"k1", "v1"},
		{"k2", "v2"},
		{"k3", "v3"},
		{"k4", "v4"},
	}
	assertNodeKVPairs(t, newNode, expectedPairs)
}

func TestLeafUpdate(t *testing.T) {
	oldNode := setupBtreeNode()
	newNode := BNode(make([]byte, BTREE_PAGE_SIZE))

	leafUpdate(newNode, oldNode, 1, []byte("k2"), []byte("v2n"))

	assert.Equal(t, uint16(3), newNode.nkeys())
	assert.Equal(t, BNODE_LEAF, int(newNode.btype()))

	expectedPairs := []struct{ key, val string }{
		{"k1", "v1"},
		{"k2", "v2n"},
		{"k3", "v3"},
	}
	assertNodeKVPairs(t, newNode, expectedPairs)
}
