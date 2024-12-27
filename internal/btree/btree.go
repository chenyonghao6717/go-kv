package btree

import (
	"encoding/binary"
	"fmt"
)

/*
The B+tree structure of LSM-tree.
Treat nodes of BTrees as binary to simplify loads and dumps with disks.
Assign 4 KB space for each node.

An internal node layout:
|             header            |          data           |
| type | count | size | padding | child_pointers | unused |
|  2B  |  2B   |  2B  |   2B    |   count * 8B   |  ...   |

A leaf node layout:
|             header            |               data             |
| type | count | size | padding | kv_offsets  | kv_data | unused |
|  2B  |  2B   |  2B  |   2B    | count * 2B  |   ...   |  ...   |
where each kv_offset is the index(relative to the beginning of the kv_data part) of the first byte of a KV pair.

A KV pair layout:
| key_len | val_len | key | val |
|   2B    |   2B    | ... | ... |
*/

type BTree struct {
	rootPtr uint64

	get func(uint64) []byte
	new func([]byte) uint64
	del func(uint64)
}

type BTreeNode []byte

type BTreeNodeType byte

const (
	internal BTreeNodeType = iota
	leaf
)

const (
	typeLiteralInternal = "INTERNAL"
	typeLiteralLeaf     = "LEAF"
)

func getTypeLiteral(type_ BTreeNodeType) string {
	if type_ == internal {
		return typeLiteralInternal
	}
	return typeLiteralLeaf
}

const (
	headerOffset = 0
	headerLen    = 8
	typeOffset   = 0
	typeLen      = 2
	countOffset  = 2
	countLen     = 2
	sizeOffset   = 4
	sizeLen      = 2
)

func getSlice(slice []byte, offset uint16, len uint16) []byte {
	return slice[offset : offset+len]
}

func (node BTreeNode) getType() BTreeNodeType {
	return BTreeNodeType(binary.LittleEndian.Uint16(getSlice(node, typeOffset, typeLen)))
}

func (node BTreeNode) setType(type_ BTreeNodeType) {
	binary.LittleEndian.PutUint16(getSlice(node, typeOffset, typeLen), uint16(type_))
}

func (node BTreeNode) getCount() uint16 {
	return binary.LittleEndian.Uint16(getSlice(node, countOffset, countLen))
}

func (node BTreeNode) setCount(count uint16) {
	binary.LittleEndian.PutUint16(getSlice(node, countOffset, countLen), count)
}

func (node BTreeNode) getSize() uint16 {
	return binary.LittleEndian.Uint16(getSlice(node, sizeOffset, sizeLen))
}

func (node BTreeNode) setSize(size uint16) {
	binary.LittleEndian.PutUint16(getSlice(node, sizeOffset, sizeLen), size)
}

const (
	panicTypeMismatchMsg = "expected %v but got %v"
	panicOutOfBoundMsg   = "index %d out of bound %d"
)

func panicOutOfBound(idx uint16, bound uint16) {
	panic(fmt.Sprintf(panicOutOfBoundMsg, idx, bound))
}

func panicTypeMismatch(expected BTreeNodeType, actual BTreeNodeType) {
	panic(fmt.Sprintf(panicTypeMismatchMsg, getTypeLiteral(expected), getTypeLiteral(actual)))
}

func (node BTreeNode) checkType(type_ BTreeNodeType) {
	if nodeType := node.getType(); nodeType != type_ {
		panicTypeMismatch(type_, node.getType())
	}
}

func (node BTreeNode) checkIdx(idx uint16) {
	if count := node.getCount(); idx >= count {
		panicOutOfBound(idx, count)
	}
}

const (
	dataOffset  = 8
	childPtrLen = 8
	// within the data part
	kvOffsetOffset = 0
	kvOffsetLen    = 2
	// within a KV pair
	keyLenOffset   = 0
	keyLenLen      = 2
	valueLenOffset = 2
	valueLenLen    = 2
	keyOffset      = 4
)

func (node BTreeNode) getData() []byte {
	return getSlice(node, dataOffset, node.getSize()-dataOffset)
}

func (node BTreeNode) getChildPtr(idx uint16) uint64 {
	node.checkType(internal)
	node.checkIdx(idx)
	return binary.LittleEndian.Uint64(getSlice(node.getData(), idx*childPtrLen, childPtrLen))
}

// TODO: Check data out of 4 KB limitation.
func (node BTreeNode) setChildPtr(idx uint16, ptr uint64) {
	node.checkType(internal)
	node.checkIdx(idx)
	binary.LittleEndian.PutUint64(getSlice(node.getData(), idx*childPtrLen, childPtrLen), ptr)
}

func (node BTreeNode) getKV(idx uint16) ([]byte, []byte) {
	node.checkType(leaf)
	node.checkIdx(idx)

	count := node.getCount()
	data := node.getData()

	kvOffsets := data[:count*kvOffsetLen]
	kvOffset := binary.LittleEndian.Uint16(getSlice(kvOffsets, idx*kvOffsetLen, kvOffsetLen))

	kvPairs := data[count*kvOffsetLen:]
	kvPair := kvPairs[kvOffset:]

	keyLen := binary.LittleEndian.Uint16(getSlice(kvPair, keyLenOffset, keyLenLen))
	valueLen := binary.LittleEndian.Uint16(getSlice(kvPair, valueLenOffset, valueLenLen))
	kvPair = getSlice(kvPair, 0, keyLenLen+valueLenLen+keyLen+valueLen)

	return getSlice(kvPair, keyOffset, keyLen), getSlice(kvPair, keyOffset+keyLen, valueLen)
}
