package btree

import (
	"encoding/binary"
	"fmt"
)

/*
The B+tree structure of LSM-tree.

An internal node layout:
|    header    |          data           |
| type | count | child_pointers | unused |
|  2B  |  2B   |   count * 8B   |  ...   |

A leaf node layout:
|    header    |  metadata   |      data        |
| type | count | kv_offsets  | kv_data | unused |
|  2B  |  2B   | count * 2B  |   ...   |  ...   |
where each kv_offset is the index(relative to the beginning of the data part) of the first byte of a KV pair.

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

// treat nodes of BTrees as binary to simplify loads and dumps with disks
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
	typeOffset  = 0
	typeLen     = 2
	countOffset = 2
	countLen    = 4
)

func (node BTreeNode) getTypeSlice() []byte {
	return node[typeOffset:][:typeLen]
}

func (node BTreeNode) getType() BTreeNodeType {
	return BTreeNodeType(binary.LittleEndian.Uint16(node.getTypeSlice()))
}

func (node BTreeNode) setType(type_ BTreeNodeType) {
	binary.LittleEndian.PutUint16(node.getTypeSlice(), uint16(type_))
}

func (node BTreeNode) getCountSlice() []byte {
	return node[countOffset:][:countLen]
}

func (node BTreeNode) getCount() uint16 {
	return binary.LittleEndian.Uint16(node.getCountSlice())
}

func (node BTreeNode) setCount(count uint16) {
	binary.LittleEndian.PutUint16(node.getCountSlice(), count)
}

const (
	dataStart       = 4
	childPointerLen = 8
)

const (
	metadataOffset = 4
	// within a KV pair
	kvOffsetLen  = 2
	kvKeyLen     = 2
	kvValueLen   = 2
	kvDataOffset = 4
)

const (
	panicTypeMistMatch = "mismatch between type %s and type %s"
	panicOutOfBound    = "index %d out of bound %d"
)

func (node BTreeNode) checkType(type_ BTreeNodeType) {
	if nodeType := node.getType(); nodeType != type_ {
		panic(fmt.Sprintf(panicTypeMistMatch, getTypeLiteral(nodeType), getTypeLiteral(type_)))
	}
}

func (node BTreeNode) checkIdx(idx uint16) {
	if count := node.getCount(); idx >= count {
		panic(fmt.Sprintf(panicOutOfBound, idx, count))
	}
}

func (node BTreeNode) getChildPointer(idx uint16) uint64 {
	node.checkType(internal)
	node.checkIdx(idx)
	data := node[dataStart:]
	return binary.LittleEndian.Uint64(data[idx*childPointerLen:])
}

func (node BTreeNode) getKV(idx uint16) ([]byte, []byte) {
	node.checkType(internal)
	node.checkIdx(idx)

	count := node.getCount()
	metadata := node[metadataOffset:][:kvOffsetLen*count]
	kvOffset := binary.LittleEndian.Uint16(metadata[idx*kvOffsetLen:][:kvOffsetLen])
	data := node[metadataOffset+len(metadata):]
	keyLen := binary.LittleEndian.Uint16(data[kvOffset:][:kvKeyLen])
	valueLen := binary.LittleEndian.Uint16(data[kvOffset+keyLen:][:kvValueLen])
	key := data[kvDataOffset:][:keyLen]
	value := data[kvOffset+keyLen:][:valueLen]
	return key, value
}

