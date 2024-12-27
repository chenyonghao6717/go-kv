package btree

import (
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetTypeLiteral(t *testing.T) {
	assert.Equal(t, typeLiteralInternal, getTypeLiteral(internal))
	assert.Equal(t, typeLiteralLeaf, getTypeLiteral(leaf))
}

func TestGetSlice(t *testing.T) {
	slice := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	offset := uint16(2)
	len := uint16(4)
	expected := []byte{2, 3, 4, 5}
	assert.Equal(t, expected, getSlice(slice, offset, len))
}

func getSliceWithRandomIntegers(size uint16) []byte {
	node := make([]byte, size)
	for i := uint16(0); i < size; i++ {
		node[i] = byte(rand.Intn(int(math.Pow(2, 8))))
	}
	return node
}

func getNode(type_ BTreeNodeType, count uint16, size uint16) BTreeNode {
	node := BTreeNode(getSliceWithRandomIntegers(4096))
	node.setType(type_)
	node.setCount(count)
	node.setSize(size)
	return node
}

func TestSetGetType(t *testing.T) {
	node := getNode(leaf, 0, 10)
	node.setType(leaf)
	assert.Equal(t, leaf, node.getType())
}

func TestSetGetCount(t *testing.T) {
	expectedCount := uint16(10)
	node := getNode(leaf, expectedCount, 10)
	node.setCount(expectedCount)
	assert.Equal(t, expectedCount, node.getCount())
}

func TestSetGetSize(t *testing.T) {
	expectedSize := uint16(10)
	node := getNode(leaf, 0, expectedSize)
	node.setSize(expectedSize)
	assert.Equal(t, expectedSize, node.getSize())
}

func TestCheckType(t *testing.T) {
	expectedType := leaf
	actualType := internal
	node := getNode(actualType, 0, 10)
	assert.PanicsWithValue(
		t,
		fmt.Sprintf(panicTypeMismatchMsg, getTypeLiteral(expectedType), getTypeLiteral(actualType)),
		func() {
			node.checkType(expectedType)
		},
	)

	sameType := actualType
	assert.NotPanics(t, func() { node.checkType(sameType) })
}

func TestCheckIdx(t *testing.T) {
	outOfBoundIdx := uint16(15)
	count := uint16(10)
	node := getNode(leaf, count, 10)
	node.setCount(count)
	assert.PanicsWithValue(
		t,
		fmt.Sprintf(panicOutOfBoundMsg, outOfBoundIdx, count),
		func() {
			node.checkIdx(outOfBoundIdx)
		},
	)

	inBoundIdx := uint16(5)
	assert.NotPanics(t, func() { node.checkIdx(inBoundIdx) })
}

func TestGetData(t *testing.T) {
	size := uint16(20)
	dataOffset := uint16(8)
	node := getNode(internal, 0, size)
	assert.Equal(
		t,
		[]byte(node[dataOffset:size]),
		node.getData(),
	)
}

func TestChildPtr(t *testing.T) {
	idx := uint16(0)
	ptr := uint64(rand.Int63())
	size := uint16(100)
	node := getNode(internal, 2, size)
	node.setChildPtr(idx, ptr)
	assert.Equal(t, ptr, node.getChildPtr(idx))
	node.setChildPtr(idx+1, ptr)
	assert.Equal(t, ptr, node.getChildPtr(idx+1))

	idxOutOfBound := idx + 10
	assert.Panics(t, func() { node.getChildPtr(idxOutOfBound) })
}

func getKV(keyLen uint16, valLen uint16) []byte {
	kv := getSliceWithRandomIntegers(keyLenLen + valueLenLen + keyLen + valLen)
	binary.LittleEndian.PutUint16(kv, keyLen)
	binary.LittleEndian.PutUint16(kv[valueLenOffset:], valLen)
	return kv
}

func TestGetKV(t *testing.T) {
	kvPairsNum := uint16(3)
	keyLens := []uint16{10, 20, 30}
	valLens := []uint16{40, 50, 60}
	kvOffsets := make([]byte, 2*3)

	size := uint16(headerLen + kvPairsNum*kvOffsetLen)
	kvs := [][]byte{}
	for i := uint16(0); i < kvPairsNum; i++ {
		kvs = append(kvs, getKV(keyLens[i], valLens[i]))
		size += uint16(len(kvs[i]))
	}

	binary.LittleEndian.PutUint16(kvOffsets[0*kvOffsetLen:], uint16(0))
	binary.LittleEndian.PutUint16(kvOffsets[1*kvOffsetLen:], uint16(len(kvs[0])))
	binary.LittleEndian.PutUint16(kvOffsets[2*kvOffsetLen:], uint16(len(kvs[0]) + len(kvs[1])))

	header := BTreeNode(make([]byte, 8))
	header.setType(leaf)
	header.setCount(kvPairsNum)
	header.setSize(size)

	node := BTreeNode(append([]byte(header), kvOffsets...))
	for i := uint16(0); i < kvPairsNum; i++ {
		node = append(node, kvs[i]...)
	}

	for i := uint16(0); i < 1; i++ {
		expectedKey := kvs[i][keyLenLen+valueLenLen:][:keyLens[i]]
		expectedVal := kvs[i][keyLenLen+valueLenLen+len(expectedKey):][:valLens[i]]
		actualKey, actualVal := node.getKV(0)
		assert.Equal(t, expectedKey, actualKey)
		assert.Equal(t, expectedVal, actualVal)
	}
}
