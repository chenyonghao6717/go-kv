package memtable

import (
	"math/rand"
	"sync"
)

/*
The non-thread safe implementation of skip list. Extra synchronization is needed.
The skiplist only supports insertions and updates. Deletions can be done by marked
as deleted(update the value to nil).
Using 0-1 random numbers to determine whether a node needs to be lifted.
*/

const (
	maxHeight = uint8(16)
)

/*
This function returns the overall layer number that a node can be lifted to.
It ranges from 1 to maxHeight(both inclusively).
A node at each layer has 1/2 of possibility to be lifted to the upper layer.
So a node has (1/2) ^ 15 of possibility to be lifted to the uppermost layer.
*/
func liftLayers() uint8 {
	layer := uint8(1)
	for ; layer < maxHeight; layer++ {
		if rand.Intn(2) == 0 {
			break
		}
	}
	return layer
}

type node struct {
	nexts []*node
	key   string
	val   []byte
}

func (n *node) GetVal() []byte {
	return n.val
}

type Skiplist struct {
	head, tail *node
	// only count non-nil KV pairs
	len uint32
	// the byte size occupied by all KV pairs
	size    uint32
	rwMutex sync.RWMutex
}

func NewSkipList() *Skiplist {
	head := node{nexts: make([]*node, maxHeight)}
	tail := node{nexts: make([]*node, maxHeight)}
	for i := uint8(0); i < maxHeight; i++ {
		head.nexts[i] = &tail
	}
	return &Skiplist{head: &head, tail: &tail, size: 0}
}

func newNode(key string, val []byte, layerNum uint8) *node {
	nexts := make([]*node, layerNum)
	return &node{
		nexts: nexts,
		key:   key,
		val:   val,
	}
}

func (st *Skiplist) GetLen() uint32 {
	st.rwMutex.RLock()
	defer st.rwMutex.RUnlock()
	return uint32(st.len)
}

func (st *Skiplist) GetSize() uint32 {
	st.rwMutex.RLock()
	defer st.rwMutex.RUnlock()
	return uint32(st.size)
}

func (st *Skiplist) IsEmpty() bool {
	st.rwMutex.RLock()
	defer st.rwMutex.RUnlock()

	return st.size == 0
}

func initBound(initNode *node) []*node {
	bounds := make([]*node, maxHeight)
	for i := uint8(0); i < maxHeight; i++ {
		bounds[i] = initNode
	}
	return bounds
}

func narrowDownBound(head *node, tail *node, leftBound *node, rightBound *node, key string, layer uint8) (*node, *node) {
	cur := leftBound
	next := cur.nexts[layer]
	for next != rightBound {
		if (cur == head || cur.key <= key) && (next == tail || next.key >= key) {
			break
		}
		cur = next
		next = next.nexts[layer]
	}
	return cur, next
}

/*
For each layer, we inherit the 2 bound nodes from the upper layer. A lower layer may
have more nodes than the upper layer within the same bound so we traverse the current
layer to try to narrow down the bound till we meet the lowest layer. We use two nodes
slices to track bound nodes for each layers. This method is also useful for insert nodes.
If a newly inserted node is lifted to the nth layer, we can have the nth left bound node
point to the node and have the node point to the right bound.
*/
func (st *Skiplist) searchBounds(key string) ([]*node, []*node) {
	leftBounds := initBound(st.head)
	rightBounds := initBound(st.tail)

	if st.size == uint32(0) {
		return leftBounds, rightBounds
	}

	leftBounds[maxHeight-1] = st.head
	rightBounds[maxHeight-1] = st.head.nexts[maxHeight-1]

	for i := maxHeight - 1; ; i-- {
		leftBound, rightBound := narrowDownBound(st.head, st.tail, leftBounds[i], rightBounds[i], key, i)
		leftBounds[i] = leftBound
		rightBounds[i] = rightBound
		if i == 0 {
			break
		}
		leftBounds[i-1] = leftBound
		rightBounds[i-1] = rightBound
	}

	leftBound, rightBound := narrowDownBound(st.head, st.tail, leftBounds[0], rightBounds[0], key, 0)
	leftBounds[0] = leftBound
	rightBounds[0] = rightBound

	return leftBounds, rightBounds
}

func (st *Skiplist) searchWithBounds(key string, leftBounds []*node, rightBounds []*node) *node {
	if leftBounds[0] != st.head && leftBounds[0].key == key {
		return leftBounds[0]
	}
	if rightBounds[0] != st.head && rightBounds[0].key == key {
		return rightBounds[0]
	}
	return nil
}

func (st *Skiplist) Get(key string) *node {
	leftBounds, rightBounds := st.searchBounds(key)
	return st.searchWithBounds(key, leftBounds, rightBounds)
}

/*
Serve as both insert and update.
*/
func (st *Skiplist) Update(key string, val []byte) bool {
	leftBounds, rightBounds := st.searchBounds(key)
	node := st.searchWithBounds(key, leftBounds, rightBounds)
	if node != nil {
		// the node was marked as deleted before
		if node.val == nil {
			st.len += 1
			st.size += uint32(len(val))
		} else {
			st.size -= uint32(len(node.val))
			st.size += uint32(len(val))
			node.val = val
		}
		return true
	}
	layerNum := liftLayers()
	node = newNode(key, val, layerNum)
	for i := uint8(0); i < layerNum; i++ {
		leftBounds[i].nexts[i] = node
		node.nexts[i] = rightBounds[i]
	}
	st.len += 1
	st.size += uint32(len(key) + len(val))
	return true
}
