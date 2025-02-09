package memtable

import (
	"sync"
)

const (
	// if the mutable(1st) skiplist exceeds the threshold,
	// then it will be frozen and a new skiplist will be created.
	skipListThreshold = 256 * 1024 * 1024
)

type memtable struct {
	// sorted by created time desending(the latest one has the index 0)
	skiplists []*Skiplist
	rwMutex   sync.RWMutex
}

func NewMemtable() *memtable {
	return &memtable{
		skiplists: make([]*Skiplist, 0),
	}
}

func (mt *memtable) Get(key string) ([]byte, bool) {
	mt.rwMutex.RLock()
	defer mt.rwMutex.RUnlock()

	for _, st := range mt.skiplists {
		node := st.Get(key)
		if node == nil {
			continue
		}
		return node.GetVal(), true
	}
	return nil, false
}

func (mt *memtable) popSkiplist() {
	len_ := len(mt.skiplists)
	if len_ == 0 {
		return
	}
	mt.skiplists = mt.skiplists[:len_-1]
}

func (mt *memtable) newSkiplist() {
	mt.skiplists = append([]*Skiplist{NewSkipList()}, mt.skiplists...)
}

func (mt *memtable) Update(key string, val []byte) bool {
	mt.rwMutex.Lock()
	defer mt.rwMutex.Unlock()

	if val == nil {
		panic("Nil val")
	}
	if len(mt.skiplists) == 0 || mt.skiplists[0].GetSize() >= skipListThreshold {
		mt.newSkiplist()
	}
	return mt.skiplists[0].Update(key, val)
}

func (mt *memtable) Delete(key string) bool {
	mt.rwMutex.Lock()
	defer mt.rwMutex.Unlock()

	if len(mt.skiplists) == 0 || mt.skiplists[0].GetSize() >= skipListThreshold {
		mt.newSkiplist()
	}
	return mt.skiplists[0].Update(key, nil)
}

type Iterator interface {
	getKey() string
	getVal() string
	next()
	hasNext() bool
}

type MemtableIterator struct {
	st     *Skiplist
	cursor *node
}

func (it *MemtableIterator) getKey() string {
	return it.cursor.key
}

func (it *MemtableIterator) getVal() string {
	return string(it.cursor.val)
}

func (it *MemtableIterator) next() {
	it.cursor = it.cursor.nexts[0]
}

func (it *MemtableIterator) hasNext() bool {
	return it.cursor != nil && it.cursor != it.st.tail
}

func (mt *memtable) getLastIterator() *MemtableIterator {
	if len(mt.skiplists) <= 1 {
		return nil
	}
	lastSkiplist := mt.skiplists[len(mt.skiplists)-1]
	return &MemtableIterator{
		st:     lastSkiplist,
		cursor: lastSkiplist.head.nexts[0],
	}
}
