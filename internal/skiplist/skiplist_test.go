package skiplist

import (
	"kv/test"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLiftLayers(t *testing.T) {
	testTimes := 1000
	for i := 0; i < testTimes; i++ {
		assert.LessOrEqual(t, liftLayers(), maxHeight)
		assert.GreaterOrEqual(t, liftLayers(), uint8(1))
	}
}

func TestNewSkipList(t *testing.T) {
	st := NewSkipList()

	assert.Equal(t, uint8(len(st.head.nexts)), maxHeight)
	for _, headNext := range st.head.nexts {
		assert.Equal(t, headNext, st.tail)
	}

	assert.Equal(t, uint8(len(st.tail.nexts)), maxHeight)
	for _, tailNext := range st.tail.nexts {
		assert.Nil(t, tailNext)
	}

	assert.Zero(t, st.GetSize())
	assert.True(t, st.IsEmpty())
}

func insert(st *skipList, keys []string, vals []string, wg *sync.WaitGroup) {
	if wg != nil {
		defer wg.Done()
	}
	for i, key := range keys {
		st.Insert(key, vals[i])
	}
}

func delete(st *skipList, keys []string, wg *sync.WaitGroup) {
	if wg != nil {
		defer wg.Done()
	}
	for _, key := range keys {
		st.Delete(key)
	}
}

func TestConcurrentInsert(t *testing.T) {
	st := NewSkipList()
	testTimes := 1_000
	concurrrency_num := 10
	keys := test.RandStrs(testTimes, testTimes)
	vals := test.RandStrs(testTimes, testTimes)

	var wg sync.WaitGroup
	wg.Add(concurrrency_num)

	for i := 0; i < concurrrency_num; i++ {
		go insert(st, keys, vals, &wg)
	}
	wg.Wait()

	for i, key := range keys {
		assert.Equal(t, vals[i], st.Search(key))
	}
}

func TestConcurrentDelete(t *testing.T) {
	st := NewSkipList()
	testTimes := 1_000
	concurrrency_num := 10
	keys := test.RandStrs(testTimes, testTimes)
	vals := test.RandStrs(testTimes, testTimes)

	insert(st, keys, vals, nil)

	var wgDelete sync.WaitGroup
	wgDelete.Add(concurrrency_num)
	for i := 0; i < concurrrency_num; i++ {
		go delete(st, keys, &wgDelete)
	}
	wgDelete.Wait()

	for _, key := range keys {
		assert.Nil(t, st.Search(key))
	}
	assert.Zero(t, st.GetSize())
}
