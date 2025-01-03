package skiplist

import (
	"github.com/stretchr/testify/assert"
	"kv/test"
	"testing"
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

func TestInsert(t *testing.T) {
	st := NewSkipList()
	testTimes := 1_000
	keys := test.RandStrs(testTimes, testTimes)
	vals := test.RandStrs(testTimes, testTimes)
	for i, key := range keys {
		st.Insert(key, vals[i])
	}
	for i, key := range keys {
		assert.Equal(t, vals[i], st.Search(key))
	}
	assert.Equal(t, uint64(len(keys)), st.GetSize())
}

func TestDelete(t *testing.T) {
	st := NewSkipList()
	testTimes := 1_000
	keys := test.RandStrs(testTimes, testTimes)
	vals := test.RandStrs(testTimes, testTimes)
	for i, key := range keys {
		st.Insert(key, vals[i])
	}
	for _, key := range keys {
		st.Delete(key)
	}
	assert.Equal(t, uint64(0), st.GetSize())
	for _, key := range keys {
		assert.Nil(t, st.Search(key))
	}
}
