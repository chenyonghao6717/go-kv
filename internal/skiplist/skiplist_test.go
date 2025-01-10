package skiplist

import (
	"kv/test"
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

func TestUpdateAndGet(t *testing.T) {
	st := NewSkipList()
	strs := test.RandStrs(1000, 100)
	for _, str := range strs {
		st.Update(str, []byte(str))
	}
	for _, str := range strs {
		assert.Equal(t, str, string(st.Get(str).val))
	}
	for _, str := range strs {
		st.Update(str, nil)
	}
	for _, str := range strs {
		assert.Nil(t, st.Get(str).val)
	}
}
