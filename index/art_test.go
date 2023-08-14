package index

import (
	"bitcaskGo/data"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestAdaptiveRadixTree_Put(t *testing.T) {
	art := NewART()
	test1 := art.Put([]byte("test-1"), &data.LogRecordPos{
		FileId: 1,
		Offset: 10,
	})

	assert.Nil(t, test1)
	test2 := art.Put([]byte("test-2"), &data.LogRecordPos{
		FileId: 1,
		Offset: 20,
	})

	assert.Nil(t, test2)
	test3 := art.Put([]byte("test-3"), &data.LogRecordPos{
		FileId: 1,
		Offset: 30,
	})

	assert.Nil(t, test3)

	test4 := art.Put([]byte("test-3"), &data.LogRecordPos{
		FileId: 10,
		Offset: 33,
	})
	assert.Equal(t, test4.Offset, int64(30))
	assert.Equal(t, test4.FileId, uint32(1))
}

func TestAdaptiveRadixTree_Get(t *testing.T) {
	art := NewART()
	art.Put([]byte("test-1"), &data.LogRecordPos{
		FileId: 1,
		Offset: 10,
	})

	pos := art.Get([]byte("test-1"))
	//t.Log(pos)
	assert.NotNil(t, pos)

	art.Put([]byte("test-2"), &data.LogRecordPos{
		FileId: 1,
		Offset: 20,
	})

	pos = art.Get([]byte("test-2"))
	//t.Log(pos)
	assert.NotNil(t, pos)

	art.Put([]byte("test-3"), &data.LogRecordPos{
		FileId: 1,
		Offset: 30,
	})

	pos = art.Get([]byte("test-3"))
	t.Log(pos)
	assert.NotNil(t, pos)

	art.Put([]byte("test-3"), &data.LogRecordPos{
		FileId: 1111,
		Offset: 3333,
	})

	pos = art.Get([]byte("test-3"))
	t.Log(pos)
	assert.NotNil(t, pos)

	pos = art.Get([]byte("not exist"))
	t.Log(pos)
	assert.Nil(t, pos)
}

func TestAdaptiveRadixTree_Delete(t *testing.T) {
	art := NewART()
	res1, success1 := art.Delete([]byte("not exist"))
	//t.Log(success)
	assert.Nil(t, res1)
	assert.False(t, success1)
	test1 := art.Put([]byte("test-1"), &data.LogRecordPos{
		FileId: 1,
		Offset: 10,
	})
	assert.Nil(t, test1)

	res2, success2 := art.Delete([]byte("test-1"))
	//t.Log(success)
	assert.Equal(t, res2.FileId, uint32(1))
	assert.Equal(t, res2.Offset, int64(10))
	assert.True(t, success2)

	pos := art.Get([]byte("test-1"))
	//t.Log(pos)
	assert.Nil(t, pos)
}

func TestAdaptiveRadixTree_Size(t *testing.T) {
	art := NewART()
	//empty now
	size := art.Size()
	//t.Log(size)
	assert.Equal(t, 0, size)

	art.Put([]byte("test-1"), &data.LogRecordPos{
		FileId: 1,
		Offset: 10,
	})

	art.Put([]byte("test-2"), &data.LogRecordPos{
		FileId: 1,
		Offset: 20,
	})

	//rewrite a data, size should remain the same
	art.Put([]byte("test-1"), &data.LogRecordPos{
		FileId: 1,
		Offset: 30,
	})

	size = art.Size()
	//t.Log(size)
	assert.Equal(t, 2, size)
}

func TestAdaptiveRadixTree_Iterator(t *testing.T) {
	art := NewART()

	art.Put([]byte("test1"), &data.LogRecordPos{FileId: 1, Offset: 10})
	art.Put([]byte("test2"), &data.LogRecordPos{FileId: 1, Offset: 20})
	art.Put([]byte("test3"), &data.LogRecordPos{FileId: 1, Offset: 30})
	iterator := art.Iterator(false)
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		assert.NotNil(t, iterator.Key())
		assert.NotNil(t, iterator.Value())
	}
}
