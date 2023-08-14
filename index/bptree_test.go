package index

import (
	"bitcaskGo/data"
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"testing"
)

func TestBPlusTree_Put(t *testing.T) {
	path := filepath.Join(os.TempDir(), "bptree-put")
	_ = os.MkdirAll(path, os.ModePerm)

	defer func() {
		_ = os.RemoveAll(path)
	}()

	bptree := NewBPlusTree(path, false)
	res1 := bptree.Put([]byte("test1"), &data.LogRecordPos{FileId: 1, Offset: 10})
	assert.Nil(t, res1)

	res2 := bptree.Put([]byte("test2"), &data.LogRecordPos{FileId: 1, Offset: 20})
	assert.Nil(t, res2)

	res3 := bptree.Put([]byte("test3"), &data.LogRecordPos{FileId: 1, Offset: 30})
	assert.Nil(t, res3)

	res4 := bptree.Put([]byte("test3"), &data.LogRecordPos{FileId: 3, Offset: 33})
	assert.Equal(t, res4.FileId, uint32(1))
	assert.Equal(t, res4.Offset, int64(30))

}

func TestBPlusTree_Get(t *testing.T) {
	path := filepath.Join(os.TempDir(), "bptree-get")
	_ = os.MkdirAll(path, os.ModePerm)

	defer func() {
		_ = os.RemoveAll(path)
	}()

	bptree := NewBPlusTree(path, false)

	//get the empty value
	pos := bptree.Get([]byte("not exist"))
	//t.Log(pos)
	assert.Nil(t, pos)

	bptree.Put([]byte("test1"), &data.LogRecordPos{FileId: 1, Offset: 10})
	pos1 := bptree.Get([]byte("test1"))
	//t.Log(pos1)
	assert.NotNil(t, pos1)

	bptree.Put([]byte("test2"), &data.LogRecordPos{FileId: 1, Offset: 20})
	pos2 := bptree.Get([]byte("test2"))
	//t.Log(pos2)
	assert.NotNil(t, pos2)

	bptree.Put([]byte("test3"), &data.LogRecordPos{FileId: 1, Offset: 30})
	pos3 := bptree.Get([]byte("test3"))
	//t.Log(pos3)
	assert.NotNil(t, pos3)

	//update the value
	bptree.Put([]byte("test3"), &data.LogRecordPos{FileId: 1, Offset: 333})
	pos4 := bptree.Get([]byte("test3"))
	//t.Log(pos4)
	assert.NotNil(t, pos4)
}

func TestBPlusTree_Delete(t *testing.T) {
	path := filepath.Join(os.TempDir(), "bptree-delete")
	_ = os.MkdirAll(path, os.ModePerm)

	defer func() {
		_ = os.RemoveAll(path)
	}()

	bptree := NewBPlusTree(path, false)

	//delete a empty value
	res1, ok1 := bptree.Delete([]byte("not exist"))
	t.Log(res1)
	t.Log(ok1)
	//assert.Nil(t, res1)
	//assert.False(t, ok1)
	//t.Log(pos)

	bptree.Put([]byte("test1"), &data.LogRecordPos{FileId: 1, Offset: 10})
	res2, ok2 := bptree.Delete([]byte("test1"))
	//t.Log(res2)
	assert.Equal(t, res2.FileId, uint32(1))
	assert.Equal(t, res2.Offset, int64(10))
	assert.True(t, ok2)

	pos1 := bptree.Get([]byte("test1"))
	//t.Log(pos1)
	assert.Nil(t, pos1)
}

func TestBPlusTree_Size(t *testing.T) {
	path := filepath.Join(os.TempDir(), "bptree-delete")
	_ = os.MkdirAll(path, os.ModePerm)

	defer func() {
		_ = os.RemoveAll(path)
	}()

	bptree := NewBPlusTree(path, false)

	//get an empty size
	size1 := bptree.Size()
	t.Log(size1)
	assert.Equal(t, 0, size1)

	bptree.Put([]byte("test1"), &data.LogRecordPos{FileId: 1, Offset: 10})

	bptree.Put([]byte("test2"), &data.LogRecordPos{FileId: 1, Offset: 20})

	size2 := bptree.Size()
	//t.Log(size2)
	assert.Equal(t, 2, size2)

	//update a key and check the size
	bptree.Put([]byte("test2"), &data.LogRecordPos{FileId: 1, Offset: 30})

	size3 := bptree.Size()
	assert.Equal(t, size2, size3)
}

func TestBPlusTree_Iterator(t *testing.T) {
	path := filepath.Join(os.TempDir(), "bptree-iter")
	_ = os.MkdirAll(path, os.ModePerm)

	defer func() {
		_ = os.RemoveAll(path)
	}()

	bptree := NewBPlusTree(path, false)

	bptree.Put([]byte("a"), &data.LogRecordPos{FileId: 1, Offset: 10})

	bptree.Put([]byte("b"), &data.LogRecordPos{FileId: 1, Offset: 10})

	bptree.Put([]byte("c"), &data.LogRecordPos{FileId: 1, Offset: 10})

	iter := bptree.Iterator(false)
	//iter := bptree.Iterator(true)

	for iter.Rewind(); iter.Valid(); iter.Next() {
		//t.Log(string(iter.Key()))
		assert.NotNil(t, iter.Key())
		assert.NotNil(t, iter.Value())
	}
}
