package index

import (
	"bitcaskGo/data"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBtree_Put(t *testing.T) {
	bt := NewBtree()

	test1 := bt.Put(nil, &data.LogRecordPos{FileId: 1, Offset: 100})
	assert.True(t, test1)

	test2 := bt.Put([]byte("chenyi"), &data.LogRecordPos{FileId: 1, Offset: 2})
	assert.True(t, test2)
}

func TestBtree_Get(t *testing.T) {
	bt := NewBtree()
	res1 := bt.Put(nil, &data.LogRecordPos{FileId: 1, Offset: 100})
	assert.True(t, res1)

	test1 := bt.Get(nil)
	assert.Equal(t, uint32(1), test1.FileId)
	assert.Equal(t, int64(100), test1.Offset)

	res2 := bt.Put([]byte("chen"), &data.LogRecordPos{FileId: 1, Offset: 2})
	assert.True(t, res2)

	res3 := bt.Put([]byte("chen"), &data.LogRecordPos{FileId: 1, Offset: 3})
	assert.True(t, res3)

	//test rewrite file  //Offset: 2 -> 3
	test2 := bt.Get([]byte("chen"))
	assert.Equal(t, uint32(1), test2.FileId)
	assert.Equal(t, int64(3), test2.Offset)
}

func TestBtree_Delete(t *testing.T) {
	bt := NewBtree()
	res1 := bt.Put(nil, &data.LogRecordPos{FileId: 1, Offset: 100})
	assert.True(t, res1)

	test1 := bt.Delete(nil)
	assert.True(t, test1)

	res2 := bt.Put([]byte("chenyi"), &data.LogRecordPos{FileId: 22, Offset: 33})
	assert.True(t, res2)

	test2 := bt.Delete([]byte("chenyi"))
	assert.True(t, test2)

}

func TestBTree_Iterator(t *testing.T) {
	bt1 := NewBtree()
	//case1: empty btree
	iter1 := bt1.Iterator(false)
	assert.Equal(t, false, iter1.Valid()) //empty iterator, thus the valid funciton return false

	//case2:btree have 1 data
	bt1.Put([]byte("test1"), &data.LogRecordPos{
		FileId: 1,
		Offset: 1,
	})
	iter2 := bt1.Iterator(false)
	//when the iterators pointer points to the only one data
	assert.NotNil(t, iter2.Key())
	assert.NotNil(t, iter2.Value())
	assert.Equal(t, true, iter2.Valid())
	iter2.Next()
	//because there is only one data,
	//after Next() operation, valid() should return false now
	assert.Equal(t, false, iter2.Valid())

	//case3: btree have more than one data
	bt1.Put([]byte("test2"), &data.LogRecordPos{
		FileId: 1,
		Offset: 1,
	})
	bt1.Put([]byte("test3"), &data.LogRecordPos{
		FileId: 1,
		Offset: 1,
	})
	bt1.Put([]byte("test4"), &data.LogRecordPos{
		FileId: 1,
		Offset: 1,
	})
	iter3 := bt1.Iterator(false)
	for iter3.Rewind(); iter3.Valid(); iter3.Next() {
		//t.Log("key = ", string(iter3.Key()))   check the output
		assert.NotNil(t, iter3.Key())
	}
	iter4 := bt1.Iterator(true) //reverse
	for iter4.Rewind(); iter4.Valid(); iter4.Next() {
		//t.Log("key = ", string(iter4.Key()))   check the output
		assert.NotNil(t, iter4.Key())
	}
	//test seek function
	iter5 := bt1.Iterator(false)
	for iter5.Seek([]byte("test2")); iter5.Valid(); iter5.Next() {
		//t.Log(string(iter5.Key()))  check the output
		assert.NotNil(t, iter5.Key())
	}
	iter6 := bt1.Iterator(true)
	for iter6.Seek([]byte("test4")); iter6.Valid(); iter6.Next() {
		//t.Log(string(iter6.Key())) //check the output
		assert.NotNil(t, iter6.Key())
	}
}
