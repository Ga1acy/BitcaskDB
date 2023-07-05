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
