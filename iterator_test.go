package bitcaskGo

import (
	"bitcaskGo/utils"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestDB_NewIterator(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-iterator-1")
	opts.DirPath = dir
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	iterator := db.NewIterator(DefaultIteratorOptions)
	defer iterator.Close()
	assert.NotNil(t, iterator)
	assert.Equal(t, false, iterator.Valid())
}

func TestDB_NewIterator_One_Data(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-iterator-2")
	opts.DirPath = dir
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put(utils.GetTestKey(10), utils.GetTestKey(10))
	assert.Nil(t, err)
	iterator := db.NewIterator(DefaultIteratorOptions)
	defer iterator.Close()
	assert.NotNil(t, iterator)
	assert.Equal(t, true, iterator.Valid())
	val, err := iterator.Value()
	assert.Nil(t, err)
	assert.Equal(t, utils.GetTestKey(10), iterator.Key())
	assert.Equal(t, utils.GetTestKey(10), val)
}

func TestDB_NewIterator_Multi_Values(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-iterator1-3")
	opts.DirPath = dir
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put([]byte("test1"), utils.RandomValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("test2"), utils.RandomValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("test3"), utils.RandomValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("test4"), utils.RandomValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("test5"), utils.RandomValue(10))
	assert.Nil(t, err)
	//normal case
	iterator1 := db.NewIterator(DefaultIteratorOptions)
	defer iterator1.Close()
	for iterator1.Rewind(); iterator1.Valid(); iterator1.Next() {
		//t.Log(string(iterator1.Key()))  check the output
		assert.NotNil(t, iterator1.Key())
	}

	//test seek function
	iterator1.Rewind()
	for iterator1.Seek([]byte("test2")); iterator1.Valid(); iterator1.Next() {
		//t.Log(string(iterator1.Key()))  check the output
		assert.NotNil(t, iterator1.Key())
	}

	//reverse traversal case
	reverseOpts := DefaultIteratorOptions
	reverseOpts.Reverse = true
	iterator2 := db.NewIterator(reverseOpts)
	defer iterator2.Close()
	for iterator2.Rewind(); iterator2.Valid(); iterator2.Next() {
		//t.Log(string(iterator2.Key()))
		assert.NotNil(t, iterator2.Key())
	}

	//test seek function
	iterator2.Rewind()
	for iterator2.Seek([]byte("test2")); iterator2.Valid(); iterator2.Next() {
		//t.Log(string(iterator2.Key()))
		assert.NotNil(t, iterator2.Key())
	}

}

func TestDB_NewIterator_Multi_Values_Prefix(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-iterator1-4")
	opts.DirPath = dir
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put([]byte("teast1"), utils.RandomValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("tebst2"), utils.RandomValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("tecst3"), utils.RandomValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("tedst4"), utils.RandomValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("teest5"), utils.RandomValue(10))
	assert.Nil(t, err)

	preFixOpts := DefaultIteratorOptions
	preFixOpts.Prefix = []byte("teb")
	iterator := db.NewIterator(preFixOpts)
	defer iterator.Close()
	//Rewind, Next have skipToNext function
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		//t.Log(string(iterator.Key()))   check the output
		assert.NotNil(t, iterator.Key())
	}

	//test Rewind's skipToNext along
	iterator.Rewind()
	//t.Log(string(iterator.Key())) check the output
	assert.NotNil(t, iterator.Key())
}
