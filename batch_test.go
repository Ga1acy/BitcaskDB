package bitcaskGo

import (
	"bitcaskGo/utils"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

// test some normal function
func TestDB_WriteBatch1(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-batch-1")
	opts.DirPath = dir
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	wb1 := db.NewWriteBatch(DefaultWriteBatchOptions)
	err = wb1.Put(utils.GetTestKey(1), utils.RandomValue(10))
	assert.Nil(t, err)
	err = wb1.Delete(utils.GetTestKey(2))
	assert.Nil(t, err)

	//case1:before commit
	//WriteBatch put data into pendingWrites, but not yet commit
	//so there is no date in the disk
	_, err = db.Get(utils.GetTestKey(1))
	//t.Log(err)
	assert.Equal(t, ErrKeyNotFound, err)

	//case2:after commit
	err = wb1.Commit()
	assert.Nil(t, err)

	val1, err := db.Get(utils.GetTestKey(1))
	assert.NotNil(t, val1)
	assert.Nil(t, err)

	wb2 := db.NewWriteBatch(DefaultWriteBatchOptions)

	err = wb2.Delete(utils.GetTestKey(1))
	assert.Nil(t, err)

	err = wb2.Commit()
	assert.Nil(t, err)

	val2, err := db.Get(utils.GetTestKey(1))
	assert.Nil(t, val2)
	assert.Equal(t, ErrKeyNotFound, err)
}

// test the situation of restart the database
func TestDB_WriteBatch2(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-batch-2")
	opts.DirPath = dir
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	//put a actual data
	err = db.Put(utils.GetTestKey(1), utils.RandomValue(10))
	assert.Nil(t, err)

	wb1 := db.NewWriteBatch(DefaultWriteBatchOptions)

	err = wb1.Put(utils.GetTestKey(2), utils.RandomValue(10))
	assert.Nil(t, err)
	err = wb1.Delete(utils.GetTestKey(1)) //delete the actual data
	assert.Nil(t, err)

	err = wb1.Commit()
	assert.Nil(t, err)

	err = wb1.Put(utils.GetTestKey(3), utils.RandomValue(10))
	assert.Nil(t, err)
	err = wb1.Commit()
	assert.Nil(t, err)

	//restart the database
	err = db.Close()
	assert.Nil(t, err)

	db1, err := Open(opts)
	assert.Nil(t, err)

	//key1 should be deleted, and key2 should be committed
	val1, err := db1.Get(utils.GetTestKey(1))
	assert.Nil(t, val1)
	assert.Equal(t, ErrKeyNotFound, err)

	val2, err := db1.Get(utils.GetTestKey(2))
	assert.NotNil(t, val2)
	assert.Nil(t, err)

	//check the if the seqNo add correctly
	//t.Log(db.seqNo)
	assert.Equal(t, uint64(2), db.seqNo)
}

// test shut down the transaction which writes a batch of data,
// and see if those data is in the disk
func TestDB_WriteBatch3(t *testing.T) {
	opts := DefaultOptions
	//dir, _ := os.MkdirTemp("", "bitcask-go-batch-2")
	dir := "/tmp/bitcask-go-batch-4"
	opts.DirPath = dir
	db, err := Open(opts)
	//defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	//first: writing data to buffer, but not yet commit
	//we need to shut it down when writing

	//second: note all these codes

	//wbOpts := DefaultWriteBatchOptions
	//wbOpts.MaxBatchNum = 10000000
	//wb := db.NewWriteBatch(wbOpts)
	//for i := 0; i < 500000; i++ {
	//	err = wb.Put(utils.GetTestKey(i), utils.RandomValue(1024))
	//	assert.Nil(t, err)
	//}
	//
	//err = wb.Commit()
	//assert.Nil(t, err)

	//third:check the data file
	//result:there should be some data in the data file

	//fourth:check if those data is written to the database
	//the result should be 0
	keys := db.ListKeys()
	//t.Log(len(keys))
	assert.Equal(t, 0, len(keys))
}
