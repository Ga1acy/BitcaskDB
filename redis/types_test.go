package redis

import (
	bitcask "bitcaskGo"
	"bitcaskGo/utils"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
	"time"
)

func TestRedisDataStructure_Get(t *testing.T) {
	opts := bitcask.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-redit-get")
	opts.DirPath = dir
	rds, err := NewRedisDataStructure(opts)
	assert.Nil(t, err)

	err = rds.Set(utils.GetTestKey(1), 0, utils.RandomValue(100))
	assert.Nil(t, err)
	err = rds.Set(utils.GetTestKey(2), time.Second*5, utils.RandomValue(100))
	assert.Nil(t, err)

	//case1: normal situation
	val1, err := rds.Get(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, val1)
	//t.Log(string(val1))

	//case2: the data is expired
	time.Sleep(time.Second * 6)
	val2, err := rds.Get(utils.GetTestKey(2))
	assert.Nil(t, err)
	assert.Nil(t, val2)

	//case3: the data doesn't exist
	_, err = rds.Get(utils.GetTestKey(3))
	assert.Equal(t, err, bitcask.ErrKeyNotFound)
}

func TestRedisDataStructure_Del(t *testing.T) {
	opts := bitcask.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-redit-del")
	opts.DirPath = dir
	rds, err := NewRedisDataStructure(opts)
	assert.Nil(t, err)

	//case1: delete a key which doesn't exsit
	err = rds.Del(utils.GetTestKey(1))
	assert.Nil(t, err)
	//t.Log(err)

	//case2:delete a key which exist
	err = rds.Set(utils.GetTestKey(2), 0, utils.RandomValue(100))
	assert.Nil(t, err)

	err = rds.Del(utils.GetTestKey(2))
	assert.Nil(t, err)

	_, err = rds.Get(utils.GetTestKey(2))
	//t.Log(err)
	assert.Equal(t, err, bitcask.ErrKeyNotFound)
}

func TestRedisDataStructure_Type(t *testing.T) {
	opts := bitcask.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-redit-del")
	opts.DirPath = dir
	rds, err := NewRedisDataStructure(opts)
	assert.Nil(t, err)

	//case1: get the type of key that doesn't exist
	typ1, err := rds.Type(utils.GetTestKey(1))
	assert.Equal(t, uint8(0), typ1)
	assert.Equal(t, err, bitcask.ErrKeyNotFound)

	//case2: get the type of key that exist
	err = rds.Set(utils.GetTestKey(2), 0, utils.RandomValue(100))
	assert.Nil(t, err)
	typ2, err := rds.Type(utils.GetTestKey(2))
	assert.Nil(t, err)
	assert.Equal(t, String, typ2)
}

func TestRedisDataStructure_HGet(t *testing.T) {
	opts := bitcask.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-redit-get")
	opts.DirPath = dir
	rds, err := NewRedisDataStructure(opts)
	assert.Nil(t, err)

	//set
	ok1, err := rds.HSet(utils.GetTestKey(1), []byte("field1"), utils.RandomValue(100))
	assert.True(t, ok1)
	assert.Nil(t, err)
	v1 := utils.RandomValue(100)

	//set a same key, same field and same value
	ok2, err := rds.HSet(utils.GetTestKey(1), []byte("field1"), v1)
	assert.False(t, ok2)
	assert.Nil(t, err)

	v2 := utils.RandomValue(100)

	ok3, err := rds.HSet(utils.GetTestKey(1), []byte("field2"), v2)
	assert.True(t, ok3)
	assert.Nil(t, err)

	//get
	val1, err := rds.HGet(utils.GetTestKey(1), []byte("field1"))
	assert.Equal(t, val1, v1)
	assert.Nil(t, err)

	val2, err := rds.HGet(utils.GetTestKey(1), []byte("field2"))
	assert.Equal(t, val2, v2)
	assert.Nil(t, err)

	val3, err := rds.HGet(utils.GetTestKey(1), []byte("field-not-exist"))
	assert.Equal(t, err, bitcask.ErrKeyNotFound)
	assert.Nil(t, val3)

}

func TestRedisDataStructure_HDel(t *testing.T) {
	opts := bitcask.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-redit-hdel")
	opts.DirPath = dir
	rds, err := NewRedisDataStructure(opts)
	assert.Nil(t, err)

	//set
	ok1, err := rds.HSet(utils.GetTestKey(1), []byte("field1"), utils.RandomValue(100))
	assert.True(t, ok1)
	assert.Nil(t, err)
	v1 := utils.RandomValue(100)

	ok2, err := rds.HSet(utils.GetTestKey(1), []byte("field1"), v1)
	assert.False(t, ok2)
	assert.Nil(t, err)

	v2 := utils.RandomValue(100)

	ok3, err := rds.HSet(utils.GetTestKey(1), []byte("field2"), v2)
	assert.True(t, ok3)
	assert.Nil(t, err)

	//delete
	//case1: delete a empty key
	del1, err := rds.HDel(utils.GetTestKey(11), nil)
	assert.False(t, del1)

	//case 2: delete a exist value
	del2, err := rds.HDel(utils.GetTestKey(1), []byte("field1"))
	assert.True(t, del2)
	assert.Nil(t, err)

	//check if the delete operation is valid

	val1, err := rds.HGet(utils.GetTestKey(1), []byte("field1"))
	assert.Nil(t, val1)
	assert.Nil(t, err)
}

func TestRedisDataStructure_SIsMember(t *testing.T) {
	opts := bitcask.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-redit-sismember")
	opts.DirPath = dir
	rds, err := NewRedisDataStructure(opts)
	assert.Nil(t, err)

	//test SAdd
	//case1:add 2 same value
	ok, err := rds.SAdd(utils.GetTestKey(1), []byte("val-1"))
	assert.Nil(t, err)
	assert.True(t, ok)
	ok, err = rds.SAdd(utils.GetTestKey(1), []byte("val-1"))
	assert.Nil(t, err)
	assert.False(t, ok)

	//case2:add a different value in the same key
	ok, err = rds.SAdd(utils.GetTestKey(1), []byte("val-2"))
	assert.Nil(t, err)
	assert.True(t, ok)

	//test SIsMember

	//case1: the key and value both doesn't exist
	ok, err = rds.SIsMember(utils.GetTestKey(2), []byte("val-2"))
	assert.Nil(t, err)
	assert.False(t, ok)
	//case2: key and value both exist
	ok, err = rds.SIsMember(utils.GetTestKey(1), []byte("val-1"))
	assert.Nil(t, err)
	assert.True(t, ok)

	ok, err = rds.SIsMember(utils.GetTestKey(1), []byte("val-2"))
	assert.Nil(t, err)
	assert.True(t, ok)

	//case3: key exist but value doesn't
	ok, err = rds.SIsMember(utils.GetTestKey(1), []byte("val-not-exist"))
	assert.Nil(t, err)
	assert.False(t, ok)
}

func TestRedisDataStructure_SRem(t *testing.T) {
	opts := bitcask.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-redit-srem")
	opts.DirPath = dir
	rds, err := NewRedisDataStructure(opts)
	assert.Nil(t, err)

	ok, err := rds.SAdd(utils.GetTestKey(1), []byte("val-1"))
	assert.Nil(t, err)
	assert.True(t, ok)
	ok, err = rds.SAdd(utils.GetTestKey(1), []byte("val-1"))
	assert.Nil(t, err)
	assert.False(t, ok)

	ok, err = rds.SAdd(utils.GetTestKey(1), []byte("val-2"))
	assert.Nil(t, err)
	assert.True(t, ok)

	//test SRem
	//case1 remove a data that both key and value doesn't exist
	ok, err = rds.SRem(utils.GetTestKey(2), []byte("val-1"))
	assert.False(t, ok)
	assert.Nil(t, err)

	//case2 remove data that both key and value exist
	ok, err = rds.SRem(utils.GetTestKey(1), []byte("val-1"))
	assert.True(t, ok)
	assert.Nil(t, err)

	ok, err = rds.SRem(utils.GetTestKey(1), []byte("val-2"))
	assert.True(t, ok)
	assert.Nil(t, err)

}

func TestRedisDataStructure_LPop(t *testing.T) {
	opts := bitcask.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-redit-lpop")
	opts.DirPath = dir
	rds, err := NewRedisDataStructure(opts)
	assert.Nil(t, err)

	res, err := rds.LPush(utils.GetTestKey(1), []byte("val-1"))
	assert.Nil(t, err)
	assert.Equal(t, uint32(1), res)

	//it's another key's list,
	//the res should be 1
	res, err = rds.LPush(utils.GetTestKey(2), []byte("val-2"))
	assert.Nil(t, err)
	assert.Equal(t, uint32(1), res)

	res, err = rds.LPush(utils.GetTestKey(1), []byte("val-1"))
	assert.Nil(t, err)
	assert.Equal(t, uint32(2), res)

	res, err = rds.LPush(utils.GetTestKey(1), []byte("val-2"))
	assert.Nil(t, err)
	assert.Equal(t, uint32(3), res)

	//list1: key1-val2 | key1-val1 | key1-val1
	//list2: key2-val2

	val, err := rds.LPop(utils.GetTestKey(2))
	assert.Nil(t, err)
	assert.Equal(t, "val-2", string(val))

	val, err = rds.LPop(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.Equal(t, "val-2", string(val))

	val, err = rds.LPop(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.Equal(t, "val-1", string(val))

	val, err = rds.LPop(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.Equal(t, "val-1", string(val))

}

func TestRedisDataStructure_RPop(t *testing.T) {
	opts := bitcask.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-redit-rpop")
	opts.DirPath = dir
	rds, err := NewRedisDataStructure(opts)
	assert.Nil(t, err)

	res, err := rds.RPush(utils.GetTestKey(1), []byte("val-1"))
	assert.Nil(t, err)
	assert.Equal(t, uint32(1), res)

	//it's another key's list,
	//the res should be 1
	res, err = rds.RPush(utils.GetTestKey(2), []byte("val-2"))
	assert.Nil(t, err)
	assert.Equal(t, uint32(1), res)

	res, err = rds.RPush(utils.GetTestKey(1), []byte("val-1"))
	assert.Nil(t, err)
	assert.Equal(t, uint32(2), res)

	res, err = rds.RPush(utils.GetTestKey(1), []byte("val-2"))
	assert.Nil(t, err)
	assert.Equal(t, uint32(3), res)

	//list1: key1-val1 | key1-val1 | key1-val2
	//list2: key2-val2

	val, err := rds.RPop(utils.GetTestKey(2))
	assert.Nil(t, err)
	assert.Equal(t, "val-2", string(val))

	val, err = rds.RPop(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.Equal(t, "val-2", string(val))

	val, err = rds.RPop(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.Equal(t, "val-1", string(val))

	val, err = rds.RPop(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.Equal(t, "val-1", string(val))

}
