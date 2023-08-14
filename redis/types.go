package redis

import (
	"bitcaskGo"
	"bitcaskGo/utils"
	"encoding/binary"
	"errors"
	"time"
)

type redisDataType = byte

var (
	ErrWrongTypeOperation = errors.New("WRONGTYPE Operation against the key holding the wrong kind of value")
)

const (
	String redisDataType = iota
	Hash
	Set
	List
	ZSet
)

type RedisDataStructure struct {
	db *bitcaskGo.DB
}

func NewRedisDataStructure(options bitcaskGo.Options) (*RedisDataStructure, error) {
	db, err := bitcaskGo.Open(options)
	if err != nil {
		return nil, err
	}
	return &RedisDataStructure{db: db}, nil
}

//====================== String data structure ======================

func (rds *RedisDataStructure) Set(key []byte, ttl time.Duration, value []byte) error {
	if value == nil {
		return nil
	}

	//encode value : type + expire + payload
	//type take 1 byte
	buf := make([]byte, binary.MaxVarintLen64+1)
	buf[0] = String
	var index = 1

	//ttl:time to live,
	//if we set the ttl,means that this data can only survive this time,
	//thus when ttl != 0, we need to add the ttl to expire,
	//expire is a timestamp, means that when we reach that time,
	//this data is expired
	var expire int64 = 0
	if ttl != 0 {
		expire = time.Now().Add(ttl).UnixNano()
	}
	//index is the length of type and expire so far
	index += binary.PutVarint(buf[index:], expire)
	encValue := make([]byte, index+len(value))

	copy(encValue[:index], buf[:index])
	//now encValue have type, expire and payload
	copy(encValue[index:], value)

	//call the pur interface to write data
	return rds.db.Put(key, encValue)
}

func (rds *RedisDataStructure) Get(key []byte) ([]byte, error) {
	encValue, err := rds.db.Get(key)
	if err != nil {
		return nil, err
	}

	//decode
	dataType := encValue[0]
	if dataType != String {
		return nil, ErrWrongTypeOperation
	}
	var index = 1
	expire, n := binary.Varint(encValue[index:])
	index += n

	//check if it's expired
	if expire > 0 && expire <= time.Now().UnixNano() {
		return nil, nil
	}

	return encValue[index:], nil
}

// ====================== Hash data structure ======================

// HSet set a k-v value in database as the type of hash
func (rds *RedisDataStructure) HSet(key, field, value []byte) (bool, error) {
	metadata, err := rds.findMetadata(key, Hash)
	if err != nil {
		return false, err
	}

	//construct a key that belongs hash data part
	//in other words, construct a hashInternalKey
	hik := &hashInternalKey{
		key:     key,
		version: metadata.version,
		field:   field,
	}
	encKey := hik.encode()

	//find if it exists first
	var exist = true
	if _, err = rds.db.Get(encKey); err == bitcaskGo.ErrKeyNotFound {
		exist = false
	}

	wb := rds.db.NewWriteBatch(bitcaskGo.DefaultWriteBatchOptions)
	//if it doesn't exist,update the size in metadata
	if !exist {
		metadata.size++
		_ = wb.Put(key, metadata.encode())
	}
	_ = wb.Put(encKey, value)
	if err = wb.Commit(); err != nil {
		return false, err
	}
	return !exist, nil
}

// HGet get the value through hik,
// hik is constructed by using metadata
func (rds *RedisDataStructure) HGet(key, field []byte) ([]byte, error) {
	metadata, err := rds.findMetadata(key, Hash)
	if err != nil {
		return nil, err
	}

	//if size is 0, means that there is no data under this key
	if metadata.size == 0 {
		return nil, nil
	}

	hik := &hashInternalKey{
		key:     key,
		version: metadata.version,
		field:   field,
	}

	return rds.db.Get(hik.encode())
}

func (rds *RedisDataStructure) HDel(key, field []byte) (bool, error) {
	//key is use for getting the metadata
	metadata, err := rds.findMetadata(key, Hash)
	if err != nil {
		return false, err
	}

	//if size is 0, means that there is no data under this key
	if metadata.size == 0 {
		return false, nil
	}

	hik := &hashInternalKey{
		key:     key,
		version: metadata.version,
		field:   field,
	}

	encKey := hik.encode()

	//find if it exists first
	var exist = true
	if _, err = rds.db.Get(encKey); err == bitcaskGo.ErrKeyNotFound {
		exist = false
	}

	//only when it exists, can it be deleted
	if exist {
		wb := rds.db.NewWriteBatch(bitcaskGo.DefaultWriteBatchOptions)
		metadata.size--
		_ = wb.Put(key, metadata.encode())
		_ = wb.Delete(key)
		if err = wb.Commit(); err != nil {
			return false, err
		}
	}
	return exist, nil
}

// ====================== Set data structure ======================

func (rds *RedisDataStructure) SAdd(key, member []byte) (bool, error) {
	metadata, err := rds.findMetadata(key, Set)
	if err != nil {
		return false, err
	}

	//construct a setInternalKey
	sik := &setInternalKey{
		key:     key,
		version: metadata.version,
		member:  member,
	}

	var success bool
	//check if the sik exists
	if _, err = rds.db.Get(sik.encode()); err == bitcaskGo.ErrKeyNotFound {
		//if the sik doesn't exist, construct a new one
		wb := rds.db.NewWriteBatch(bitcaskGo.DefaultWriteBatchOptions)
		metadata.size++
		_ = wb.Put(key, metadata.encode())
		_ = wb.Put(sik.encode(), nil)
		if err = wb.Commit(); err != nil {
			return false, err
		}
		success = true
	}
	return success, nil
}

func (rds *RedisDataStructure) SIsMember(key, member []byte) (bool, error) {
	metadata, err := rds.findMetadata(key, Set)
	if err != nil {
		return false, err
	}

	if metadata.size == 0 {
		return false, err
	}

	//construct a setInternalKey
	sik := &setInternalKey{
		key:     key,
		version: metadata.version,
		member:  member,
	}

	_, err = rds.db.Get(sik.encode())
	if err != nil && err != bitcaskGo.ErrKeyNotFound {
		return false, err
	}
	if err == bitcaskGo.ErrKeyNotFound {
		return false, nil
	}
	return true, nil
}

// SRem S Remove, remove some data under an key
func (rds *RedisDataStructure) SRem(key, member []byte) (bool, error) {
	metadata, err := rds.findMetadata(key, Set)
	if err != nil {
		return false, err
	}

	if metadata.size == 0 {
		return false, err
	}

	//construct a setInternalKey
	sik := &setInternalKey{
		key:     key,
		version: metadata.version,
		member:  member,
	}

	if _, err = rds.db.Get(sik.encode()); err == bitcaskGo.ErrKeyNotFound {
		return false, nil
	}

	//update
	wb := rds.db.NewWriteBatch(bitcaskGo.DefaultWriteBatchOptions)
	metadata.size--
	_ = wb.Put(key, metadata.encode())
	_ = wb.Delete(sik.encode())
	if err = wb.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

// ====================== List data structure ======================

func (rds *RedisDataStructure) LPush(key, element []byte) (uint32, error) {
	return rds.pushInner(key, element, true)
}

func (rds *RedisDataStructure) RPush(key, element []byte) (uint32, error) {
	return rds.pushInner(key, element, false)
}

func (rds *RedisDataStructure) LPop(key []byte) ([]byte, error) {
	return rds.popInner(key, true)
}

func (rds *RedisDataStructure) RPop(key []byte) ([]byte, error) {
	return rds.popInner(key, false)
}

// return how many data in the argument key's list
func (rds *RedisDataStructure) pushInner(key, element []byte, isLeft bool) (uint32, error) {
	metadata, err := rds.findMetadata(key, List)
	if err != nil {
		return 0, err
	}

	//construct listInnerKey
	lik := &listInternalKey{
		key:     key,
		version: metadata.version,
	}
	if isLeft {
		lik.index = metadata.head - 1
	} else {
		lik.index = metadata.tail
	}

	//update the metadata and data part
	wb := rds.db.NewWriteBatch(bitcaskGo.DefaultWriteBatchOptions)
	metadata.size++
	if isLeft {
		//push from left
		metadata.head--
	} else {
		//push from right
		metadata.tail++
	}
	_ = wb.Put(key, metadata.encode())
	_ = wb.Put(lik.encode(), element)
	if err = wb.Commit(); err != nil {
		return 0, err
	}

	return metadata.size, nil
}

func (rds *RedisDataStructure) popInner(key []byte, isLeft bool) ([]byte, error) {
	metadata, err := rds.findMetadata(key, List)
	if err != nil {
		return nil, err
	}

	if metadata.size == 0 {
		return nil, nil
	}
	//construct listInnerKey
	lik := &listInternalKey{
		key:     key,
		version: metadata.version,
	}
	if isLeft {
		lik.index = metadata.head
	} else {
		lik.index = metadata.tail - 1
	}

	element, err := rds.db.Get(lik.encode())
	if err != nil {
		return nil, err
	}

	//update the metadata
	metadata.size--
	if isLeft {
		metadata.head++
	} else {
		metadata.tail--
	}
	if err = rds.db.Put(key, metadata.encode()); err != nil {
		return nil, err
	}

	return element, nil
}

// ====================== ZSet data structure ======================

func (rds *RedisDataStructure) ZAdd(key []byte, score float64, member []byte) (bool, error) {
	metadata, err := rds.findMetadata(key, ZSet)
	if err != nil {
		return false, err
	}

	//construct zsetInnerKey
	zik := &zsetInternalKey{
		key:     key,
		version: metadata.version,
		member:  member,
		score:   score,
	}

	//check if the zik exist
	var exist = true
	value, err := rds.db.Get(zik.encodeWithMember())
	if err != nil && err != bitcaskGo.ErrKeyNotFound {
		return false, err
	}
	if err == bitcaskGo.ErrKeyNotFound {
		exist = false
	}

	if exist {
		//if it's same, return directly
		if score == utils.BytesToFloat64(value) {
			return false, err
		}
	}

	wb := rds.db.NewWriteBatch(bitcaskGo.DefaultWriteBatchOptions)
	if !exist {
		metadata.size++
		_ = wb.Put(key, metadata.encode())
	}
	if exist {
		oldKey := &zsetInternalKey{
			key:     key,
			version: metadata.version,
			member:  member,
			score:   utils.BytesToFloat64(value),
		}
		_ = wb.Delete(oldKey.encodeWithScore())
	}
	_ = wb.Put(zik.encodeWithMember(), utils.Float64ToBytes(score))
	_ = wb.Put(zik.encodeWithScore(), nil)
	if err = wb.Commit(); err != nil {
		return false, err
	}

	return !exist, nil
}

func (rds *RedisDataStructure) ZScore(key []byte, member []byte) (float64, error) {
	metadata, err := rds.findMetadata(key, ZSet)
	if err != nil {
		return -1, err
	}
	if metadata.size == 0 {
		return -1, err
	}
	//construct zsetInnerKey
	zik := &zsetInternalKey{
		key:     key,
		version: metadata.version,
		member:  member,
	}

	value, err := rds.db.Get(zik.encodeWithMember())
	if err != nil {
		return -1, err
	}
	return utils.BytesToFloat64(value), err

}

// find the metadata, if it doesn't exist, create a new one
func (rds *RedisDataStructure) findMetadata(key []byte, datatype redisDataType) (*metadata, error) {
	metaBuf, err := rds.db.Get(key)
	if err != nil && err != bitcaskGo.ErrKeyNotFound {
		return nil, err
	}
	var meta *metadata
	var exist = true
	if err == bitcaskGo.ErrKeyNotFound {
		exist = false
	} else {
		meta = decodeMetadata(metaBuf)
		//check data type
		if meta.dataType != datatype {
			return nil, ErrWrongTypeOperation
		}
		//check the expire time
		if meta.expire > 0 && meta.expire <= time.Now().UnixNano() {
			exist = false
		}
	}
	if !exist {
		meta = &metadata{
			dataType: datatype,
			expire:   0,
			version:  time.Now().UnixNano(),
			size:     0,
		}
		if datatype == List {
			meta.head = initialListMark
			meta.tail = initialListMark
		}
	}
	return meta, nil
}
