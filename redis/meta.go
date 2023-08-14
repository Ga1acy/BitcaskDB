package redis

import (
	"bitcaskGo/utils"
	"encoding/binary"
	"math"
)

const (
	//dataType + expire + version + size
	maxMetaDataSize = 1 + binary.MaxVarintLen64*2 + binary.MaxVarintLen32
	//extra head and tail
	extraListMetaSize = binary.MaxVarintLen64 * 2

	initialListMark = math.MaxUint64 / 2
)

// key ----> metadata
// key|version|field ----> value
type metadata struct {
	dataType byte   //type of data
	expire   int64  //the time that data expire
	version  int64  //version number
	size     uint32 //the number of data under the key
	head     uint64 //only use for list
	tail     uint64 //only use for list
}

func (md *metadata) encode() []byte {
	var size = maxMetaDataSize
	if md.dataType == List {
		size += extraListMetaSize
	}
	buf := make([]byte, size)
	buf[0] = md.dataType
	var index = 1
	index += binary.PutVarint(buf[index:], md.expire)
	index += binary.PutVarint(buf[index:], md.version)
	index += binary.PutVarint(buf[index:], int64(md.size))

	if md.dataType == List {
		index += binary.PutUvarint(buf[index:], md.head)
		index += binary.PutUvarint(buf[index:], md.tail)
	}

	return buf[:index]
}

func decodeMetadata(buf []byte) *metadata {
	dataType := buf[0]
	var index = 1
	expire, n := binary.Varint(buf[index:])
	index += n
	version, n := binary.Varint(buf[index:])
	index += n
	size, n := binary.Varint(buf[index:])
	index += n
	var head uint64 = 0
	var tail uint64 = 0
	if dataType == List {
		head, n = binary.Uvarint(buf[index:])
		index += n
		tail, _ = binary.Uvarint(buf[index:])
	}

	return &metadata{
		dataType: dataType,
		expire:   expire,
		version:  version,
		size:     uint32(size),
		head:     head,
		tail:     tail,
	}
}

type hashInternalKey struct {
	key     []byte
	version int64
	field   []byte
}

func (hik *hashInternalKey) encode() []byte {
	//8 is size of version
	buf := make([]byte, len(hik.key)+len(hik.field)+8)
	//key
	var index = 0
	copy(buf[index:index+len(hik.key)], hik.key)
	index += len(hik.key)

	//version
	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(hik.version))
	index += 8

	//field
	copy(buf[index:], hik.field)
	return buf
}

type setInternalKey struct {
	key     []byte
	version int64
	member  []byte
}

func (sik *setInternalKey) encode() []byte {
	//8 is size of version, 4 is size of member size
	buf := make([]byte, len(sik.key)+len(sik.member)+8+4)
	//key
	var index = 0
	copy(buf[index:index+len(sik.key)], sik.key)
	index += len(sik.key)

	//version
	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(sik.version))
	index += 8

	//member
	copy(buf[index:index+len(sik.member)], sik.member)
	index += len(sik.member)

	//member size
	binary.LittleEndian.PutUint32(buf[index:], uint32(len(sik.member)))

	return buf
}

type listInternalKey struct {
	key     []byte
	version int64
	index   uint64
}

func (lik *listInternalKey) encode() []byte {
	buf := make([]byte, len(lik.key)+8*2)
	//key
	var index = 0
	copy(buf[index:index+len(lik.key)], lik.key)
	index += len(lik.key)

	//version
	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(lik.version))
	index += 8

	//index
	binary.LittleEndian.PutUint64(buf[index:], lik.index)
	return buf
}

type zsetInternalKey struct {
	key     []byte
	version int64
	member  []byte
	score   float64
}

func (zik *zsetInternalKey) encodeWithMember() []byte {
	// key | version | member
	buf := make([]byte, len(zik.key)+8+len(zik.member))

	//key
	var index = 0
	copy(buf[index:index+len(zik.key)], zik.key)
	index += len(zik.key)

	//version
	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(zik.version))
	index += 8

	//member
	copy(buf[index:], zik.member)

	return buf
}

func (zik *zsetInternalKey) encodeWithScore() []byte {
	scoreByte := utils.Float64ToBytes(zik.score)
	// key | version | score | member | member size
	buf := make([]byte, len(zik.key)+8+len(zik.member)+len(scoreByte)+4)

	//key
	var index = 0
	copy(buf[index:index+len(zik.key)], zik.key)
	index += len(zik.key)

	//version
	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(zik.version))
	index += 8

	//score
	copy(buf[index:index+len(scoreByte)], scoreByte)
	index += len(scoreByte)

	//member
	copy(buf[index:index+len(zik.member)], zik.member)
	index += len(zik.member)

	binary.LittleEndian.PutUint32(buf[index:], uint32(len(zik.member)))

	return buf
}
