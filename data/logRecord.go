package data

import (
	"encoding/binary"
	"hash/crc32"
)

type LogRecordType = byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDeleted
	LogRecordTxnFinished
)

// logRecordHeader:
// crc  --4 bytes
// type -- 1 byte
// key size --dynamic size max to 5 bytes
// value size -- dynamic size, max to 5 bytes
//
//total:15bytes
const maxLogRecordHeaderSize = 4 + 1 + binary.MaxVarintLen32*2

type LogRecord struct {
	Key   []byte
	Value []byte
	Type  LogRecordType
}

type logRecordHeader struct {
	crc           uint32        //crc check value
	logRecordType LogRecordType //type of logRecord(deleted or normal)
	keySize       uint32        //size of key
	valueSize     uint32        //size of value
}

// LogRecordPos 数据内存索引，表示数据在磁盘上的位置
type LogRecordPos struct {
	FileId uint32 //文件id，代表数据在哪个文件当中
	Offset int64  //数据存储在文件中的哪个位置
	Size   uint32 //标识数据在磁盘上的大小
}

// TransactionRecord the data save temporary in one transaction
// save in the TransactionBuffer
type TransactionRecord struct {
	Record   *LogRecord
	Position *LogRecordPos
}

// EncodeLogRecord Encode LogRecord, return a byte array and length
//
//		4 bytes    1byte    variant(max 5)	variant(max 5)
//	-----------+----------+--------------+----------------+-----------+-----------+
//	|  crc   |    type   |	  key size  |	 value size   |	    key    |	value  |
//	----------+--------- +--------------+-----------------+----------+-----------+
func EncodeLogRecord(logRecord *LogRecord) ([]byte, int64) {
	//construct a header byte array
	header := make([]byte, maxLogRecordHeaderSize)

	//save type value after 4 bytes(crc size)
	header[4] = logRecord.Type
	var index = 5

	//save the key size and value size after 5 bytes
	//use variant type
	//update the index after every save operation
	index += binary.PutVarint(header[index:], int64(len(logRecord.Key)))
	index += binary.PutVarint(header[index:], int64(len(logRecord.Value)))

	//get the size of encoded bytes array
	var encoBytesSize = index + len(logRecord.Key) + len(logRecord.Value)

	//construct the encoded logRecord bytes array
	encoBytes := make([]byte, encoBytesSize)

	//copy the header part
	copy(encoBytes[:index], header[:index])
	//save logRecord's key and value, because key and value is already the byte form, we don't need to encode them
	copy(encoBytes[index:], logRecord.Key)
	copy(encoBytes[index+len(logRecord.Key):], logRecord.Value)

	//calculate crc value using the whole bytes array expects crc
	crc := crc32.ChecksumIEEE(encoBytes[4:])          //return uint32
	binary.LittleEndian.PutUint32(encoBytes[:4], crc) //PutUint32: take 4 bytes place

	//for testing
	//fmt.Printf("header length:%d, crc: %d\n", index, crc)

	return encoBytes, int64(encoBytesSize)
}

// EncodeLogRecordPos encode the logRecord position
func EncodeLogRecordPos(pos *LogRecordPos) []byte {
	encPos := make([]byte, binary.MaxVarintLen32*2+binary.MaxVarintLen64)
	var index = 0
	index += binary.PutVarint(encPos[index:], int64(pos.FileId))
	index += binary.PutVarint(encPos[index:], pos.Offset)
	index += binary.PutVarint(encPos[index:], int64(pos.Size))
	return encPos[:index]
}

// DecodeLogRecordPos decode the logRecord position
func DecodeLogRecordPos(encPos []byte) *LogRecordPos {
	var index = 0

	fileId, n := binary.Varint(encPos[index:])
	index += n
	offset, n := binary.Varint(encPos[index:])
	index += n
	size, _ := binary.Varint(encPos[index:])

	return &LogRecordPos{
		FileId: uint32(fileId),
		Offset: offset,
		Size:   uint32(size),
	}
}

// decode the header information in the byte array
// return logRecordHeader and it's size
func decodeLogRecordHeader(buf []byte) (*logRecordHeader, int64) {
	if len(buf) <= 4 {
		return nil, 0
	}

	header := &logRecordHeader{
		crc:           binary.LittleEndian.Uint32(buf[:4]),
		logRecordType: buf[4],
	}

	var index = 5
	//get the keySize
	keySize, n := binary.Varint(buf[index:])
	header.keySize = uint32(keySize)
	index += n

	//get the value size
	valueSize, n := binary.Varint(buf[index:])
	header.valueSize = uint32(valueSize)
	index += n //index is the size of logRecordHeader now

	return header, int64(index)
}

func getLogRecordCRC(logRecord *LogRecord, header []byte) uint32 {
	if logRecord == nil {
		return 0
	}

	//calculate the crc using header data
	crc := crc32.ChecksumIEEE(header[:])
	//update the crc using logRecord's key and value
	crc = crc32.Update(crc, crc32.IEEETable, logRecord.Key)
	crc = crc32.Update(crc, crc32.IEEETable, logRecord.Value)

	return crc
}
