package data

import "encoding/binary"

type LogRecordType byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDeleted
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
}

// EncodeLogRecord Encode LogRecord, return a byte arrary and length
func EncodeLogRecord(logRecord *LogRecord) ([]byte, int64) {
	return nil, 0
}

// decode the header information in the byte array
// return logRecordHeader and it's size
func decodeLogRecordHeader(buf []byte) (*logRecordHeader, int64) {
	return nil, 0
}

func getLogRecordCRC(logRecord *LogRecord, header []byte) uint32 {
	return 0
}
