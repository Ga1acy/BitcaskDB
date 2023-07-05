package data

type LogRecordType byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDeleted
)

type LogRecord struct {
	Key   []byte
	Value []byte
	Type  LogRecordType
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
