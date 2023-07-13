package data

import (
	"github.com/stretchr/testify/assert"
	"hash/crc32"
	"testing"
)

func TestEncodeLogRecord(t *testing.T) {
	//case1: normal
	logRecord1 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("chenyi"),
		Type:  LogRecordNormal,
	}

	test1, n1 := EncodeLogRecord(logRecord1)
	assert.NotNil(t, test1)
	assert.Greater(t, n1, int64(5))

	//case2: empty value
	logRecord2 := &LogRecord{
		Key:  []byte("name"),
		Type: LogRecordNormal,
	}

	test2, n2 := EncodeLogRecord(logRecord2)
	assert.NotNil(t, test2)
	assert.Greater(t, n2, int64(5)) //crc + type is 5 bytes

	//case3: type is deleted
	logRecord3 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("chenyi"),
		Type:  LogRecordDeleted,
	}

	test3, n3 := EncodeLogRecord(logRecord3)
	t.Log(test3, n3)
	assert.NotNil(t, test3)
	assert.Greater(t, n3, int64(5))
}

func TestDecodeLogRecordHeader(t *testing.T) {
	//using the information from logRecord in TestEncodelogRecord function
	headerBuf1 := []byte{151, 110, 52, 182, 0, 8, 12}
	header1, size1 := decodeLogRecordHeader(headerBuf1)

	assert.NotNil(t, header1)
	assert.Equal(t, int64(7), size1)
	assert.Equal(t, uint32(3056889495), header1.crc)
	assert.Equal(t, LogRecordNormal, header1.logRecordType)
	assert.Equal(t, uint32(4), header1.keySize)
	assert.Equal(t, uint32(6), header1.valueSize)

	//case2
	headerBuf2 := []byte{9, 252, 88, 14, 0, 8, 0}
	header2, size2 := decodeLogRecordHeader(headerBuf2)

	assert.NotNil(t, header2)
	assert.Equal(t, int64(7), size2)
	assert.Equal(t, uint32(240712713), header2.crc)
	assert.Equal(t, LogRecordNormal, header2.logRecordType)
	assert.Equal(t, uint32(4), header2.keySize)
	assert.Equal(t, uint32(0), header2.valueSize)

	//case3
	headerBuf3 := []byte{18, 183, 162, 107, 1, 8, 12}
	header3, size3 := decodeLogRecordHeader(headerBuf3)
	//t.Log(header3, size3)
	assert.NotNil(t, header3)
	assert.Equal(t, int64(7), size3)
	assert.Equal(t, uint32(1805825810), header3.crc)
	assert.Equal(t, LogRecordDeleted, header3.logRecordType)
	assert.Equal(t, uint32(4), header3.keySize)
	assert.Equal(t, uint32(6), header3.valueSize)

}

func TestGetlogRecordCRC(t *testing.T) {
	//case1
	logRecord1 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("chenyi"),
		Type:  LogRecordNormal,
	}
	headerBuf := []byte{151, 110, 52, 182, 0, 8, 12}

	crc1 := getLogRecordCRC(logRecord1, headerBuf[crc32.Size:])
	assert.Equal(t, uint32(3056889495), crc1)

	//case2
	logRecord2 := &LogRecord{
		Key:  []byte("name"),
		Type: LogRecordNormal,
	}
	headerBuf2 := []byte{9, 252, 88, 14, 0, 8, 0}
	crc2 := getLogRecordCRC(logRecord2, headerBuf2[crc32.Size:])
	assert.Equal(t, uint32(240712713), crc2)

	//case3
	logRecord3 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("chenyi"),
		Type:  LogRecordDeleted,
	}
	headerBuf3 := []byte{18, 183, 162, 107, 1, 8, 12}
	crc3 := getLogRecordCRC(logRecord3, headerBuf3[crc32.Size:])
	assert.Equal(t, uint32(1805825810), crc3)

}
