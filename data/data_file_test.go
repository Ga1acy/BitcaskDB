package data

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestOpenDataFile(t *testing.T) {
	dataFile1, err := OpenDataFile(os.TempDir(), 0)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile1)

	dataFile2, err := OpenDataFile(os.TempDir(), 11)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile2)

	dataFile3, err := OpenDataFile(os.TempDir(), 11)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile3)

	t.Log(os.TempDir())
}

func TestDatafile_Write(t *testing.T) {
	dataFile, err := OpenDataFile(os.TempDir(), 0)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)

	err = dataFile.Write([]byte("test1"))
	assert.Nil(t, err)

	err = dataFile.Write([]byte("test2"))
	assert.Nil(t, err)

	err = dataFile.Write([]byte("test3"))
	assert.Nil(t, err)
}

func TestDatafile_Close(t *testing.T) {
	dataFile, err := OpenDataFile(os.TempDir(), 22)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)

	err = dataFile.Write([]byte("test"))
	assert.Nil(t, err)

	err = dataFile.Close()
	assert.Nil(t, err)
}

func TestDatafile_Sync(t *testing.T) {
	dataFile, err := OpenDataFile(os.TempDir(), 33)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)

	err = dataFile.Write([]byte("test"))
	assert.Nil(t, err)

	err = dataFile.Sync()
	assert.Nil(t, err)
}

func TestDatafile_ReadLogRecord(t *testing.T) {
	datafile, err := OpenDataFile(os.TempDir(), 77)
	assert.Nil(t, err)
	assert.NotNil(t, datafile)

	//construct the first logRecord
	logRecord1 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("test1"),
		Type:  LogRecordNormal,
	}

	encoLogRecord1, size1 := EncodeLogRecord(logRecord1)
	err = datafile.Write(encoLogRecord1)
	assert.Nil(t, err)
	//t.Log(size1)

	readLogRecord1, readSize1, err := datafile.ReadLogRecord(0)
	assert.Nil(t, err)
	assert.Equal(t, logRecord1, readLogRecord1)
	assert.Equal(t, size1, readSize1)

	//construct more logRecords, and read them from different positions

	logRecord2 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("test2"),
		Type:  LogRecordNormal,
	}

	encoLogRecord2, size2 := EncodeLogRecord(logRecord2)
	err = datafile.Write(encoLogRecord2)
	assert.Nil(t, err)
	//t.Log(size2)

	readLogRecord2, readSize2, err := datafile.ReadLogRecord(size1)
	assert.Nil(t, err)
	assert.Equal(t, logRecord2, readLogRecord2)
	assert.Equal(t, size2, readSize2)

	//special case: the deleted logRecord is in the end of data file
	logRecord3 := &LogRecord{
		Key:   []byte("1"),
		Value: []byte(""),
		Type:  LogRecordDeleted,
	}

	encoLogRecord3, size3 := EncodeLogRecord(logRecord3)
	err = datafile.Write(encoLogRecord3)
	assert.Nil(t, err)
	//t.Log(size3)

	readLogRecord3, readSize3, err := datafile.ReadLogRecord(size1 + size2)
	assert.Nil(t, err)
	assert.Equal(t, logRecord3, readLogRecord3)
	assert.Equal(t, size3, readSize3)
}
