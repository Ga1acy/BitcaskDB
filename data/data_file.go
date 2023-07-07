package data

import (
	"bitcaskGo/fileio"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"path/filepath"
)

const DataFileNameSuffix = ".data"

var (
	ErrInvalidCRC = errors.New("invalid crc value, the log record may be broken")
)

type Datafile struct {
	Fileid    uint32           //File id
	WriteOff  int64            //The position file write to 文件写到了哪个位置
	IOManager fileio.IOManager //io write & read manage  io读写管理
}

// OpenDataFile open a new data file
func OpenDataFile(dirPath string, fileId uint32) (*Datafile, error) {
	//Construct the file name
	fileName := filepath.Join(dirPath, fmt.Sprintf("%09d", fileId)+DataFileNameSuffix)

	//Construct the IOManager interface
	ioManager, err := fileio.NewIOManager(fileName)
	if err != nil {
		return nil, err
	}

	//Construct the Data File
	dataFile := &Datafile{
		Fileid:    fileId,
		WriteOff:  0,
		IOManager: ioManager,
	}

	return dataFile, nil
}

func (df *Datafile) ReadLogRecord(offset int64) (*LogRecord, int64, error) {
	//get the file size
	fileSize, err := df.IOManager.Size()
	if err != nil {
		return nil, 0, err
	}

	var headerSize int64 = maxLogRecordHeaderSize

	//when we handle the last logRecord at the data file,
	//this logRecord may be smaller than maxLogRecordHeaderSize
	//thus we may read more data which doesn't belong to the logRecord
	//and make an EOF error

	/*
					last logRecord   redundant data
			offset|----------------|--------------
			   	  |----------------------|
		           maxLogRecordHeaderSize
	*/
	if offset+maxLogRecordHeaderSize > fileSize {
		headerSize = fileSize - offset
	}

	//get the encoded header information of logRecord
	encoHeaderBuf, err := df.readNBytes(headerSize, offset)
	if err != nil {
		return nil, 0, err
	}
	//decode the encoHeader
	header, headerSize := decodeLogRecordHeader(encoHeaderBuf)

	//if header is nil,or those three value is 0,
	//means that we are in the end of this data file
	//and there are no more logRecords
	//thus we need to return the EOF error
	if header == nil {
		return nil, 0, io.EOF
	}
	if header.crc == 0 && header.keySize == 0 && header.valueSize == 0 {
		return nil, 0, io.EOF
	}

	//get the size of key and value
	keySize, valueSize := int64(header.keySize), int64(header.valueSize)

	var logRecordSize = headerSize + keySize + valueSize

	//set logRecord's type
	logRecord := &LogRecord{
		Type: header.logRecordType,
	}
	if keySize > 0 || valueSize > 0 {
		//read after the logRecordHeader, and get the kvBuf that contains key and value
		/*
						header 		 key   value
			offset|---------------|------|-------|
							      |     kvBuf    |
		*/

		kvBuf, err := df.readNBytes(keySize+valueSize, offset+headerSize)
		if err != nil {
			return nil, 0, err
		}
		//set logRecord's key
		logRecord.Key = kvBuf[:keySize]
		//set logRecord's value
		logRecord.Value = kvBuf[keySize:]
	}
	//encoHeaderBuf[crc32.size : headerSize]: in order to cut off the redundant header value, just keep the crc value
	crc := getLogRecordCRC(logRecord, encoHeaderBuf[crc32.Size:headerSize])
	if crc != header.crc {
		return nil, 0, ErrInvalidCRC
	}
	return logRecord, logRecordSize, nil
}

func (df *Datafile) Write(buf []byte) error {
	n, err := df.IOManager.Write(buf)
	if err != nil {
		return err
	}
	//update the writeoff after Write operation
	df.WriteOff += int64(n)
	return nil
}

func (df *Datafile) Sync() error {
	return df.IOManager.Sync()
}

func (df *Datafile) Close() error {
	return df.IOManager.Close()
}

// read n bytes from offset
func (df *Datafile) readNBytes(n int64, offset int64) (b []byte, err error) {
	b = make([]byte, n)
	_, err = df.IOManager.Read(b, offset)
	return
}
