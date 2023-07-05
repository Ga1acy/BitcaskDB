package data

import "bitcaskGo/fileio"

const DataFileNameSuffix = ".data"

type Datafile struct {
	Fileid    uint32           //File id
	WriteOff  int64            //The position file write to 文件写到了哪个位置
	IOManager fileio.IOManager //io write & read manage  io读写管理
}

func OpenDataFile(dirPath string, fileId uint32) (*Datafile, error) {
	return nil, nil
}

func (df *Datafile) ReadLogRecord(offset int64) (*LogRecord, int64, error) {
	return nil, 0, nil
}

func (df *Datafile) Write(buf []byte) error {
	return nil
}

func (df *Datafile) Sync() error {
	return nil
}
