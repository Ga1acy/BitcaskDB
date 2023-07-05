package fileio

import "os"

//Stand system file Io
//标准系统文件IO

type FileIO struct {
	fd *os.File //system file  descriptor
}

// Create a file_io
func NewFileIOManager(fildPath string) (*FileIO, error) {
	fd, err := os.OpenFile(
		fildPath,
		os.O_CREATE|os.O_RDWR|os.O_APPEND, // if this file doesn't exist, then create one(O_CREATE), this file can READ & Write(O_RDWR), and append the data when write
		DataFilePerm,                      //0644, give the perm
	)
	if err != nil {
		return nil, err
	}
	return &FileIO{fd: fd}, nil
}

func (fio *FileIO) Read(b []byte, offset int64) (int, error) {
	return fio.fd.ReadAt(b, offset)

}

func (fio *FileIO) Write(b []byte) (int, error) {
	return fio.fd.Write(b)
}

func (fio *FileIO) Sync() error {
	return fio.fd.Sync()
}

func (fio *FileIO) Close() error {
	return fio.fd.Close()
}
