package fileio

import (
	"golang.org/x/exp/mmap"
	"os"
)

// MMap IO, memory file map
type MMap struct {
	readerAt *mmap.ReaderAt
}

// NewMMapIOManager initiate a mmap io manager
func NewMMapIOManager(fileName string) (*MMap, error) {
	_, err := os.OpenFile(fileName, os.O_CREATE, DataFilePerm)
	if err != nil {
		return nil, err
	}
	readerAt, err := mmap.Open(fileName)
	if err != nil {
		return nil, err
	}
	return &MMap{readerAt: readerAt}, nil
}

func (mmap *MMap) Read(b []byte, offset int64) (int, error) {
	return mmap.readerAt.ReadAt(b, offset)
}

// read only
func (mmap *MMap) Write([]byte) (int, error) {
	panic("doesn't support this operation")
}

func (mmap *MMap) Sync() error {
	panic("doesn't support this operation")
}

func (mmap *MMap) Close() error {
	return mmap.readerAt.Close()
}

func (mmap *MMap) Size() (int64, error) {
	return int64(mmap.readerAt.Len()), nil
}
