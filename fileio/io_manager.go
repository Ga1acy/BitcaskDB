package fileio

const DataFilePerm = 0644

type IOManager interface {
	//Read data from the specified file.
	Read([]byte, int64) (int, error)

	// Write byte arrary to file
	Write([]byte) (int, error)

	// Sync 把内存缓冲区的数据持久化到磁盘当中
	// Persist the data from the memory buffer to the disk.
	Sync() error

	// Close file
	Close() error
}
