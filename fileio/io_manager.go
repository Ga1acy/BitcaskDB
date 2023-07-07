package fileio

const DataFilePerm = 0644

type IOManager interface {
	//Read data from the specified file.
	Read([]byte, int64) (int, error)

	// Write byte array to file
	Write([]byte) (int, error)

	// Sync 把内存缓冲区的数据持久化到磁盘当中
	// Persist the data from the memory buffer to the disk.
	Sync() error

	// Close close file
	Close() error

	// Size get the file size
	Size() (int64, error)
}

func NewIOManager(fileName string) (IOManager, error) {
	return NewFileIOManager(fileName)
}
