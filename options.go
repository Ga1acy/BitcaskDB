package bitcaskGo

import "os"

type Options struct {
	//Database 's data 's directory
	DirPath string

	//DataFile size
	DataFileSize int64

	//Decide if we need to do sync after every writing
	SyncWrites bool

	// the type of indexer
	IndexerType IndexerType
}

type IndexerType = int8

const (
	//BTree indexer
	BTree IndexerType = iota + 1
	// ART Adaptive Radix Tree indexer
	ART
)

var DefaultOptions = Options{
	DirPath:      os.TempDir(),
	DataFileSize: 256 * 1024 * 1024,
	SyncWrites:   false,
	IndexerType:  BTree,
}
