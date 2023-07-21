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

type IteratorOptions struct {
	//traverse keys have the Prefix, default is nil
	Prefix []byte

	//the order of traverse, default is false
	Reverse bool
}

type WriteBatchOptions struct {
	//the max number of data in one batch
	MaxBatchNum uint

	//whether sync or not when we commit the transaction
	SyncWrites bool
}

var DefaultOptions = Options{
	DirPath:      os.TempDir(),
	DataFileSize: 256 * 1024 * 1024,
	SyncWrites:   false,
	IndexerType:  BTree,
}

var DefaultIteratorOptions = IteratorOptions{
	Prefix:  nil,
	Reverse: false,
}

var DefaultWriteBatchOptions = WriteBatchOptions{
	MaxBatchNum: 10000,
	SyncWrites:  true,
}
