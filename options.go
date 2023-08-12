package bitcaskGo

import "os"

type Options struct {
	//Database 's data 's directory
	DirPath string

	//DataFile size
	DataFileSize int64

	//Decide if we need to do sync after every writing
	SyncWrites bool

	//do the sync operation when bytes accumulate to this option
	BytesPerSync uint

	// the type of indexer
	IndexerType IndexerType

	//whether we need mmap when start or not
	MMapAtStartup bool

	//threshold for data file merging
	DataFileMergeRatio float32
}

type IndexerType = int8

const (
	//BTree indexer
	BTree IndexerType = iota + 1
	// ART Adaptive Radix Tree indexer
	ART
	// BPTree B Plus Tree indexer save the index into disk
	BPTree
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
	DirPath:            os.TempDir(),
	DataFileSize:       256 * 1024 * 1024,
	SyncWrites:         false,
	BytesPerSync:       0,
	IndexerType:        BTree,
	MMapAtStartup:      true,
	DataFileMergeRatio: 0.5,
}

var DefaultIteratorOptions = IteratorOptions{
	Prefix:  nil,
	Reverse: false,
}

var DefaultWriteBatchOptions = WriteBatchOptions{
	MaxBatchNum: 10000,
	SyncWrites:  true,
}
