package bitcaskGo

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
