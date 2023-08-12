package index

import (
	"bitcaskGo/data"
	"bytes"
	"github.com/google/btree"
)

// Indexer abstract index interface, connect to other data structure by implementing this interface
// 抽象索引接口，后续想要接入其他数据结构，实现该接口则可
type Indexer interface {

	// Put : store the data position information which correspond by key in index
	//向索引中存取key对应的数据位置信息
	Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos

	// Get : get the position information by key
	Get(key []byte) *data.LogRecordPos //通过key拿到索引位置信息

	// Delete the position information by key
	//通过key删除对应的索引位置信息

	Delete(key []byte) (*data.LogRecordPos, bool)

	// Size return index's size
	Size() int

	// Iterator index iterator
	Iterator(reverse bool) Iterator

	// Close the indexer
	Close() error
}

// IndexType enum different type of indexers
type IndexType = int8

const (
	// Btree index
	Btree IndexType = iota + 1

	// ART 自适应基数树索引
	ART

	// BPTree B Plus Tree indexer
	BPTree
)

// NewIndexer Init indexer depends on the indextype
func NewIndexer(typ IndexType, dirPath string, sync bool) Indexer {
	switch typ {
	case Btree:
		return NewBtree()
	case ART:
		return NewART()
	case BPTree:
		return NewBPlusTree(dirPath, sync)
	default:
		panic("unsupported index type")
	}

}

type Item struct {
	key []byte
	pos *data.LogRecordPos
}

func (aitem Item) Less(bitem btree.Item) bool {

	return bytes.Compare(aitem.key, bitem.(*Item).key) == -1

}

// Iterator generic index iterator interface
type Iterator interface {
	// Rewind return to the beginning of iterator
	Rewind()
	// Seek find the first key that is greater than or equal to the target key, and iterate from there
	Seek(key []byte)
	// Next jump to next key
	Next()
	// Valid check if the key is available, in other words, check if the iterate is over, in order to quit to iterate
	Valid() bool
	// Key get the key in current iterate position
	Key() []byte
	// Value get the values in current iterate position
	Value() *data.LogRecordPos
	// Close the current iterator and release relevant resources
	Close()
}
