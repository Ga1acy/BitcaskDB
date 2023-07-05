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
	Put(key []byte, pos *data.LogRecordPos) bool

	// Get : get the position information by key
	Get(key []byte) *data.LogRecordPos //通过key拿到索引位置信息

	// Delete  delete the position information by key
	//通过key删除对应的索引位置信息
	Delete(key []byte) bool
}

// IndexType enum different type of indexers
type IndexType = int8

const (
	//BTree index
	Btree IndexType = iota + 1
	//自适应基数树索引
	ART
)

// NewIndexer Init indexer depends on the indextype
func NewIndexer(typ IndexType) Indexer {
	switch typ {
	case Btree:
		return NewBtree()
		//TODO
	case ART:
		return nil
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
