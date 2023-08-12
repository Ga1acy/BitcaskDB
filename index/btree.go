package index

import (
	"bitcaskGo/data"
	"bytes"
	"sort"
	"sync"

	"github.com/google/btree"
)

// BTree btree索引，封装btree库
type BTree struct {
	tree *btree.BTree
	lock *sync.RWMutex
}

func NewBtree() *BTree {
	return &BTree{
		tree: btree.New(32),
		lock: new(sync.RWMutex),
	}
}

func (bt *BTree) Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos {
	it := &Item{key: key, pos: pos}
	bt.lock.Lock()
	oldItem := bt.tree.ReplaceOrInsert(it)
	bt.lock.Unlock()
	if oldItem == nil {
		return nil
	}
	return oldItem.(*Item).pos
}
func (bt *BTree) Get(key []byte) *data.LogRecordPos {
	it := &Item{key: key}
	btreeItem := bt.tree.Get(it) //Get operation return an Item
	if btreeItem == nil {
		return nil
	}
	return btreeItem.(*Item).pos
}
func (bt *BTree) Delete(key []byte) (*data.LogRecordPos, bool) {
	it := &Item{key: key}
	bt.lock.Lock()
	oldItem := bt.tree.Delete(it) //Delete operation return a old values
	bt.lock.Unlock()
	if oldItem == nil { //if the old values is nil, means that the delete operation is failed
		return nil, false
	}
	return oldItem.(*Item).pos, true
}

func (bt *BTree) Size() int {
	return bt.tree.Len()
}

func (bt *BTree) Iterator(reverse bool) Iterator {
	if bt.tree == nil {
		return nil
	}
	bt.lock.RLock()
	defer bt.lock.RUnlock()
	return newBTreeIterator(bt.tree, reverse)
}

// Close unnecessary method
func (bt *BTree) Close() error {
	return nil
}

// BTree index iterator
type btreeIterator struct {
	currIndex int     //current iterating index position of the traversal
	reverse   bool    //whether it is a reverse traversal
	values    []*Item //key + logRecordPos
}

func newBTreeIterator(tree *btree.BTree, reverse bool) *btreeIterator {
	var idx int
	values := make([]*Item, tree.Len())

	//save all data to the array
	saveValues := func(item btree.Item) bool {
		values[idx] = item.(*Item)
		idx++
		return true
	}

	if reverse {
		tree.Descend(saveValues)
	} else {
		tree.Ascend(saveValues)
	}

	return &btreeIterator{
		currIndex: 0,
		reverse:   reverse,
		values:    values,
	}
}

func (btIte *btreeIterator) Rewind() {
	btIte.currIndex = 0

}

func (btIte *btreeIterator) Seek(key []byte) {
	//the values array is reversal
	if btIte.reverse { //use binary search to find the key in values array
		btIte.currIndex = sort.Search(len(btIte.values), func(i int) bool {
			return bytes.Compare(btIte.values[i].key, key) <= 0 //it's reversal, thus we use <=
		})
	} else {
		btIte.currIndex = sort.Search(len(btIte.values), func(i int) bool {
			return bytes.Compare(btIte.values[i].key, key) >= 0 //it's normal, we use >=
		})
	}

}

func (btIte *btreeIterator) Next() {
	btIte.currIndex += 1
}

func (btIte *btreeIterator) Valid() bool {
	return btIte.currIndex < len(btIte.values)
}

func (btIte *btreeIterator) Key() []byte {
	return btIte.values[btIte.currIndex].key
}

func (btIte *btreeIterator) Value() *data.LogRecordPos {
	return btIte.values[btIte.currIndex].pos
}

func (btIte *btreeIterator) Close() {
	btIte.values = nil
}
