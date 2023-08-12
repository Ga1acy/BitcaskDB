package index

import (
	"bitcaskGo/data"
	"bytes"
	goart "github.com/plar/go-adaptive-radix-tree"
	"sort"
	"sync"
)

// AdaptiveRadixTree art index
// warp the package of https://github.com/plar/go-adaptive-radix-tree
type AdaptiveRadixTree struct {
	tree goart.Tree
	lock *sync.RWMutex
}

// NewART initial an ART
func NewART() *AdaptiveRadixTree {
	return &AdaptiveRadixTree{
		tree: goart.New(),
		lock: new(sync.RWMutex),
	}
}

func (art *AdaptiveRadixTree) Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos {
	art.lock.Lock()
	oldValue, _ := art.tree.Insert(key, pos)
	art.lock.Unlock()
	if oldValue == nil {
		return nil
	}
	return oldValue.(*data.LogRecordPos)
}

func (art *AdaptiveRadixTree) Get(key []byte) *data.LogRecordPos {
	art.lock.RLock()
	defer art.lock.RUnlock()
	value, success := art.tree.Search(key)
	if !success {
		return nil
	}
	return value.(*data.LogRecordPos)
}

func (art *AdaptiveRadixTree) Delete(key []byte) (*data.LogRecordPos, bool) {
	art.lock.Lock()
	oldValue, deleted := art.tree.Delete(key)
	art.lock.Unlock()
	if oldValue == nil {
		return nil, false
	}
	return oldValue.(*data.LogRecordPos), deleted
}

func (art *AdaptiveRadixTree) Size() int {
	art.lock.RLock()
	size := art.tree.Size()
	art.lock.RUnlock()
	return size

}

// Close unnecessary method
func (art *AdaptiveRadixTree) Close() error {
	return nil
}

func (art *AdaptiveRadixTree) Iterator(reverse bool) Iterator {
	art.lock.RLock()
	defer art.lock.RUnlock()
	return newARTIterator(art.tree, reverse)
}

// ART index iterator
type artIterator struct {
	currIndex int     //current iterating index position of the traversal
	reverse   bool    //whether it is a reverse traversal
	values    []*Item //key + logRecordPos
}

func newARTIterator(tree goart.Tree, reverse bool) *artIterator {
	var idx int
	if reverse {
		idx = tree.Size() - 1
	}
	values := make([]*Item, tree.Size())
	saveValues := func(node goart.Node) bool {
		item := &Item{
			key: node.Key(),
			pos: node.Value().(*data.LogRecordPos),
		}
		values[idx] = item
		if reverse {
			idx--
		} else {
			idx++
		}
		return true
	}

	tree.ForEach(saveValues)

	return &artIterator{
		currIndex: 0,
		reverse:   reverse,
		values:    values,
	}
}

func (artIte *artIterator) Rewind() {
	artIte.currIndex = 0
}

func (artIte *artIterator) Seek(key []byte) {
	//the values array is reversal
	if artIte.reverse { //use binary search to find the key in values array
		artIte.currIndex = sort.Search(len(artIte.values), func(i int) bool {
			return bytes.Compare(artIte.values[i].key, key) <= 0 //it's reversal, thus we use <=
		})
	} else {
		artIte.currIndex = sort.Search(len(artIte.values), func(i int) bool {
			return bytes.Compare(artIte.values[i].key, key) >= 0 //it's normal, we use >=
		})
	}

}

func (artIte *artIterator) Next() {
	artIte.currIndex += 1
}

func (artIte *artIterator) Valid() bool {
	return artIte.currIndex < len(artIte.values)
}

func (artIte *artIterator) Key() []byte {
	return artIte.values[artIte.currIndex].key
}

func (artIte *artIterator) Value() *data.LogRecordPos {
	return artIte.values[artIte.currIndex].pos
}

func (artIte *artIterator) Close() {
	artIte.values = nil
}
