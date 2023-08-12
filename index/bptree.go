package index

import (
	"bitcaskGo/data"
	"go.etcd.io/bbolt"
	"path/filepath"
)

const bptreeIndexFileName = "bptree-index"

var indexBucketName = []byte("bitcask-index")

// BPlusTree BPlus tree index
// wrap the "go.etcd.io/bbolt" package
type BPlusTree struct {
	tree *bbolt.DB
}

// NewBPlusTree initiate BPlus tree index
func NewBPlusTree(dirPath string, syncWrites bool) *BPlusTree {
	opts := bbolt.DefaultOptions
	opts.NoSync = !syncWrites
	//save the index information into disk
	bptree, err := bbolt.Open(filepath.Join(dirPath, bptreeIndexFileName), 0644, opts)
	if err != nil {
		panic("failed to open bptree")
	}

	if err = bptree.Update(func(tx *bbolt.Tx) error {
		_, err = tx.CreateBucketIfNotExists(indexBucketName)
		return err
	}); err != nil {
		panic("failed to create bucket in bptree")
	}

	return &BPlusTree{tree: bptree}
}

func (bptree *BPlusTree) Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos {
	var oldValue []byte
	if err := bptree.tree.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		oldValue = bucket.Get(key)
		return bucket.Put(key, data.EncodeLogRecordPos(pos))
	}); err != nil {
		panic("failed to put the value in bptree")
	}
	if len(oldValue) == 0 {
		return nil
	}
	return data.DecodeLogRecordPos(oldValue)
}

// Get : get the position information by key
func (bptree *BPlusTree) Get(key []byte) *data.LogRecordPos {
	var pos *data.LogRecordPos
	if err := bptree.tree.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		encLogRecordPos := bucket.Get(key)
		if len(encLogRecordPos) != 0 {
			pos = data.DecodeLogRecordPos(encLogRecordPos)
		}
		return nil
	}); err != nil {
		panic("failed to get the value in bptree")
	}
	return pos
}

// Delete the position information by key
// 通过key删除对应的索引位置信息
func (bptree *BPlusTree) Delete(key []byte) (*data.LogRecordPos, bool) {
	var oldValue []byte
	if err := bptree.tree.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		if oldValue = bucket.Get(key); len(oldValue) != 0 {
			return bucket.Delete(key)
		}
		return nil
	}); err != nil {
		panic("failed to delete the value in bptree")
	}
	if len(oldValue) == 0 {
		return nil, false
	}
	return data.DecodeLogRecordPos(oldValue), true
}

// Size return index's size
func (bptree *BPlusTree) Size() int {
	var size int
	if err := bptree.tree.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		size = bucket.Stats().KeyN
		return nil
	}); err != nil {
		panic("failed to get the size in bptree")
	}
	return size
}

// Close the BPTree indexer
func (bptree *BPlusTree) Close() error {
	return bptree.tree.Close()
}

// Iterator index iterator
func (bptree *BPlusTree) Iterator(reverse bool) Iterator {
	return newBptreeIterator(bptree.tree, reverse)
}

type bptreeIterator struct {
	tx        *bbolt.Tx
	cursor    *bbolt.Cursor
	reverse   bool
	currKey   []byte
	currValue []byte
}

func newBptreeIterator(tree *bbolt.DB, reverse bool) *bptreeIterator {
	//open the transaction manually
	tx, err := tree.Begin(false)
	if err != nil {
		panic("failed to begin a transaction")
	}
	bpi := &bptreeIterator{
		tx:      tx,
		cursor:  tx.Bucket(indexBucketName).Cursor(),
		reverse: reverse,
	}
	//rewind the iterator manually,get the initial currKey and currValue
	//thus avoid the mistake in Valid()
	bpi.Rewind()
	return bpi
}

func (bpIterator *bptreeIterator) Rewind() {
	if bpIterator.reverse {
		bpIterator.currKey, bpIterator.currValue = bpIterator.cursor.Last()
	} else {
		bpIterator.currKey, bpIterator.currValue = bpIterator.cursor.First()
	}
}

// Seek find the first key that is greater than or equal to the target key, and iterate from there
func (bpIterator *bptreeIterator) Seek(key []byte) {
	bpIterator.currKey, bpIterator.currValue = bpIterator.cursor.Seek(key)
}

// Next jump to next key
func (bpIterator *bptreeIterator) Next() {
	if bpIterator.reverse {
		bpIterator.currKey, bpIterator.currValue = bpIterator.cursor.Prev()
	} else {
		bpIterator.currKey, bpIterator.currValue = bpIterator.cursor.Next()
	}
}

// Valid check if the key is available, in other words, check if the iterate is over, in order to quit to iterate
func (bpIterator *bptreeIterator) Valid() bool {
	return len(bpIterator.currKey) != 0
}

// Key get the key in current iterate position
func (bpIterator *bptreeIterator) Key() []byte {
	return bpIterator.currKey
}

// Value get the values in current iterate position
func (bpIterator *bptreeIterator) Value() *data.LogRecordPos {
	return data.DecodeLogRecordPos(bpIterator.currValue)
}

// Close the current iterator and release relevant resources
func (bpIterator *bptreeIterator) Close() {
	_ = bpIterator.tx.Rollback()
}
