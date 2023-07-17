package bitcaskGo

import (
	"bitcaskGo/index"
	"bytes"
)

// Iterator for users
type Iterator struct {
	indexIter index.Iterator //Index iterator
	db        *DB
	options   IteratorOptions
}

func (db *DB) NewIterator(opts IteratorOptions) *Iterator {
	indexIter := db.index.Iterator(opts.Reverse)
	return &Iterator{
		indexIter: indexIter,
		db:        db,
		options:   opts,
	}
}

func (it *Iterator) Rewind() {
	it.indexIter.Rewind()
	it.skipToNext()
}

// Seek find the first key that is greater than or equal to the target key, and iterate from there
func (it *Iterator) Seek(key []byte) {
	it.indexIter.Seek(key)
	it.skipToNext()
}

// Next jump to next key
func (it *Iterator) Next() {
	it.indexIter.Next()
	it.skipToNext()
}

// Valid check if the key is available, in other words, check if the iterate is over, in order to quit to iterate
func (it *Iterator) Valid() bool {
	return it.indexIter.Valid()
}

// Key get the key in current iterate position
func (it *Iterator) Key() []byte {
	return it.indexIter.Key()
}

// Value get the values in current iterate position
func (it *Iterator) Value() ([]byte, error) {
	logRecordPos := it.indexIter.Value()
	it.db.mu.RLock()
	defer it.db.mu.RUnlock()
	return it.db.getValueByPosition(logRecordPos)

}

// Close the current iterator and release relevant resources
func (it *Iterator) Close() {
	it.indexIter.Close()
}

func (it *Iterator) skipToNext() {
	prefixLen := len(it.options.Prefix)
	if prefixLen == 0 {
		return
	}

	for ; it.indexIter.Valid(); it.indexIter.Next() {
		key := it.indexIter.Key()
		if prefixLen <= len(key) && bytes.Compare(it.options.Prefix, key[:prefixLen]) == 0 {
			break
		}
	}
}
