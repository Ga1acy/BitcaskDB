package bitcaskGo

import (
	"bitcaskGo/data"
	"encoding/binary"
	"sync"
	"sync/atomic"
)

const nonTransactionSeqNo uint64 = 0

var txnFinKey = []byte("txn-finished")

// WriteBatch write a batch of data as the type of atom, ensure transaction's atomicity
// a batch of operation, not only write, but we can do delete operation also
type WriteBatch struct {
	options       WriteBatchOptions
	mu            *sync.Mutex
	db            *DB
	pendingWrites map[string]*data.LogRecord //temporary save the data which written by user
}

func (db *DB) NewWriteBatch(opts WriteBatchOptions) *WriteBatch {

	if db.options.IndexerType == BPTree && !db.seqNoFileExists && !db.isInitial {
		panic("can not use write batch, seq no file does not exists ")
	}

	return &WriteBatch{
		options:       opts,
		mu:            new(sync.Mutex),
		db:            db,
		pendingWrites: make(map[string]*data.LogRecord),
	}
}

// Put write data as the form batch
func (wb *WriteBatch) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	wb.mu.Lock()
	defer wb.mu.Unlock()

	//save logRecord temporary
	logRecord := &data.LogRecord{
		Key:   key,
		Value: value,
	}
	wb.pendingWrites[string(key)] = logRecord
	return nil
}

func (wb *WriteBatch) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	wb.mu.Lock()
	defer wb.mu.Unlock()

	//if the data doesn't exist , return directly
	//if it's in the pendingWrites, delete it
	logRecordPos := wb.db.index.Get(key)
	if logRecordPos == nil {
		if wb.pendingWrites[string(key)] != nil {
			delete(wb.pendingWrites, string(key))
		}
		return nil
	}

	//save logRecord temporary
	logRecord := &data.LogRecord{Key: key, Type: data.LogRecordDeleted}
	wb.pendingWrites[string(key)] = logRecord

	return nil
}

func (wb *WriteBatch) Commit() error {
	wb.mu.Lock()
	defer wb.mu.Unlock()

	if len(wb.pendingWrites) == 0 {
		return nil
	}

	if uint(len(wb.pendingWrites)) > wb.options.MaxBatchNum {
		return ErrExceedMaxBatchNum
	}
	//get a lock to ensure serialization of transaction commits.
	wb.db.mu.Lock()
	defer wb.db.mu.Unlock()

	//get the newest transaction seqNo
	seqNo := atomic.AddUint64(&wb.db.seqNo, 1)

	//begin to write data to the data filek

	//logRecordPos's buffer, when the write process finished,
	//can we update the position information to memory index
	positions := make(map[string]*data.LogRecordPos)

	for _, record := range wb.pendingWrites {
		logRecordPos, err := wb.db.appendLogRecord(&data.LogRecord{
			Key:   logRecordKeyWithSeq(record.Key, seqNo),
			Value: record.Value,
			Type:  record.Type,
		})
		if err != nil {
			return err
		}

		positions[string(record.Key)] = logRecordPos
	}

	//write a data to signify that the transaction is completed
	finishedRecord := &data.LogRecord{
		Key:  logRecordKeyWithSeq(txnFinKey, seqNo),
		Type: data.LogRecordTxnFinished,
	}
	if _, err := wb.db.appendLogRecord(finishedRecord); err != nil {
		return err
	}

	//decide sync or not depend on the WriteBatch's options
	if wb.options.SyncWrites && wb.db.activeFile != nil {
		if err := wb.db.activeFile.Sync(); err != nil {
			return err
		}
	}

	//update the memory index
	for _, record := range wb.pendingWrites {
		pos := positions[string(record.Key)]
		var oldPos *data.LogRecordPos
		if record.Type == data.LogRecordNormal {
			oldPos = wb.db.index.Put(record.Key, pos)
		}
		if record.Type == data.LogRecordDeleted {
			oldPos, _ = wb.db.index.Delete(record.Key)
		}
		if oldPos != nil {
			wb.db.reclaimSize += int64(oldPos.Size)
		}
	}

	//clearing temporary data
	wb.pendingWrites = make(map[string]*data.LogRecord)

	return nil
}

// encode logRecord's key with seqNo
// -----------|---------------------
//
//	seqNo 			key
func logRecordKeyWithSeq(key []byte, seqNo uint64) []byte {
	seq := make([]byte, binary.MaxVarintLen64)
	size := binary.PutUvarint(seq[:], seqNo)

	encoKey := make([]byte, size+len(key))
	copy(encoKey[:size], seq[:size])
	copy(encoKey[size:], key)

	return encoKey
}

// parse logRecord's key, get the real key and seqNo
func parselogRecordKey(key []byte) ([]byte, uint64) {
	seqNo, n := binary.Uvarint(key)
	realKey := key[n:]
	return realKey, seqNo
}
