package bitcaskGo

import (
	"bitcaskGo/data"
	"bitcaskGo/index"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type DB struct {
	options    Options
	mu         *sync.RWMutex
	activeFile *data.Datafile            //current active file  use for write
	olderFiles map[uint32]*data.Datafile //the set of older file map by fileid(uint32) only for read
	index      index.Indexer             //Memory index 内存索引
	fileIds    []int                     //File id,only use for load index, can't be used or update in other place
	seqNo      uint64                    //the transaction sequence number, increase globally
	isMerging  bool                      //check if the database is in the process of merging
}

// Open Open a Bitcask storage engine instance.
// 打开bitcask存储引擎实例
func Open(options Options) (*DB, error) {
	//Check the user's database options
	if err := CheckOptions(options); err != nil {
		return nil, err
	}
	//Check if the data directory exist, if not, create a new one
	if _, err := os.Stat(options.DirPath); os.IsNotExist(err) {
		if err := os.MkdirAll(options.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}
	db := &DB{
		options:    options,
		mu:         new(sync.RWMutex),
		olderFiles: make(map[uint32]*data.Datafile),
		index:      index.NewIndexer(options.IndexerType),
	}

	//load merge data directory
	if err := db.loadMergeFiles(); err != nil {
		return nil, err
	}

	//Load the data file
	if err := db.loadDataFiles(); err != nil {
		return nil, err
	}

	//load index from hint file
	if err := db.loadIndexFromHintFile(); err != nil {
		return nil, err
	}

	if err := db.loadIndexFromDataFiles(); err != nil {
		return nil, err
	}
	return db, nil
}

// Put Write key/value data , the key can't be empty
func (db *DB) Put(key []byte, value []byte) error {
	//Check the key is available
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	//Construct LogRecord struct
	logRecord := &data.LogRecord{
		Key:   logRecordKeyWithSeq(key, nonTransactionSeqNo),
		Value: value,
		Type:  data.LogRecordNormal,
	}
	//Append(Write) logRecord to activeFile
	pos, err := db.appendLogRecordWithLock(logRecord)
	if err != nil {
		return err
	}

	if success := db.index.Put(key, pos); !success {
		return ErrIndexUpdateFailed
	}

	return nil
}

func (db *DB) Get(key []byte) ([]byte, error) {
	//Have a read lock
	db.mu.RLock()
	defer db.mu.RUnlock() //Unlock when function return
	//Check if the key is available
	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}
	//Get the logRecordPos from index by using key
	//从内存数据结构中取出key对应的索引信息
	logRecordPos := db.index.Get(key)

	//If key doesn't in memory data indexer, this key isn't exist
	//如果key不在内存索引中，说明key不存在
	if logRecordPos == nil {
		return nil, ErrKeyNotFound
	}

	//get value from data file by using logRecordPos
	return db.getValueByPosition(logRecordPos)
}

// Close the database
func (db *DB) Close() error {
	if db.activeFile == nil {
		return nil
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	//close the active data file
	if err := db.activeFile.Close(); err != nil {
		return err
	}

	//close older data files
	for _, olderfile := range db.olderFiles {
		if err := olderfile.Close(); err != nil {
			return err
		}
	}
	return nil
}

// Sync Persisting data files, sync active file's data into disk
func (db *DB) Sync() error {
	if db.activeFile == nil {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.activeFile.Sync()
}

// ListKeys get all keys in the database
func (db *DB) ListKeys() [][]byte {
	iterator := db.index.Iterator(false) //use index iterator, because index have all keys' information
	keys := make([][]byte, db.index.Size())
	var idx int
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		keys[idx] = iterator.Key()
		idx++
	}
	return keys
}

// Fold get all data(keys and values), and do specific operation user ask
// when fn return false, shut down the traverse
func (db *DB) Fold(fn func(key []byte, value []byte) bool) error {
	db.mu.RLock()
	defer db.mu.RUnlock()
	iterator := db.index.Iterator(false)
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		value, err := db.getValueByPosition(iterator.Value())
		if err != nil {
			return err
		}
		if !fn(iterator.Key(), value) {
			break
		}
	}
	return nil
}

// get value by using logRecordPos
func (db *DB) getValueByPosition(logRecordPos *data.LogRecordPos) ([]byte, error) {

	//Get the datafile from correspond FileId
	var dataFile *data.Datafile

	//Check if the datafile is active file
	if db.activeFile.Fileid == logRecordPos.FileId {
		dataFile = db.activeFile
	} else {
		dataFile = db.olderFiles[logRecordPos.FileId]
	}

	//Check if the datafile is empty
	if dataFile == nil {
		return nil, ErrDataFileNotFound
	}

	//Read the data by using correspond offset from logRecordPos

	logRecord, _, err := dataFile.ReadLogRecord(logRecordPos.Offset)
	if err != nil {
		return nil, err
	}
	if logRecord.Type == data.LogRecordDeleted {
		return nil, ErrLogRecordDeleted
	}

	return logRecord.Value, nil
}

func (db *DB) Delete(key []byte) error {
	//Validate the key
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	//Check if the key exists, if it doesn't, return directly
	if logRecordPos := db.index.Get(key); logRecordPos == nil {
		return nil
	}

	//Construct the logRecord which type is deleted
	logRecord := &data.LogRecord{
		Key:  logRecordKeyWithSeq(key, nonTransactionSeqNo),
		Type: data.LogRecordDeleted,
	}

	//Write(append) this logRecord to data file
	_, err := db.appendLogRecordWithLock(logRecord)
	if err != nil {
		return err
	}
	//Delete the correspond key in index
	if success := db.index.Delete(key); !success {
		return ErrIndexUpdateFailed
	}
	return nil
}

func (db *DB) appendLogRecordWithLock(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.appendLogRecord(logRecord)
}

// Append logRecord to activeFile
func (db *DB) appendLogRecord(logRecord *data.LogRecord) (*data.LogRecordPos, error) {

	//Check if the current active datafile is existed,
	//because when database have no write, the active datafile is empty
	//if it's empty , create a new one

	if db.activeFile == nil {
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}

	//Encode logRecord, get ready for writing
	encLogRecord, length := data.EncodeLogRecord(logRecord)

	//Check if the data size bigger than activefile's limit
	if db.activeFile.WriteOff+length > db.options.DataFileSize {
		//if so, in order to save the data to disk, we need to sync the datafile to disk
		//先持久化数据文件，保证数据都持久化到磁盘中
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}

		//Transform the activefile to olderfile
		db.olderFiles[db.activeFile.Fileid] = db.activeFile

		//Open a new activefile
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}

	writeoff := db.activeFile.WriteOff
	if err := db.activeFile.Write(encLogRecord); err != nil {
		return nil, err
	}

	//Do the sync() options depends on user's option
	if db.options.SyncWrites {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
	}

	//Construct memory index information
	//构造内存索引信息
	pos := &data.LogRecordPos{FileId: db.activeFile.Fileid, Offset: writeoff}
	return pos, nil

}

// Set current active datafile
// we must have mutex lock when we use this method
func (db *DB) setActiveDataFile() error {
	var initialFileId uint32 = 0
	if db.activeFile != nil {
		//if there was an active datafile
		//the new active datafile's FileId should plus 1
		initialFileId = db.activeFile.Fileid + 1
	}
	dataFile, err := data.OpenDataFile(db.options.DirPath, initialFileId)
	if err != nil {
		return err
	}
	db.activeFile = dataFile
	return nil
}

func (db *DB) loadDataFiles() error {
	dirEntries, err := os.ReadDir(db.options.DirPath)
	if err != nil {
		return err
	}

	var fileIds []int
	for _, entry := range dirEntries {
		//Get the file id
		if strings.HasSuffix(entry.Name(), data.DataFileNameSuffix) {
			splitFileName := strings.Split(entry.Name(), ".")
			fileId, err := strconv.Atoi(splitFileName[0])
			if err != nil {
				return ErrDataDirectoryCorrupted
			}
			fileIds = append(fileIds, fileId)
		}
	}
	//Sort the file id
	sort.Ints(fileIds)
	db.fileIds = fileIds

	//Go through each file id and open the correspond data file
	for i, fid := range fileIds {
		dataFile, err := data.OpenDataFile(db.options.DirPath, uint32(fid))
		if err != nil {
			return err
		}
		//If it's the last one, means that the file id is biggest
		//thus, the data file is active data file
		if i == len(fileIds)-1 {
			db.activeFile = dataFile
		} else { //Otherwise, it's older file
			db.olderFiles[uint32(fid)] = dataFile
		}
	}
	return nil
}

func (db *DB) loadIndexFromDataFiles() error {
	//The database is empty, no datafile
	if len(db.fileIds) == 0 {
		return nil
	}

	//check if the merge process happened
	hasMerge, nonMergeFileId := false, uint32(0)
	mergeFinishedFileName := filepath.Join(db.options.DirPath, data.MergeFinishedFileName)
	if _, err := os.Stat(mergeFinishedFileName); err == nil {
		//means that merge is finished
		//thus we already get some index from hint file
		fid, err := db.getNonMergeFileId(db.options.DirPath)
		if err != nil {
			return err
		}
		hasMerge = true
		nonMergeFileId = fid
	}
	updateIndex := func(key []byte, typ data.LogRecordType, pos *data.LogRecordPos) {
		//Check if the logRecord has been deleted
		var success bool
		if typ == data.LogRecordDeleted {
			success = db.index.Delete(key)
		} else { //Save the key---logRecordPos index to memory indexer
			success = db.index.Put(key, pos)
		}
		if !success {
			panic("failed to update index when startup")
		}
	}

	//two dimension slice, one seqNo(uint64) map to a list of transactionRecords
	var TransactionBuffer = make(map[uint64][]*data.TransactionRecord)
	var currentSeqNo = nonTransactionSeqNo
	//Go through fileIds and handle the logRecords in each file
	//Go through each file:
	for i, fid := range db.fileIds {
		var fileid = uint32(fid)
		//if we have merged, means that we already load those index
		//which file id less than nonMergeFileId from hint file,
		//thus we should jump out of them while loading

		//in other words, if the file id small than the recent nonMerge file id
		//means that we already load that from hint file
		if hasMerge && fileid < nonMergeFileId {
			continue
		}
		var dataFile *data.Datafile
		if fileid == db.activeFile.Fileid {
			dataFile = db.activeFile
		} else {
			dataFile = db.olderFiles[fileid]
		}
		//Handle logRecords in each dataFile
		var offset int64 = 0 //In each datafile, the offset stars from 0
		for {
			logRecord, size, err := dataFile.ReadLogRecord(offset)
			//Check the type of err: when we hit the last record, it's correct to get a EOF err
			if err != nil {
				if err == io.EOF {
					break //jump out of the loop
				}
				return err
			}

			//Construct and save the memory index
			logRecordPos := &data.LogRecordPos{FileId: fileid, Offset: offset}

			//parse the key, get the real key and seqNo
			realKey, seqNo := parselogRecordKey(logRecord.Key)

			if seqNo == nonTransactionSeqNo {
				//non transaction action, update immediately
				updateIndex(realKey, logRecord.Type, logRecordPos)
			} else {

				//if this logRecord's type is TxnFinished,
				//means the transaction finished,
				//thus update the batch of data which belong to the seqNo into index
				if logRecord.Type == data.LogRecordTxnFinished {
					//use for loop to update one by one
					for _, txnRecord := range TransactionBuffer[seqNo] {
						updateIndex(txnRecord.Record.Key, txnRecord.Record.Type, txnRecord.Position)
					}
					//delete those batch of data belong to the seqNo
					delete(TransactionBuffer, seqNo)
				} else {
					//if this logRecord's type is not TxnFinished,
					//means that we don't know if this transaction is succeeded
					//thus we need to put it in the TxnBuffer
					logRecord.Key = realKey
					TransactionBuffer[seqNo] = append(TransactionBuffer[seqNo], &data.TransactionRecord{
						Record:   logRecord,
						Position: logRecordPos,
					})
				}

			}
			//update the transaction seqNo
			if seqNo > currentSeqNo {
				currentSeqNo = seqNo
			}
			//Update the offset , read in a new position next time
			offset += size
		}
		//If we are in the last(active) file, we need to update the active file's WriteOff by using offset
		//make sure that we can append the logRecord in a right position
		if i == len(db.fileIds)-1 {
			db.activeFile.WriteOff = offset
		}

	}
	//update the transaction seqNo
	db.seqNo = currentSeqNo
	return nil
}

func CheckOptions(options Options) error {
	if options.DirPath == "" {
		return errors.New("database directory path is empty")
	}

	if options.DataFileSize <= 0 {
		return errors.New("database data file size must be greater than 0")
	}

	return nil
}
