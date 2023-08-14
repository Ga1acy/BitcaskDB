package bitcaskGo

import (
	"bitcaskGo/data"
	"bitcaskGo/fileio"
	"bitcaskGo/index"
	"bitcaskGo/utils"
	"errors"
	"fmt"
	"github.com/gofrs/flock"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

const (
	seqNoKey     = "seq-no"
	fileLockName = "flock"
)

type DB struct {
	options         Options
	mu              *sync.RWMutex
	activeFile      *data.Datafile            //current active file  use for write
	olderFiles      map[uint32]*data.Datafile //the set of older file map by fileid(uint32) only for read
	index           index.Indexer             //Memory index 内存索引
	fileIds         []int                     //File id,only use for load index, can't be used or update in other place
	seqNo           uint64                    //the transaction sequence number, increase globally
	isMerging       bool                      //check if the database is in the process of merging
	seqNoFileExists bool                      //signify that if the file which save the transaction seqNo exists
	isInitial       bool                      //if is the first time to initial this data directory
	fileLock        *flock.Flock              //file lock ensure the mutex of different processes
	bytesWrite      uint                      //the total number of bytes that were written
	reclaimSize     int64                     //signify the size that need to be merged/reclaimed
}

type Stat struct {
	KeyNum          uint  //number of keys in database
	DataFileNum     uint  //number of data files
	ReclaimableSize int64 //number of data that can be merged (in bytes)
	DiskSize        int64 //Disk space occupied by the data directory
}

// Open Open a Bitcask storage engine instance.
// 打开bitcask存储引擎实例
func Open(options Options) (*DB, error) {
	//Check the user's database options
	if err := CheckOptions(options); err != nil {
		return nil, err
	}
	var isInitial bool
	//Check if the data directory exist, if not, create a new one
	if _, err := os.Stat(options.DirPath); os.IsNotExist(err) {
		isInitial = true
		if err := os.MkdirAll(options.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	//check if the current data directory is using
	fileLock := flock.New(filepath.Join(options.DirPath, fileLockName))
	hold, err := fileLock.TryLock()
	if err != nil {
		return nil, err
	}
	if !hold {
		return nil, ErrDataBaseisUsing
	}

	//if the data directory exists, but it's empty,
	//we still need to set isInitial to true
	entries, err := os.ReadDir(options.DirPath)
	if err != nil {
		return nil, err
	}
	if len(entries) == 0 {
		isInitial = true
	}

	db := &DB{
		options:    options,
		mu:         new(sync.RWMutex),
		olderFiles: make(map[uint32]*data.Datafile),
		index:      index.NewIndexer(options.IndexerType, options.DirPath, options.SyncWrites),
		isInitial:  isInitial,
		fileLock:   fileLock,
	}

	//load merge data directory
	if err := db.loadMergeFiles(); err != nil {
		return nil, err
	}

	//Load the data file
	if err := db.loadDataFiles(); err != nil {

		return nil, err
	}

	//if we use b plus tree as the indexer
	//we don't need to load index from data files
	if options.IndexerType != BPTree {
		//load index from hint file
		if err := db.loadIndexFromHintFile(); err != nil {
			return nil, err
		}

		//load index from data files
		if err := db.loadIndexFromDataFiles(); err != nil {
			return nil, err
		}
		//reset the ioType to standard file io
		if err := db.resetIOType(); err != nil {
			return nil, err
		}

	}

	//retrieve the current seqNo when indexer is B Plus Tree
	if options.IndexerType == BPTree {
		if err := db.loadSeqNo(); err != nil {
			return nil, err
		}
		//because we jump out the loadIndex function,
		//thus we need to update the writeoff in active file manually
		if db.activeFile != nil {
			size, err := db.activeFile.IOManager.Size()
			if err != nil {
				return nil, err
			}
			db.activeFile.WriteOff = size
		}
	}
	return db, nil
}

// Stat return relevant stat information of database
func (db *DB) Stat() *Stat {
	db.mu.RLock()
	defer db.mu.RUnlock()
	var dataFileNum = uint(len(db.olderFiles))
	if db.activeFile != nil {
		dataFileNum += 1
	}

	dirSize, err := utils.DirSize(db.options.DirPath)
	if err != nil {
		panic(fmt.Sprintf("failed to get dir size : %v", err))
	}

	return &Stat{
		KeyNum:          uint(db.index.Size()),
		DataFileNum:     dataFileNum,
		ReclaimableSize: db.reclaimSize,
		DiskSize:        dirSize, //
	}
}

// Backup the database, copy all the data file to new directory
func (db *DB) Backup(dir string) error {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return utils.CopyDir(db.options.DirPath, dir, []string{fileLockName})
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

	if oldPos := db.index.Put(key, pos); oldPos != nil {
		db.reclaimSize += int64(oldPos.Size)
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
	defer func() {
		if err := db.fileLock.Unlock(); err != nil {
			panic(fmt.Sprintf("failed to unlock the directory, %v", err))
		}

		//when the indexer is bptree, we need to close the indexer when close the db
		//because the indexer is also a db
		if err := db.index.Close(); err != nil {
			panic(fmt.Sprintf("failed to close the index"))
		}
	}()
	if db.activeFile == nil {
		return nil
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	//save the current seq no
	seqNoFile, err := data.OpenSeqNoFile(db.options.DirPath)
	if err != nil {
		return err
	}
	seqNoRecord := &data.LogRecord{
		Key:   []byte(seqNoKey),
		Value: []byte(strconv.FormatUint(db.seqNo, 10)), //converse the seqNo into a decimal string
	}
	encLogRecord, _ := data.EncodeLogRecord(seqNoRecord)
	if err := seqNoFile.Write(encLogRecord); err != nil {
		return err
	}

	if err := seqNoFile.Sync(); err != nil {
		return err
	}

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
	defer iterator.Close()
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
	defer iterator.Close()
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
	pos, err := db.appendLogRecordWithLock(logRecord)
	if err != nil {
		return err
	}
	db.reclaimSize += int64(pos.Size)
	//Delete the correspond key in index
	oldPos, success := db.index.Delete(key)
	if !success {
		return ErrIndexUpdateFailed
	}
	if oldPos != nil {
		db.reclaimSize += int64(oldPos.Size)
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

	db.bytesWrite += uint(length)

	//Do the sync() options depends on user's option
	var needSync = db.options.SyncWrites
	if !needSync && db.options.BytesPerSync > 0 && db.bytesWrite >= db.options.BytesPerSync {
		needSync = true
	}
	if needSync {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}

		//reset the bytesWrite
		if db.bytesWrite > 0 {
			db.bytesWrite = 0
		}
	}

	//Construct memory index information
	//构造内存索引信息
	pos := &data.LogRecordPos{FileId: db.activeFile.Fileid, Offset: writeoff, Size: uint32(length)}
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
	dataFile, err := data.OpenDataFile(db.options.DirPath, initialFileId, fileio.StandardFIO)
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
		ioType := fileio.StandardFIO
		if db.options.MMapAtStartup {
			ioType = fileio.MemoryMap
		}
		dataFile, err := data.OpenDataFile(db.options.DirPath, uint32(fid), ioType)
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
		var oldPos *data.LogRecordPos
		if typ == data.LogRecordDeleted {
			oldPos, _ = db.index.Delete(key)
			db.reclaimSize += int64(pos.Size)
		} else { //Save the key---logRecordPos index to memory indexer
			oldPos = db.index.Put(key, pos)
		}
		if oldPos != nil {
			db.reclaimSize += int64(oldPos.Size)
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
			//Check the type of err: when we hit the last record, it's correct to get an EOF err
			if err != nil {
				if err == io.EOF {
					break //jump out of the loop
				}
				return err
			}

			//Construct and save the memory index
			logRecordPos := &data.LogRecordPos{FileId: fileid, Offset: offset, Size: uint32(size)}

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
		//If we are in the active(last) file, we need to update the active file's WriteOff by using offset
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

	if options.DataFileMergeRatio < 0 || options.DataFileMergeRatio > 1 {
		return errors.New("invalid merge ratio, must between 0 and 1")
	}

	return nil
}

func (db *DB) loadSeqNo() error {
	fileName := filepath.Join(db.options.DirPath, data.SeqNoFileName)
	if _, err := os.Stat(fileName); os.IsNotExist(err) {
		return nil
	}
	seqNofile, err := data.OpenSeqNoFile(db.options.DirPath)
	if err != nil {
		return err
	}

	record, _, err := seqNofile.ReadLogRecord(0)
	seqNo, err := strconv.ParseUint(string(record.Value), 10, 64)
	if err != nil {
		return err
	}

	db.seqNoFileExists = true
	db.seqNo = seqNo
	return os.Remove(fileName)
}

// set data file's ioType to standard file io
func (db *DB) resetIOType() error {
	if db.activeFile == nil {
		return nil
	}

	//reset the current active data file
	if err := db.activeFile.SetIOManager(db.options.DirPath, fileio.StandardFIO); err != nil {
		return err
	}

	//reset old data files
	for _, datafile := range db.olderFiles {
		if err := datafile.SetIOManager(db.options.DirPath, fileio.StandardFIO); err != nil {
			return err
		}
	}
	return nil
}
