package bitcaskGo

import (
	"bitcaskGo/data"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
)

const (
	mergeDirName     = "-merge"
	mergeFinishedKey = "merge-finished"
)

func (db *DB) Merge() error {
	//if the database is empty, return directly
	if db.activeFile == nil {
		return nil
	}
	db.mu.Lock()
	//if the database is merging, return directly
	if db.isMerging {
		db.mu.Unlock()
		return ErrMergeIsProcessing
	}
	db.isMerging = true
	defer func() {
		//set the flag when process ends
		db.isMerging = false
	}()

	//sync the current active data file before merging
	if err := db.activeFile.Sync(); err != nil {
		db.mu.Unlock()
		return err
	}

	//transfer the active date file into older file
	db.olderFiles[db.activeFile.Fileid] = db.activeFile

	//open a new active data file
	//the later write action will operate in this file
	if err := db.setActiveDataFile(); err != nil {
		db.mu.Unlock()
		return nil
	}
	//record the first file id that doesn't in merge process
	nonMergeFileId := db.activeFile.Fileid

	//retrieve all data files that need to be merged
	var mergeFiles []*data.Datafile
	for _, dataFile := range db.olderFiles {
		mergeFiles = append(mergeFiles, dataFile)
	}
	db.mu.Unlock()

	sort.Slice(mergeFiles, func(i, j int) bool {
		return mergeFiles[i].Fileid < mergeFiles[j].Fileid
	})

	mergePath := db.getMergePath()

	//if the mergePath exist
	//means that there was a merge process before
	//we need to delete it
	if _, err := os.Stat(mergePath); err == nil {
		//mergePath exist
		if err := os.RemoveAll(mergePath); err != nil {
			return err
		}
	}

	//create a mergePath directory
	if err := os.MkdirAll(mergePath, os.ModePerm); err != nil {
		return err
	}

	//open a new temporary bitcask instance
	mergeOptions := db.options
	mergeOptions.DirPath = mergePath
	mergeOptions.SyncWrites = false //accelerate the merge speed
	mergeDB, err := Open(mergeOptions)
	if err != nil {
		return err
	}

	//open a hint file to save index information
	hintFile, err := data.OpenHintFile(mergePath)
	if err != nil {
		return err
	}

	//traverse and process every data file which need to be merged
	for _, dataFile := range mergeFiles {
		//each data file's logRecord starts from offset zero
		var offset int64 = 0
		//process every logRecord in a data file
		for {
			logRecord, size, err := dataFile.ReadLogRecord(offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}
			//get the real key and the logRecordPos in index
			realKey, _ := parselogRecordKey(logRecord.Key)
			logRecordPos := db.index.Get(realKey)
			//compare with the position information in index,
			//see if it's available
			if logRecordPos != nil &&
				logRecordPos.FileId == dataFile.Fileid &&
				logRecordPos.Offset == offset {
				//clean the transaction flag
				logRecord.Key = logRecordKeyWithSeq(realKey, nonTransactionSeqNo)
				//add this available logRecord into mergeDB
				pos, err := mergeDB.appendLogRecord(logRecord)
				if err != nil {
					return err
				}
				//add the current memory index information(position information) into hint file
				if err := hintFile.WriteHintRecord(realKey, pos); err != nil {
					return err
				}

			}
			//add the offset
			offset += size
		}
	}
	//when traverse done, do the sync operation, ensure all the date write into disk
	if err := hintFile.Sync(); err != nil {
		return err
	}

	if err := mergeDB.Sync(); err != nil {
		return err
	}

	//write a file to signify merge process have finished
	mergeFinishedFile, err := data.OpenMergeFinishedFile(mergePath)
	if err != nil {
		return err
	}
	mergeFinishedRecord := &data.LogRecord{
		Key:   []byte(mergeFinishedKey),
		Value: []byte(strconv.Itoa(int(nonMergeFileId))),
	}

	encRecord, _ := data.EncodeLogRecord(mergeFinishedRecord)
	if err := mergeFinishedFile.Write(encRecord); err != nil {
		return err
	}
	//sync the file
	if err := mergeFinishedFile.Sync(); err != nil {
		return err
	}

	return nil
}

// example:  /tmp/bitcast   ---> /tmp/bitcask-merge
func (db *DB) getMergePath() string {
	//get the father directory path
	dir := path.Dir(path.Clean(db.options.DirPath))
	base := path.Base(db.options.DirPath)
	return filepath.Join(dir, base+mergeDirName)
}

func (db *DB) loadMergeFiles() error {
	mergePath := db.getMergePath()
	//check if the mergePath exist
	if _, err := os.Stat(mergePath); err != nil {
		return err
	}

	defer func() {
		_ = os.RemoveAll(mergePath)
	}()
	dirEntries, err := os.ReadDir(mergePath)
	if err != nil {
		return err
	}

	//find the file that signify the merge is finished, check if the merge is completed
	var mergeFinished bool
	var mergeFileNames []string
	for _, entry := range dirEntries {
		if entry.Name() == data.MergeFinishedFileName {
			mergeFinished = true
		}
		mergeFileNames = append(mergeFileNames, entry.Name())
	}

	//return directly if the merge doesn't complete
	if !mergeFinished {
		return nil
	}

	//begin process

	//get nonMerged file id
	nonMergeFileId, err := db.getNonMergeFileId(mergePath)
	if err != nil {
		return err
	}

	//delete merged data files
	var fileId uint32 = 0
	for ; fileId < nonMergeFileId; fileId++ {
		fileName := data.GetFileName(db.options.DirPath, fileId)
		if _, err := os.Stat(fileName); err == nil { //if the file exist
			if err := os.Remove(fileName); err != nil { //delete the file
				return err
			}
		}
	}
	//move new data files into data directory
	for _, fileName := range mergeFileNames {
		srcPath := filepath.Join(mergePath, fileName)
		desPath := filepath.Join(db.options.DirPath, fileName)
		if err := os.Rename(srcPath, desPath); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) getNonMergeFileId(dirPath string) (uint32, error) {
	mergeFinishedFile, err := data.OpenMergeFinishedFile(dirPath)
	if err != nil {
		return 0, err
	}
	record, _, err := mergeFinishedFile.ReadLogRecord(0)
	if err != nil {
		return 0, err
	}
	nonMergeFileId, err := strconv.Atoi(string(record.Value))
	if err != nil {
		return 0, err
	}
	return uint32(nonMergeFileId), nil
}

func (db *DB) loadIndexFromHintFile() error {
	//because the hint file move to the db's data directory
	//thus this file's path is db's data dir

	//check if the hint file exist
	hintFileName := filepath.Join(db.options.DirPath)
	if _, err := os.Stat(hintFileName); err != nil {
		return nil
	}

	hintFile, err := data.OpenHintFile(db.options.DirPath)
	if err != nil {
		return err
	}

	//read the index in hint file
	var offset int64 = 0
	for {
		logRecord, size, err := hintFile.ReadLogRecord(offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		//decode the logRecord, get the value,
		// the value in hint file is encoded position index information
		logRecordPos := data.DecodeLogRecordPos(logRecord.Value)
		db.index.Put(logRecord.Key, logRecordPos)
		offset += size
	}
	return nil
}
