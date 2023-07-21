package bitcaskGo

import "errors"

var (
	ErrKeyIsEmpty             = errors.New("the key is empty")
	ErrIndexUpdateFailed      = errors.New("failed to update index")
	ErrKeyNotFound            = errors.New("can't find the key in database")
	ErrDataFileNotFound       = errors.New("data file is not found")
	ErrLogRecordDeleted       = errors.New("logrecord has been deleted")
	ErrDataDirectoryCorrupted = errors.New("the data directory may be corrupted")
	ErrExceedMaxBatchNum      = errors.New("exceed the max batch number")
	ErrMergeIsProcessing      = errors.New("merge is in the process, try again later")
)
