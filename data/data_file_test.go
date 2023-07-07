package data

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestOpenDataFile(t *testing.T) {
	dataFile1, err := OpenDataFile(os.TempDir(), 0)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile1)

	dataFile2, err := OpenDataFile(os.TempDir(), 11)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile2)

	dataFile3, err := OpenDataFile(os.TempDir(), 11)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile3)

	t.Log(os.TempDir())
}

func TestDatafile_Write(t *testing.T) {
	dataFile, err := OpenDataFile(os.TempDir(), 0)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)

	err = dataFile.Write([]byte("test1"))
	assert.Nil(t, err)

	err = dataFile.Write([]byte("test2"))
	assert.Nil(t, err)

	err = dataFile.Write([]byte("test3"))
	assert.Nil(t, err)
}

func TestDatafile_Close(t *testing.T) {
	dataFile, err := OpenDataFile(os.TempDir(), 22)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)

	err = dataFile.Write([]byte("test"))
	assert.Nil(t, err)

	err = dataFile.Close()
	assert.Nil(t, err)
}

func TestDatafile_Sync(t *testing.T) {
	dataFile, err := OpenDataFile(os.TempDir(), 33)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)

	err = dataFile.Write([]byte("test"))
	assert.Nil(t, err)

	err = dataFile.Sync()
	assert.Nil(t, err)
}
