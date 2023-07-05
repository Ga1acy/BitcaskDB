package fileio

import (
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"testing"
)

// delete the test file when the test function return
func deleteTestFile(filename string) {
	if err := os.RemoveAll(filename); err != nil {
		panic(err)
	}
}

func TestNewFileIO_NewFileIOManager(t *testing.T) {
	path := filepath.Join("/tmp", "/test.data")
	fio, err := NewFileIOManager(path)
	defer deleteTestFile(path) // Execute deleteTestFile() when the test function return (use defer)

	assert.Nil(t, err)
	assert.NotNil(t, fio)
}

func TestFileIO_Write(t *testing.T) {
	path := filepath.Join("/tmp", "/test.data")
	fio, err := NewFileIOManager(path)
	defer deleteTestFile(path)

	assert.Nil(t, err)
	assert.NotNil(t, fio)

	test, err := fio.Write([]byte(""))
	assert.Equal(t, 0, test)
	assert.Nil(t, err)

	test, err = fio.Write([]byte("chenyi"))
	t.Log(test, err)
	assert.Equal(t, 6, test)
	assert.Nil(t, err)

	test, err = fio.Write([]byte("bitcask"))
	assert.Equal(t, 7, test)
	assert.Nil(t, err)
}

func TestFileIO_Read(t *testing.T) {
	path := filepath.Join("/tmp", "/test.data")
	fio, err := NewFileIOManager(path)
	defer deleteTestFile(path)

	assert.Nil(t, err)
	assert.NotNil(t, fio)

	_, err = fio.Write([]byte("test-a"))
	assert.Nil(t, err)

	_, err = fio.Write([]byte("test-b"))
	assert.Nil(t, err)

	byte1 := make([]byte, 6)
	test, err := fio.Read(byte1, 0)
	t.Log(string(byte1), test)
	assert.Nil(t, err)
	assert.Equal(t, 6, test)
	assert.Equal(t, []byte("test-a"), byte1)

	byte2 := make([]byte, 6)
	test, err = fio.Read(byte2, 6)
	t.Log(string(byte2), test)
	assert.Nil(t, err)
	assert.Equal(t, 6, test)
	assert.Equal(t, []byte("test-b"), byte2)
}

func TestFileIO_Sync(t *testing.T) {
	path := filepath.Join("/tmp", "/test.data")
	fio, err := NewFileIOManager(path)
	defer deleteTestFile(path)

	assert.Nil(t, err)
	assert.NotNil(t, fio)

	err = fio.Sync()
	assert.Nil(t, err)
}

func TestFileIO_Close(t *testing.T) {
	path := filepath.Join("/tmp", "/test.data")
	fio, err := NewFileIOManager(path)
	defer deleteTestFile(path)

	assert.Nil(t, err)
	assert.NotNil(t, fio)

	err = fio.Close()
	assert.Nil(t, err)
}
