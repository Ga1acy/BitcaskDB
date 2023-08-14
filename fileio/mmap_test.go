package fileio

import (
	"github.com/stretchr/testify/assert"
	"io"
	"path/filepath"
	"testing"
)

func TestMMap_Read(t *testing.T) {
	path := filepath.Join("/tmp", "mmap-a.data")
	defer deleteTestFile(path)

	mmapIO, err := NewMMapIOManager(path)
	assert.Nil(t, err)
	assert.NotNil(t, mmapIO)

	//the file is empty
	b1 := make([]byte, 10)
	res1, err := mmapIO.Read(b1, 0)
	assert.Equal(t, 0, res1)
	assert.Equal(t, io.EOF, err)

	//there is date in the file
	fio, err := NewFileIOManager(path)
	assert.Nil(t, err)
	_, err = fio.Write([]byte("aa"))
	assert.Nil(t, err)

	_, err = fio.Write([]byte("bb"))
	assert.Nil(t, err)

	_, err = fio.Write([]byte("cc"))
	assert.Nil(t, err)

	mmapIO2, err := NewMMapIOManager(path)
	assert.Nil(t, err)
	size, err := mmapIO2.Size()
	assert.Nil(t, err)
	assert.Equal(t, int64(6), size)

	b2 := make([]byte, 2)
	n2, err := mmapIO2.Read(b2, 0)
	assert.Nil(t, err)
	assert.Equal(t, 2, n2)

}
