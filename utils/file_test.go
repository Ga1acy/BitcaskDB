package utils

import (
	"github.com/stretchr/testify/assert"
	"path/filepath"
	"testing"
)

func TestDirSize(t *testing.T) {
	size, err := DirSize(filepath.Join("/tmp/test-dir"))
	assert.Nil(t, err)
	assert.True(t, size > 0)
	//t.Log(size)
}

func TestAvailableDiskSize(t *testing.T) {
	size, err := AvailableDiskSize()
	assert.Nil(t, err)
	assert.True(t, size > 0)
	//t.Log(size / 1024 / 1024 / 1024)
}
