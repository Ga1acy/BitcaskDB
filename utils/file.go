package utils

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

// DirSize get the size of the directory
func DirSize(dirPath string) (int64, error) {
	var size int64
	err := filepath.Walk(dirPath, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return nil
	})
	return size, err

}

func AvailableDiskSize() (uint64, error) {
	//get the current working directory
	wd, err := syscall.Getwd()
	if err != nil {
		return 0, err
	}
	var stat syscall.Statfs_t
	if err = syscall.Statfs(wd, &stat); err != nil {
		return 0, err
	}

	//Bavail : the available block at file system
	//Bsize : the block size of file system in bytes
	return stat.Bavail * uint64(stat.Bsize), nil
}

func CopyDir(src, des string, exclude []string) error {
	//if the des directory doesn't exist, then create one
	if _, err := os.Stat(des); os.IsNotExist(err) {
		if err := os.MkdirAll(des, os.ModePerm); err != nil {
			return err
		}
	}

	return filepath.Walk(src, func(path string, info fs.FileInfo, err error) error {
		//remove the directory information of file, just keep the file name
		fileName := strings.Replace(path, src, "", 1)
		if fileName == "" {
			return nil
		}

		//compare the file with all exclusive file name, if matched, jump out this file
		for _, e := range exclude {
			matched, err := filepath.Match(e, info.Name())
			if err != nil {
				return err
			}
			if matched {
				return nil
			}
		}

		//if it's a directory, creat a same one in the des directory
		if info.IsDir() {
			return os.MkdirAll(filepath.Join(des, fileName), info.Mode())
		}
		data, err := os.ReadFile(filepath.Join(src, fileName))
		if err != nil {
			return err
		}
		return os.WriteFile(filepath.Join(des, fileName), data, info.Mode())
	})
}
