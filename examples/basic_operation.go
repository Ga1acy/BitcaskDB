package main

import (
	bitcask "bitcaskGo"
	"fmt"
)

func main() {
	opts := bitcask.DefaultOptions
	opts.DirPath = "/tmp/bitcaskGo"
	db, err := bitcask.Open(opts)
	if err != nil {
		panic(err)
	}

	err = db.Put([]byte("name1"), []byte("chenyi"))
	if err != nil {
		panic(err)
	}

	err = db.Put([]byte("name2"), []byte("zhangjianqi"))
	if err != nil {
		panic(err)
	}

	val1, err := db.Get([]byte("name1"))
	if err != nil {
		panic(err)
	}

	val2, err := db.Get([]byte("name2"))
	if err != nil {
		panic(err)
	}

	fmt.Println("val is", string(val1))
	fmt.Println("val is", string(val2))

	err = db.Delete([]byte("name1"))
	if err != nil {
		panic(err)
	}

	err = db.Delete([]byte("name2"))
	if err != nil {
		panic(err)
	}
}
