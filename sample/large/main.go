package main

import (
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"log"
	"mini-rdbms/lib/btree"
	"mini-rdbms/lib/buffer"
	"mini-rdbms/lib/disk"
)

type PageID uint

func main() {
	// insert data to BTree
	diskManager := disk.Open("large.btr")
	pool := buffer.NewBufferPool(10)
	bufferManager := buffer.NewBufferPoolManager(diskManager, pool)

	t := btree.CreateBtree(bufferManager)

	for i := 0; i < 5_00; i++ {
		b := make([]byte, 8)
		binary.LittleEndian.PutUint64(b, uint64(i))
		checksum := md5.Sum(b)
		err := t.Insert(bufferManager, b, checksum[:])
		if err != nil {
			fmt.Println(i)
			log.Fatal(err)
			return
		}
	}

	// fmt.Println(len(btree.root.children))
	bufferManager.Flush()

}
