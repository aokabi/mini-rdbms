package main

import (
	"encoding/binary"
	"fmt"
	"mini-rdbms/lib/btree"
	"mini-rdbms/lib/buffer"
	"mini-rdbms/lib/disk"
)

func main() {
	// seach Btree
	diskManager := disk.Open("large.btr")
	pool := buffer.NewBufferPool(10)
	bufferManager := buffer.NewBufferPoolManager(diskManager, pool)

	btree := btree.NewBtree(bufferManager, disk.PageID(0))

	// iter := btree.search(
	// 	bufferManager,
	// 	[]byte("Hyogo"),
	// )
	// item := iter.Next()
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(0x000c0d1d))
	item, err := btree.Search(bufferManager, b)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(item)
}
