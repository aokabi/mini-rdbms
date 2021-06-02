package main

import (
	"fmt"
	"mini-rdbms/lib/btree"
	"mini-rdbms/lib/buffer"
	"mini-rdbms/lib/disk"
	"mini-rdbms/lib/sql"
)

func main() {
	// seach Btree
	diskManager := disk.Open("../simple-table-create/simple.rly")
	pool := buffer.NewBufferPool(10)
	bufferManager := buffer.NewBufferPoolManager(diskManager, pool)

	t := btree.NewBtree(bufferManager, disk.PageID(0))

	// iter := btree.search(
	// 	bufferManager,
	// 	[]byte("Hyogo"),
	// )
	// item := iter.Next()
	iter, err := t.Search(bufferManager, sql.Encode([]byte("y")))
	if err != nil {
		fmt.Println(err)
	}
	for iter.HasNext() {
		tuple := iter.Next(bufferManager)
		fmt.Println(tuple.Key, tuple.Value)
	}
}
