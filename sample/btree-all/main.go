package main

import (
	"fmt"
	"mini-rdbms/lib/btree"
	"mini-rdbms/lib/buffer"
	"mini-rdbms/lib/disk"
)

func main() {
	// search all
	{
		diskManager := disk.Open("../simple-table-create/simple.rly")
		pool := buffer.NewBufferPool(10)
		bufferManager := buffer.NewBufferPoolManager(diskManager, pool)
		btree := btree.NewBtree(bufferManager, disk.PageID(0))
		fmt.Println("hoge")
		// iter := btree.search(
		// 	bufferManager,
		// 	[]byte("Hyogo"),
		// )
		// item := iter.Next()
		// 一番左のノード以外のアイテムが検索できない
		iter, err := btree.SearchAll(bufferManager)
		if err != nil {
			fmt.Println(err)
			return
		}
		for iter.HasNext() {
			item := iter.Next(bufferManager)
			fmt.Println(string(item.Key), string(item.Value))
		}

	}
}
