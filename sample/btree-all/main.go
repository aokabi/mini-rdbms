package main

import (
	"fmt"
	"main/lib"
)

func main() {
	// search all
	{
		disk := lib.Open("../../test.btr")
		pool := lib.NewBufferPool(10)
		bufferManager := lib.NewBufferPoolManager(disk, pool)

		btree := lib.NewBtree(bufferManager, lib.PageID(0))

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
