package main

import (
	"fmt"
	"main/lib"
)

func main() {
	// seach Btree
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
	item, err := btree.Search(bufferManager, []byte("Osaka"))
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(item)
}
