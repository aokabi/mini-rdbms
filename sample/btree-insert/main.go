package main

import (
	"fmt"
	"main/lib"
)

type PageID uint

func main() {
	// insert data to BTree
	disk := lib.Open("../../test.btr")
	pool := lib.NewBufferPool(10)
	bufferManager := lib.NewBufferPoolManager(disk, pool)

	btree := lib.CreateBtree(bufferManager)

	btree.Insert(bufferManager, []byte("Kanagawa"), []byte("Yokohama"))
	btree.Insert(bufferManager, []byte("Osaka"), []byte("Osaka"))
	btree.Insert(bufferManager, []byte("Aichi"), []byte("Nagoya"))
	btree.Insert(bufferManager, []byte("Hokkaido"), []byte("Sapporo"))
	btree.Insert(bufferManager, []byte("Fukuoka"), []byte("Fukuoka"))
	btree.Insert(bufferManager, []byte("Hyogo"), []byte("Kobe"))
	btree.Insert(bufferManager, []byte("Aomori"), []byte("Aomori"))

	fmt.Printf("%+v", btree)
	// fmt.Println(len(btree.root.children))
	bufferManager.Flush()

}
