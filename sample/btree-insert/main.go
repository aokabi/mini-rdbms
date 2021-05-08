package main

import (
	"fmt"
	"mini-rdbms/lib/btree"
	"mini-rdbms/lib/buffer"
	"mini-rdbms/lib/disk"
)

type PageID uint

func main() {
	// insert data to BTree
	diskManager := disk.Open("../../test.btr")
	pool := buffer.NewBufferPool(10)
	bufferManager := buffer.NewBufferPoolManager(diskManager, pool)

	btree := btree.CreateBtree(bufferManager)

	btree.Insert(bufferManager, []byte("Kanagawa"), []byte("Yokohama"))
	btree.Insert(bufferManager, []byte("Osaka"), []byte("Osaka"))
	btree.Insert(bufferManager, []byte("Aichi"), []byte("Nagoya"))
	btree.Insert(bufferManager, []byte("Hokkaido"), []byte("Sapporo"))
	btree.Insert(bufferManager, []byte("Fukuoka"), []byte("Fukuoka"))
	fmt.Printf("%+v", btree)
	btree.Insert(bufferManager, []byte("Hyogo"), []byte("Kobe"))
	btree.Insert(bufferManager, []byte("Aomori"), []byte("Aomori"))

	fmt.Printf("%+v", btree)
	// fmt.Println(len(btree.root.children))
	bufferManager.Flush()

}
