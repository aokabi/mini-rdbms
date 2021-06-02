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
	diskManager := disk.Open("test.btr")
	pool := buffer.NewBufferPool(10)
	bufferManager := buffer.NewBufferPoolManager(diskManager, pool)

	t := btree.CreateBtree(bufferManager)

	err := t.Insert(bufferManager, []byte("Kanagawa"), []byte("Yokohama"))
	if err != nil {
		panic(err)
	}

	t.Insert(bufferManager, []byte("Osaka"), []byte("Osaka"))
	t.Insert(bufferManager, []byte("Aichi"), []byte("Nagoya"))
	t.Insert(bufferManager, []byte("Hokkaido"), []byte("Sapporo"))
	t.Insert(bufferManager, []byte("Fukuoka"), []byte("Fukuoka"))
	fmt.Printf("%+v", t)
	t.Insert(bufferManager, []byte("Hyogo"), []byte("Kobe"))
	t.Insert(bufferManager, []byte("Aomori"), []byte("Aomori"))

	fmt.Printf("%+v", t)
	// fmt.Println(len(btree.root.children))
	bufferManager.Flush()

}
