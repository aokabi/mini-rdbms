package main

import (
	"mini-rdbms/lib/buffer"
	"mini-rdbms/lib/disk"
	"mini-rdbms/lib/sql"
)

func main() {
	diskManager := disk.Open("simple.rly")
	pool := buffer.NewBufferPool(10)
	bufManager := buffer.NewBufferPoolManager(diskManager, pool)

	table := sql.NewSimpleTable(1)
	table.Create(bufManager)

	table.Insert(bufManager, [][]byte{[]byte("z"), []byte("Alice"), []byte("Smith")})
	table.Insert(bufManager, [][]byte{[]byte("x"), []byte("Bob"), []byte("Johnson")})
	table.Insert(bufManager, [][]byte{[]byte("y"), []byte("Charlie"), []byte("Williams")})
	table.Insert(bufManager, [][]byte{[]byte("w"), []byte("Dave"), []byte("Miller")})
	table.Insert(bufManager, [][]byte{[]byte("v"), []byte("Eve"), []byte("Brown")})

	bufManager.Flush()
}
