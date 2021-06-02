package main

import (
	"bytes"
	"fmt"
	"mini-rdbms/lib/btree"
	"mini-rdbms/lib/buffer"
	"mini-rdbms/lib/disk"
	"mini-rdbms/lib/sql"
)

func main() {
	// seach Btree
	diskManager := disk.Open("../table-create/table.rly")
	pool := buffer.NewBufferPool(10)
	bufferManager := buffer.NewBufferPoolManager(diskManager, pool)

	var plan sql.PlanNode
	// Select * where id >= 'w' AND id < 'z' AND first_name < 'Dave'
	plan = sql.NewIndexScan(
		0,
		2,
		[]byte("Smith"),
		func(record *btree.Tuple) bool {
			// true if key == []byte("Smith")
			return bytes.Equal(sql.Decode(record.Key), []byte("Smith")) 
		})
	exec := plan.Start(bufferManager)
	for {
		record := exec.Next(bufferManager)
		if record == nil {
			return
		}
		fmt.Printf("%s\n", sql.Decode(record.Key))
		for _, value := range sql.DecodeElems(record.Value) {
			fmt.Printf("%s\n", value)
		}
	}

	// iter := btree.search(
	// 	bufferManager,
	// 	[]byte("Hyogo"),
	// )
	// item := iter.Next()
}
