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
	diskManager := disk.Open("../simple-table-create/simple.rly")
	pool := buffer.NewBufferPool(10)
	bufferManager := buffer.NewBufferPoolManager(diskManager, pool)

	var plan sql.PlanNode
	// Select * where id >= 'w' AND id < 'z' AND first_name < 'Dave'
	plan = sql.NewFilter(
		func(record *btree.Tuple) bool { return bytes.Compare(sql.DecodeElems(record.Value)[0], []byte("Dave")) == -1 },
		// func(record *btree.Tuple) bool { return true },
		sql.NewSeqScan(
			0,
			[]byte("w"),
			func(record *btree.Tuple) bool {
				// true if key < []byte("z")
				return bytes.Compare(sql.Decode(record.Key), []byte("z")) == -1
			}),
	)
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
