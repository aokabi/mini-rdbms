package sql

import (
	"fmt"
	"mini-rdbms/lib/btree"
	"mini-rdbms/lib/buffer"
	"mini-rdbms/lib/disk"
)

const encGroupSize = 8

type simpleTable struct {
	metaPageID  disk.PageID
	numKeyElems uint
}

// schema定義
func NewSimpleTable(numKeyElems uint) *simpleTable {
	return &simpleTable{disk.InvalidPageID, numKeyElems}
}

// 1テーブル1ツリー
// 1ノード1ページ
// 子ノードへの参照は子ノードのpageIDを保持
func (t *simpleTable) Create(bufferPoolManager *buffer.BufferPoolManager) {
	btree := btree.CreateBtree(bufferPoolManager)
	t.metaPageID = btree.MetaPageID
}

func (t *simpleTable) Insert(bufferPoolManager *buffer.BufferPoolManager, record [][]byte) {
	tree := btree.NewBtree(bufferPoolManager, t.metaPageID)
	fmt.Println(tree.MetaPageID)
	// encode primary key
	key := record[:t.numKeyElems]
	encodedKey := encodeElems(key)
	// encode value
	value := record[t.numKeyElems:]
	encodedValue := encodeElems(value)
	err := tree.Insert(bufferPoolManager, encodedKey, encodedValue)
	if err != nil {
		panic(err)
	}
}

func encodeElems(elems [][]byte) []byte {
	result := make([]byte, 0)
	for _, elem := range elems {
		result = append(result, encode(elem)...)
	}
	return result
}

func encode(elems []byte) []byte {
	result := make([]byte, 0, (len(elems)/8+1)*(encGroupSize+1))
	for i := 0; i < len(elems); i += encGroupSize {
		// 末尾なら
		if encGroupSize >= len(elems)-i {
			validLen := len(elems[i:])
			padding := make([]byte, encGroupSize-validLen)
			result = append(result, elems[i:]...)
			result = append(result, padding...)
			result = append(result, byte(validLen))
		} else {
			// 末尾じゃないなら9を入れる
			result = append(result, elems[i:i+encGroupSize]...)
			result = append(result, 9)
		}
	}
	return result
}
