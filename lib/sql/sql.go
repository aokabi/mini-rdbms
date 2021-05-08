package sql

import (
	"mini-rdbms/lib/btree"
	"mini-rdbms/lib/buffer"
)

const encGroupSize = 8

type simpleTable struct {
	tree        *btree.Btree
	numKeyElems uint
}

// 1テーブル1ツリー
// 1ノード1ページ
// 子ノードへの参照は子ノードのpageIDを保持
func newSimpleTable(bufferPoolManager *buffer.BufferPoolManager, numKeyElems uint) *simpleTable {
	btree := btree.CreateBtree(bufferPoolManager)
	return &simpleTable{btree, numKeyElems}
}

func (t *simpleTable) insert(bufferPoolManager *buffer.BufferPoolManager, record []byte) {
	// encode primary key
	key := record[:t.numKeyElems]
	encodedKey := encode(key)
	// encode value
	value := record[t.numKeyElems:]
	encodedValue := encode(value)
	t.tree.Insert(bufferPoolManager, encodedKey, encodedValue)
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
