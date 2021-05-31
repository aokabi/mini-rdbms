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
	encodedKey := EncodeElems(key)
	// encode value
	value := record[t.numKeyElems:]
	encodedValue := EncodeElems(value)
	err := tree.Insert(bufferPoolManager, encodedKey, encodedValue)
	if err != nil {
		panic(err)
	}
}

func EncodeElems(elems [][]byte) []byte {
	result := make([]byte, 0)
	for _, elem := range elems {
		result = append(result, Encode(elem)...)
	}
	return result
}

func Encode(elem []byte) []byte {
	result := make([]byte, 0, (len(elem)/8+1)*(encGroupSize+1))
	for i := 0; i < len(elem); i += encGroupSize {
		// 末尾なら
		if encGroupSize >= len(elem)-i {
			validLen := len(elem[i:])
			padding := make([]byte, encGroupSize-validLen)
			result = append(result, elem[i:]...)
			result = append(result, padding...)
			result = append(result, byte(validLen))
		} else {
			// 末尾じゃないなら9を入れる
			result = append(result, elem[i:i+encGroupSize]...)
			result = append(result, 9)
		}
	}
	return result
}

func DecodeElems(elems []byte) [][]byte {
	start := 0
	result := make([][]byte, 0)
	for i := 8; i < len(elems); i += encGroupSize + 1 {
		if elems[i] != byte(9) {
			result = append(result, Decode(elems[start:i+1]))
			start = i+1
		}
	}
	return result
}

func Decode(elem []byte) []byte {
	result := make([]byte, 0)
	for i := 0; i < len(elem); i += encGroupSize + 1 {
		// 末尾
		if lastBlockSize := int(elem[i+encGroupSize]); lastBlockSize != 9 {
			result = append(result, elem[i:i+lastBlockSize]...)
		} else {
			result = append(result, elem[i:i+encGroupSize]...)
		}
	}
	return result
}
