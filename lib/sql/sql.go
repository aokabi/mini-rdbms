package sql

import (
	"fmt"
	"mini-rdbms/lib/btree"
	"mini-rdbms/lib/buffer"
	"mini-rdbms/lib/disk"

)

const encGroupSize = 8

type simpleTable struct {
	metaPageID  disk.PageID // 初期化時にInvalidPageIDを入れる
	numKeyElems uint
}

type table struct {
	metaPageID  disk.PageID // 初期化時にInvalidPageIDを入れる
	numKeyElems uint
	uniqueIndices []uniqueIndex
}

// schema定義
func NewSimpleTable(numKeyElems uint) *simpleTable {
	return &simpleTable{disk.InvalidPageID, numKeyElems}
}

func NewTable(numKeyElems uint, uniqueIndices []uniqueIndex) *table {
	return &table{disk.InvalidPageID, numKeyElems, uniqueIndices}
}

// 1テーブル1ツリー
// 1ノード1ページ
// 子ノードへの参照は子ノードのpageIDを保持
func (t *simpleTable) Create(bufferPoolManager *buffer.BufferPoolManager) {
	btree := btree.CreateBtree(bufferPoolManager)
	t.metaPageID = btree.MetaPageID
}

func (t *table) Create(bufferPoolManager *buffer.BufferPoolManager) {
	btree := btree.CreateBtree(bufferPoolManager)
	t.metaPageID = btree.MetaPageID

	// create secondary index
	for _, v := range t.uniqueIndices {
		v.Create(bufferPoolManager)
	}
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

func (t *table) Insert(bufferPoolManager *buffer.BufferPoolManager, record [][]byte) {
	tree := btree.NewBtree(bufferPoolManager, t.metaPageID)
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

	for _, v := range t.uniqueIndices {
		v.Insert(bufferPoolManager, record)
	}
}

type uniqueIndex struct {
	metaPageID disk.PageID
	skey []uint // key columns
}

func NewUniqueIndex(skey ...uint) *uniqueIndex {
	return &uniqueIndex{skey: skey}
}
 
func (idx *uniqueIndex) Create(bufferPoolManager *buffer.BufferPoolManager) {
	btree := btree.CreateBtree(bufferPoolManager)
	idx.metaPageID = btree.MetaPageID
}

func (idx *uniqueIndex) Insert(bufferPoolManager *buffer.BufferPoolManager, record[][]byte) {
	btree := btree.NewBtree(bufferPoolManager, idx.metaPageID)
	pkeyEncoded := Encode(record[0])
	skey := make([][]byte, 0)
	for _, v := range idx.skey {
		skey = append(skey, record[int(v)])
	}
	skeyEncoded := EncodeElems(skey)
	btree.Insert(bufferPoolManager, skeyEncoded, pkeyEncoded)
}

// Encode/Decode tuple

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
