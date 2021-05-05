package lib

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"io"
	"math"
	"os"
)

const (
	invalidPageID = math.MaxUint64
	pageSize      = 4096
)

// Pageに関しては表記が面倒だからというだけで[pageSize]byteと別の型として扱わなくても良い気がする
// なぜなら[pageSize]byteなんていう型はそうそう出てこないから誤って代入するということもないだろう
// またPage <-> []byte の変換がaliasだと楽という理由もある
// とりあえず上の話は忘れて，Pageはnodeのポインタにする
// なぜならbyte配列だとnodeのデータと同期しないから
// またまた変更でmetaもいれたいのでinterfaceにした
type Page = interface{}

type PageID uint

func getPageID(b []byte) PageID {
	return PageID(binary.BigEndian.Uint64(b))
}

func (i PageID) bytes() []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(i))
	return b
}

type DiskManager struct {
	heapFile   io.ReadWriteSeeker
	nextPageID PageID
}

func newDisManager(dataFile os.File) *DiskManager {
	info, _ := dataFile.Stat()
	size := info.Size()
	nextPageID := PageID(size / pageSize)
	return &DiskManager{&dataFile, nextPageID}
}

func Open(path string) *DiskManager {
	heapFile, _ := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0755)
	return newDisManager(*heapFile)
}

func (d *DiskManager) allocatePage() PageID {
	pageID := d.nextPageID
	d.nextPageID++
	return PageID(pageID)
}

func (d *DiskManager) readPageData(pageID PageID) Page {
	offset := int64(pageSize * pageID)
	d.heapFile.Seek(offset, io.SeekStart)
	var data [pageSize]byte
	d.heapFile.Read(data[:])
	reader := bytes.NewReader(data[:])
	if pageID == 0 {
		page := meta{}
		gob.NewDecoder(reader).Decode(&page)
		return &page
	} else {
		page := node{}
		gob.NewDecoder(reader).Decode(&page)
		return &page
	}
}

func (d *DiskManager) writePageData(pageID PageID, page Page) {
	offset := int64(pageSize * pageID)
	d.heapFile.Seek(offset, io.SeekStart)
	buf := new(bytes.Buffer)
	gob.NewEncoder(buf).Encode(page)
	// binary.Write(buf, binary.LittleEndian, *page)
	var data [pageSize]byte
	copy(data[:], buf.Bytes())
	d.heapFile.Write(data[:])
}
