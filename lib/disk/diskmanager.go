package disk

import (
	"encoding/binary"
	"io"
	"math"
	"os"
)

const (
	InvalidPageID = math.MaxUint64
	PageSize      = 4096
)

// Pageに関しては表記が面倒だからというだけで[pageSize]byteと別の型として扱わなくても良い気がする
// なぜなら[pageSize]byteなんていう型はそうそう出てこないから誤って代入するということもないだろう
// またPage <-> []byte の変換がaliasだと楽という理由もある
// とりあえず上の話は忘れて，Pageはnodeのポインタにする
// なぜならbyte配列だとnodeのデータと同期しないから
// またまた変更でmetaもいれたいのでinterfaceにした
type Page [PageSize]byte

type PageID uint

func getPageID(b []byte) PageID {
	return PageID(binary.BigEndian.Uint64(b))
}

func (i PageID) Bytes() []byte {
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
	nextPageID := PageID(size / PageSize)
	return &DiskManager{&dataFile, nextPageID}
}

func Open(path string) *DiskManager {
	heapFile, _ := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0755)
	return newDisManager(*heapFile)
}

func (d *DiskManager) AllocatePage() PageID {
	pageID := d.nextPageID
	d.nextPageID++
	return PageID(pageID)
}

func (d *DiskManager) ReadPageData(pageID PageID) Page {
	offset := int64(PageSize * pageID)
	d.heapFile.Seek(offset, io.SeekStart)
	var data [PageSize]byte
	d.heapFile.Read(data[:])
	// reader := bytes.NewReader(data[:])
	return data
}

func (d *DiskManager) WritePageData(pageID PageID, page Page) {
	offset := int64(PageSize * pageID)
	d.heapFile.Seek(offset, io.SeekStart)
	// buf := new(bytes.Buffer)
	// gob.NewEncoder(buf).Encode(page)
	// binary.Write(buf, binary.LittleEndian, *page)
	// var data [PageSize]byte
	// copy(data[:], buf.Bytes())
	d.heapFile.Write(page[:])
}
