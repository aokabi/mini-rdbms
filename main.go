package main

import (
	"io"
	"os"
)

const pageSize = 4096

type PageID uint

type DiskManager struct {
	heapFile io.ReadWriteSeeker
	nextPageID PageID
}

func newDisManager(dataFile os.File) *DiskManager {
	info, _ := dataFile.Stat()
	size := info.Size()
	nextPageID := PageID(size / pageSize)
	return &DiskManager{&dataFile, nextPageID}
}

func Open(path string) *DiskManager{
	heapFile, _ := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0755)
	return newDisManager(*heapFile)
}

func (d *DiskManager) allocatePage() PageID{
	pageID := d.nextPageID
	d.nextPageID++
	return PageID(pageID)
}

func (d *DiskManager) readPageData(pageID PageID) [pageSize]byte {
	offset := int64(pageSize * pageID)
	d.heapFile.Seek(offset, io.SeekStart)
	var data [pageSize]byte
	d.heapFile.Read(data[:])
	return data
} 

func (d *DiskManager) writePageData(pageID PageID, data [pageSize]byte) {
	offset := int64(pageSize * pageID)
	d.heapFile.Seek(offset, io.SeekStart)
	d.heapFile.Write(data[:])
} 

func main() {

}
