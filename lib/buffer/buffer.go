package buffer

import (
	"errors"
	"fmt"
	"io"
	"mini-rdbms/lib/disk"
)

type BufferID uint
type PageID = disk.PageID

type Buffer struct {
	PageID  PageID
	page    disk.Page // data
	ref     uint // 参照カウント
	isDirty bool // pageが更新されていて，disk上のデータと異なっているときtrue
}

func (b *Buffer) SetPage(r io.Reader) error {
	data := make([]byte, disk.PageSize)
	n, err := r.Read(data)
	if err != nil {
		return err
	}

	if n > disk.PageSize {
		return errors.New("over size")
	}

	copy(b.page[:], data)
	b.isDirty = true

	return nil
}

// bufferにあるページからデータを読み出す
func (b *Buffer) GetPage(w io.Writer) error {
	_, err := w.Write(b.page[:])
	if err != nil {
		return err
	}

	return nil
}

func (b *Buffer) Close() {
	// dereference
	b.ref--
}

type Frame struct {
	usageCount uint // 使用頻度が高ければどんどん数字が増えていく
	buffer     *Buffer
}

type BufferPool struct {
	buffers      []Frame
	nextVictimID BufferID
}

func NewBufferPool(poolSize uint) *BufferPool {
	buffers := make([]Frame, poolSize)
	for i := range buffers {
		buffers[i] = Frame{
			0,
			&Buffer{
				disk.InvalidPageID,
				disk.Page{},
				0,
				false,
			},
		}
	}

	return &BufferPool{buffers: buffers}
}

func (p *BufferPool) useBuffer(bufferID BufferID) *Buffer {
	frame := p.buffers[bufferID]
	frame.usageCount++
	frame.buffer.ref++
	return frame.buffer
}

// 貸出中でないbufferを見つける(clock-sweepアルゴリズム)
func (p *BufferPool) evict() (BufferID, error) {
	consecutivePinned := 0
	for {
		frame := p.buffers[p.nextVictimID]
		if frame.usageCount == 0 {
			break
		}

		// 使用中でなければ
		// TODO: goで参照カウント見るのわからないので一旦置いておく
		if frame.buffer.ref == 0 {
			frame.usageCount--
			consecutivePinned = 0
		} else { // 使用中なら
			consecutivePinned++
			// もしすべてのバッファが使用中なら
			if consecutivePinned >= len(p.buffers) {
				return 0, fmt.Errorf("no available buffers")
			}
		}
	}
	victimID := p.nextVictimID
	p.nextVictimID = BufferID((int(p.nextVictimID) + 1) % len(p.buffers))
	return victimID, nil
}

type BufferPoolManager struct {
	disk      disk.DiskManager
	pool      BufferPool
	pageTable map[PageID]BufferID
}

func NewBufferPoolManager(disk *disk.DiskManager, pool *BufferPool) *BufferPoolManager {
	return &BufferPoolManager{
		disk:      *disk,
		pool:      *pool,
		pageTable: make(map[PageID]BufferID),
	}
}

func (m *BufferPoolManager) CreatePage() *Buffer {
	pageID := m.disk.AllocatePage()
	// バッファに追加する
	bufferID, err := m.pool.evict()
	if err != nil {
		fmt.Println(err)
	}

	// 払い出すバッファの中身が変更されていて，ディスクの内容が古くなっていたら，ディスクに書き出す
	frame := m.pool.buffers[bufferID]
	buffer := frame.buffer
	evictPageID := buffer.PageID
	if buffer.isDirty {
		m.disk.WritePageData(buffer.PageID, buffer.page)
		buffer.isDirty = false
	}
	frame.buffer.page = [disk.PageSize]byte{}
	frame.buffer.PageID = pageID
	frame.usageCount = 1
	frame.buffer.ref = 1

	delete(m.pageTable, evictPageID)
	m.pageTable[pageID] = bufferID
	return frame.buffer
}

func (m *BufferPoolManager) FetchPage(pageID PageID) (*Buffer, error) {
	if pageID == disk.InvalidPageID {
		return nil, errors.New("invalid pageID")
	}

	// ページテーブルにpageIDのページがあるかどうか
	if bufferID, ok := m.pageTable[pageID]; ok {
		// あるならbufferIDのバッファを貸し出す
		return m.pool.useBuffer(bufferID), nil
	}

	// ページテーブル上にないなら
	// 払い出すバッファを決定する
	bufferID, err := m.pool.evict()
	if err != nil {
		return nil, err
	}
	// 払い出すバッファの中身が変更されていて，ディスクの内容が古くなっていたら，ディスクに書き出す
	frame := m.pool.buffers[bufferID]
	buffer := frame.buffer
	evictPageID := buffer.PageID
	if buffer.isDirty {
		m.disk.WritePageData(buffer.PageID, buffer.page)
		buffer.isDirty = false
	}

	// バッファにページを読み出す
	buffer.PageID = pageID
	buffer.page = m.disk.ReadPageData(pageID)
	frame.usageCount = 1
	buffer.ref = 1

	// ページテーブルを更新する
	delete(m.pageTable, evictPageID)
	m.pageTable[pageID] = bufferID

	return buffer, nil
}

//
func (m *BufferPoolManager) Flush() {
	fmt.Println(m.pageTable)
	for pageID, bufferID := range m.pageTable {
		frame := m.pool.buffers[bufferID]
		m.disk.WritePageData(pageID, frame.buffer.page)
		frame.buffer.isDirty = false
	}
}
