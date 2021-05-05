package lib

import "fmt"

type BufferID uint

type Buffer struct {
	pageID  PageID
	page    Page
	isDirty bool
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
		buffers[i] = Frame{0, &Buffer{}}
	}

	return &BufferPool{buffers: buffers}
}

func (p *BufferPool) useBuffer(bufferID BufferID) *Buffer {
	frame := p.buffers[bufferID]
	frame.usageCount++
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
		if true {
			frame.usageCount--
			consecutivePinned = 0
		} else { // 使用中なら
			consecutivePinned++
			// もしすべてのバッファが使用中なら
			if consecutivePinned >= len(p.buffers) {
				return 0, fmt.Errorf("No available buffers")
			}
		}
	}
	victimID := p.nextVictimID
	p.nextVictimID = BufferID((int(p.nextVictimID) + 1) % len(p.buffers))
	return victimID, nil
}

type BufferPoolManager struct {
	disk      DiskManager
	pool      BufferPool
	pageTable map[PageID]BufferID
}

func NewBufferPoolManager(disk *DiskManager, pool *BufferPool) *BufferPoolManager {
	return &BufferPoolManager{
		disk:      *disk,
		pool:      *pool,
		pageTable: make(map[PageID]BufferID),
	}
}

func (m *BufferPoolManager) createPage() *Buffer {
	pageID := m.disk.allocatePage()
	// バッファに追加する
	bufferID, err := m.pool.evict()
	if err != nil {
		fmt.Println(err)
	}
	frame := m.pool.buffers[bufferID]
	frame.buffer.pageID = pageID
	frame.usageCount = 1
	fmt.Println("bufferID", bufferID)
	m.pageTable[pageID] = bufferID
	return frame.buffer
}

func (m *BufferPoolManager) fetchPage(pageID PageID) (*Buffer, error) {
	// ページテーブルにpageIDのページがあるかどうか
	if bufferID, ok := m.pageTable[pageID]; ok {
		// あるならbufferIDのバッファを貸し出す
		return m.pool.useBuffer(bufferID), nil
	}

	// ないなら
	// 払い出すバッファを決定する
	bufferID, err := m.pool.evict()
	if err != nil {
		return nil, err
	}
	// 払い出すバッファの中身が変更されていて，ディスクの内容が古くなっていたら，ディスクに書き出す
	frame := m.pool.buffers[bufferID]
	buffer := frame.buffer
	evictPageID := buffer.pageID
	if buffer.isDirty {
		m.disk.writePageData(buffer.pageID, buffer.page)
		buffer.isDirty = false
	}
	// バッファにページを読み出す
	buffer.pageID = pageID
	buffer.page = m.disk.readPageData(pageID)
	frame.usageCount = 1
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
		m.disk.writePageData(pageID, frame.buffer.page)
		frame.buffer.isDirty = false
	}
}
