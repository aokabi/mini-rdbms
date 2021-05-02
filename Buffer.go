package main

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
			return p.nextVictimID, nil
		}

		// 使用中でなければ
		if true {
			frame.usageCount--
			consecutivePinned = 0
		} else { // 使用中なら
			// もしすべてのバッファが使用中なら
			consecutivePinned++
			if consecutivePinned >= len(p.buffers) {
				return 0, fmt.Errorf("No available buffers")
			}
		}
		p.nextVictimID = BufferID((int(p.nextVictimID) + 1) % len(p.buffers))
	}
}

type BufferPoolManager struct {
	disk      DiskManager
	pool      BufferPool
	pageTable map[PageID]BufferID
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
