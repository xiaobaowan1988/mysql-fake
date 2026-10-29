package storage

import (
	"encoding/binary"
	"errors"
	"io"
)

// Doublewrite Buffer: 数据页落盘前，先把整批页顺序写入 <db>.dw 并 fsync，再写数据文件。
// 如果数据页写到一半崩溃 (页撕裂，校验和不匹配)，启动时用 .dw 中完好的副本覆盖它，
// 之后的 WAL 重放就是在完整的页上进行的。
//
// 槽位格式: magic(4) pageID(4) | page(4096)。槽位只有在数据文件 fsync 之后才会被复用，
// 因此任何可能撕裂的数据页，其副本一定还在 .dw 中。
const (
	dwMagic    = 0x44575221 // "DWR!"
	dwSlots    = 64
	dwSlotSize = 8 + PageSize
)

// WritePage 写一页 (经 Doublewrite)；页号超出当前页数时扩展文件 (崩溃恢复重放日志时会用到)。
func (d *DiskManager) WritePage(p *Page) error { return d.WritePages([]*Page{p}) }

// WritePages 批量写页: 一次 .dw 写入 + 一次 fsync 摊销整批的开销。
func (d *DiskManager) WritePages(pages []*Page) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.writePagesLocked(pages)
}

func (d *DiskManager) writePagesLocked(pages []*Page) error {
	for len(pages) > 0 {
		chunk := pages
		if len(chunk) > dwSlots {
			chunk = chunk[:dwSlots]
		}
		pages = pages[len(chunk):]
		stamped := make([]*Page, len(chunk))
		for i, p := range chunk {
			if p.ID == InvalidPageID {
				return ErrBadPageID
			}
			cp := *p // 不修改调用方的页 (可能是缓冲池帧)
			cp.StampChecksum()
			stamped[i] = &cp
		}
		if !d.NoDoublewrite {
			if d.dwUsed+len(stamped) > dwSlots {
				if err := d.syncDataLocked(); err != nil { // 回绕前必须确保之前的数据页已落盘
					return err
				}
			}
			buf := make([]byte, 0, len(stamped)*dwSlotSize)
			for _, p := range stamped {
				buf = binary.LittleEndian.AppendUint32(buf, dwMagic)
				buf = binary.LittleEndian.AppendUint32(buf, uint32(p.ID))
				buf = append(buf, p.Data[:]...)
			}
			if _, err := d.dw.WriteAt(buf, int64(d.dwUsed)*dwSlotSize); err != nil {
				return err
			}
			if err := d.dw.Sync(); err != nil {
				return err
			}
			d.dwUsed += len(stamped)
		}
		for _, p := range stamped {
			if _, err := d.f.WriteAt(p.Data[:], int64(p.ID)*PageSize); err != nil {
				return err
			}
			if uint32(p.ID) >= d.numPages {
				d.numPages = uint32(p.ID) + 1
				if err := d.writeHeader(); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// recoverDoublewrite 扫描 .dw: 数据页校验失败 (撕裂) 或仍是全零时，用校验和完好且 LSN 最新的副本恢复。
func (d *DiskManager) recoverDoublewrite() error {
	st, err := d.dw.Stat()
	if err != nil {
		return err
	}
	best := map[PageID]*Page{}
	for off := int64(0); off+dwSlotSize <= st.Size(); off += dwSlotSize {
		var slot [dwSlotSize]byte
		if _, err := d.dw.ReadAt(slot[:], off); err != nil && !errors.Is(err, io.EOF) {
			return err
		}
		if binary.LittleEndian.Uint32(slot[0:4]) != dwMagic {
			continue
		}
		id := PageID(binary.LittleEndian.Uint32(slot[4:8]))
		p := &Page{ID: id}
		copy(p.Data[:], slot[8:])
		if id == InvalidPageID || p.IsZero() || !p.ChecksumOK() {
			continue // 槽位本身是写到一半的垃圾，对应的数据页还没被动过
		}
		if cur := best[id]; cur == nil || p.LSN() >= cur.LSN() {
			best[id] = p
		}
	}
	restored := false
	for id, copyPage := range best {
		var cur Page
		n, err := d.f.ReadAt(cur.Data[:], int64(id)*PageSize)
		if err != nil && !errors.Is(err, io.EOF) {
			return err
		}
		for i := n; i < PageSize; i++ {
			cur.Data[i] = 0
		}
		if !cur.ChecksumOK() || cur.IsZero() {
			if _, err := d.f.WriteAt(copyPage.Data[:], int64(id)*PageSize); err != nil {
				return err
			}
			if uint32(id) >= d.numPages {
				d.numPages = uint32(id) + 1
			}
			restored = true
		}
	}
	if restored {
		if err := d.writeHeader(); err != nil {
			return err
		}
	}
	if err := d.f.Sync(); err != nil {
		return err
	}
	d.dwUsed = 0
	return nil
}
