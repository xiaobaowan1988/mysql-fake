// Package buffer 实现缓冲池: 页帧管理、LRU 淘汰、页级读写锁，并在写页时遵循 WAL 协议。
package buffer

import (
	"container/list"
	"errors"
	"fmt"
	"sync"

	"mysqlfake/storage"
	"mysqlfake/wal"
)

var ErrPoolFull = errors.New("buffer: all frames are pinned")

// Frame 是缓冲池中的一个页帧。Latch 保护 Page 内容的并发读写。
type Frame struct {
	Page     storage.Page
	pin      int
	dirty    bool
	needFull bool // 新分配/释放的页，首次写入记录整页镜像
	latch    sync.RWMutex
	lru      *list.Element
}

func (f *Frame) RLatch()   { f.latch.RLock() }
func (f *Frame) RUnlatch() { f.latch.RUnlock() }
func (f *Frame) WLatch()   { f.latch.Lock() }
func (f *Frame) WUnlatch() { f.latch.Unlock() }

// Pool 实现 storage.PageIO。所有读写都经过缓冲池，脏页只在淘汰/Checkpoint 时落盘。
type Pool struct {
	mu     sync.Mutex
	disk   *storage.DiskManager
	log    *wal.LogManager
	table  map[storage.PageID]*Frame
	frames map[*Frame]storage.PageID // 每个帧当前缓存的页 (用于遍历)
	free   []*Frame
	lru    *list.List // 未被 pin 的帧，Front 为最近使用
	ckpt   sync.RWMutex
}

var _ storage.PageIO = (*Pool)(nil)

func New(disk *storage.DiskManager, log *wal.LogManager, capacity int) *Pool {
	if capacity < 2 {
		capacity = 2
	}
	p := &Pool{
		disk:   disk,
		log:    log,
		table:  map[storage.PageID]*Frame{},
		frames: map[*Frame]storage.PageID{},
		lru:    list.New(),
	}
	for i := 0; i < capacity; i++ {
		p.free = append(p.free, &Frame{})
	}
	return p
}

// FetchPage 读入并 pin 一页。使用完必须调用 Unpin。
func (p *Pool) FetchPage(id storage.PageID) (*Frame, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.fetchLocked(id)
}

func (p *Pool) fetchLocked(id storage.PageID) (*Frame, error) {
	if f, ok := p.table[id]; ok {
		p.pinLocked(f)
		return f, nil
	}
	f, err := p.victimLocked()
	if err != nil {
		return nil, err
	}
	if err := p.disk.ReadPage(id, &f.Page); err != nil {
		p.free = append(p.free, f)
		return nil, err
	}
	p.installLocked(f, id)
	return f, nil
}

func (p *Pool) installLocked(f *Frame, id storage.PageID) {
	f.dirty, f.needFull, f.pin = false, false, 1
	p.table[id] = f
	p.frames[f] = id
}

func (p *Pool) pinLocked(f *Frame) {
	if f.pin == 0 && f.lru != nil {
		p.lru.Remove(f.lru)
		f.lru = nil
	}
	f.pin++
}

// Unpin 释放一次 pin；pin 归零的帧进入 LRU 候选。
func (p *Pool) Unpin(f *Frame) {
	p.mu.Lock()
	defer p.mu.Unlock()
	f.pin--
	if f.pin == 0 {
		f.lru = p.lru.PushFront(f)
	}
}

// victimLocked 取得一个可用的空帧: 先用空闲帧，否则淘汰 LRU 末尾的帧 (脏页先刷盘)。
func (p *Pool) victimLocked() (*Frame, error) {
	if n := len(p.free); n > 0 {
		f := p.free[n-1]
		p.free = p.free[:n-1]
		return f, nil
	}
	e := p.lru.Back()
	if e == nil {
		return nil, ErrPoolFull
	}
	f := e.Value.(*Frame)
	if f.dirty {
		if err := p.flushFrameLocked(f); err != nil {
			return nil, err
		}
	}
	p.lru.Remove(e)
	f.lru = nil
	id := p.frames[f]
	delete(p.table, id)
	delete(p.frames, f)
	return f, nil
}

// flushFrameLocked 遵循 WAL: 先把日志刷到页 LSN，再写数据页。
func (p *Pool) flushFrameLocked(f *Frame) error {
	if err := p.log.Flush(f.Page.LSN()); err != nil {
		return err
	}
	if err := p.disk.WritePage(&f.Page); err != nil {
		return err
	}
	f.dirty = false
	return nil
}

// ReadPage 实现 storage.PageIO: 拷贝页内容到 dst。
func (p *Pool) ReadPage(id storage.PageID, dst *storage.Page) error {
	f, err := p.FetchPage(id)
	if err != nil {
		return err
	}
	f.RLatch()
	dst.Data = f.Page.Data
	f.RUnlatch()
	dst.ID = id
	p.Unpin(f)
	return nil
}

// WritePage 实现 storage.PageIO: 计算与帧内旧内容的差异，先写日志，再修改内存页。
func (p *Pool) WritePage(src *storage.Page) error {
	p.ckpt.RLock()
	defer p.ckpt.RUnlock()
	f, err := p.FetchPage(src.ID)
	if err != nil {
		return err
	}
	defer p.Unpin(f)
	p.writeFrame(f, src)
	return nil
}

// writeFrame 要求调用方已 pin 帧并持有 ckpt 读锁。
func (p *Pool) writeFrame(f *Frame, src *storage.Page) {
	f.WLatch()
	defer f.WUnlatch()
	newData := src.Data
	copy(newData[8:16], f.Page.Data[8:16]) // 页 LSN 由日志决定，不参与 diff
	var spans [][2]int
	if f.needFull {
		spans = [][2]int{{0, storage.PageSize}}
	} else {
		spans = diff(f.Page.Data[:], newData[:])
	}
	if len(spans) == 0 {
		return
	}
	var lsn uint64
	for _, s := range spans {
		lsn = p.log.Append(&wal.Record{
			Type:   wal.RecUpdate,
			PageID: src.ID,
			Offset: uint16(s[0]),
			Before: append([]byte(nil), f.Page.Data[s[0]:s[1]]...),
			After:  append([]byte(nil), newData[s[0]:s[1]]...),
		})
	}
	f.Page.Data = newData
	f.Page.SetLSN(lsn)
	f.dirty, f.needFull = true, false
}

// diff 返回两页之间不同的字节区间；间隔很小的区间合并以减少日志条数。
func diff(old, cur []byte) [][2]int {
	const mergeGap = 16
	var spans [][2]int
	i := 0
	for i < len(old) {
		if old[i] == cur[i] {
			i++
			continue
		}
		j := i + 1
		last := i // 最后一个不同的字节
		for j < len(old) && j-last <= mergeGap {
			if old[j] != cur[j] {
				last = j
			}
			j++
		}
		spans = append(spans, [2]int{i, last + 1})
		i = last + 1
	}
	return spans
}

// AllocatePage 分配新页 (内容全零)。该页首次写入会记录整页镜像。
func (p *Pool) AllocatePage() (storage.PageID, error) {
	id, err := p.disk.AllocatePage()
	if err != nil {
		return 0, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if old, ok := p.table[id]; ok { // 理论上不会命中: 被释放的页已移出缓存
		return 0, fmt.Errorf("buffer: allocated page %d is still cached (frame %p)", id, old)
	}
	f, err := p.victimLocked()
	if err != nil {
		return 0, err
	}
	f.Page.Reset(id)
	p.installLocked(f, id)
	f.needFull = true
	f.pin = 0
	f.lru = p.lru.PushFront(f)
	return id, nil
}

// FreePage 释放页: 以整页镜像记录"空闲页"内容并刷盘，移出缓存，再把它挂到空闲链表。
func (p *Pool) FreePage(id storage.PageID) error {
	p.ckpt.RLock()
	defer p.ckpt.RUnlock()
	f, err := p.FetchPage(id) // 保持 pin 直到页被移出缓存，避免中途被淘汰
	if err != nil {
		return err
	}
	f.WLatch()
	f.needFull = true
	f.WUnlatch()
	p.writeFrame(f, storage.MakeFreePage(id, p.disk.FreeHead()))
	p.mu.Lock()
	if f.pin != 1 {
		p.mu.Unlock()
		p.Unpin(f)
		return fmt.Errorf("buffer: cannot free page %d: still in use", id)
	}
	if err := p.flushFrameLocked(f); err != nil {
		p.mu.Unlock()
		p.Unpin(f)
		return err
	}
	f.pin = 0
	delete(p.table, id)
	delete(p.frames, f)
	p.free = append(p.free, f)
	p.mu.Unlock()
	return p.disk.LinkFree(id)
}

// Commit 写入提交记录并强制日志落盘 (持久性)。
func (p *Pool) Commit(txnID uint64) error {
	lsn := p.log.Append(&wal.Record{Type: wal.RecCommit, TxnID: txnID})
	return p.log.Flush(lsn)
}

// FlushAll 把所有脏页写回磁盘。
func (p *Pool) FlushAll() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	for f := range p.frames {
		if f.dirty {
			if err := p.flushFrameLocked(f); err != nil {
				return err
			}
		}
	}
	return p.disk.Sync()
}

// Checkpoint 刷全部脏页后截断日志。
func (p *Pool) Checkpoint() error {
	p.ckpt.Lock()
	defer p.ckpt.Unlock()
	if err := p.log.FlushAll(); err != nil {
		return err
	}
	if err := p.FlushAll(); err != nil {
		return err
	}
	return p.log.Truncate()
}

// DirtyCount 返回脏页数 (测试用)。
func (p *Pool) DirtyCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	n := 0
	for f := range p.frames {
		if f.dirty {
			n++
		}
	}
	return n
}

// Abort 写入回滚记录 (不需要立即刷盘)。
func (p *Pool) Abort(txnID uint64) {
	p.log.Append(&wal.Record{Type: wal.RecAbort, TxnID: txnID})
}
