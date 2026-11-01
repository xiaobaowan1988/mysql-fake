// Package buffer 实现缓冲池: 页帧管理、LRU 淘汰、页级读写锁，并在写页时遵循 WAL 协议。
package buffer

import (
	"container/list"
	"errors"
	"fmt"
	"sync"
	"time"

	"mysqlfake/metrics"
	"mysqlfake/storage"
	"mysqlfake/wal"
)

var ErrPoolFull = errors.New("buffer: all frames are pinned")

// Frame 是缓冲池中的一个页帧。Latch 保护 Page 内容的并发读写。
type Frame struct {
	Page     storage.Page
	pin      int
	dirty    bool
	needFull bool   // 新分配/释放的页，首次写入记录整页镜像
	recLSN   uint64 // 自上次刷盘后第一次弄脏该页时 (下界) 的 LSN，受 Pool.mu 保护
	seq      uint64 // 每次写入加一，Fuzzy Checkpoint 用它判断刷盘期间页是否又被修改，受 Pool.mu 保护
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
	lru    *list.List   // 未被 pin 的帧，Front 为最近使用
	ckpt   sync.RWMutex // 写入持读锁；"全停顿"Checkpoint 持写锁。Fuzzy Checkpoint 不使用它。
	hold   int          // >0 时暂停 WAL 截断 (热备份期间)

	cpMu    sync.Mutex // 串行化 Fuzzy Checkpoint
	cpCount int

	slowFlush func() // 测试钩子: 在 Fuzzy Checkpoint 写盘前调用 (不持任何锁)
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
		metrics.BufferHits.Inc()
		return f, nil
	}
	metrics.BufferMisses.Inc()
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
	f.dirty, f.needFull, f.pin, f.recLSN = false, false, 1, 0
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
	p.unpinLocked(f)
}

func (p *Pool) unpinLocked(f *Frame) {
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
	metrics.BufferEvictions.Inc()
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
// 顺序: 持页写锁 -> 登记"脏" (recLSN 取当前 NextLSN 作为下界，保证早于将要写的日志) -> 写日志 -> 修改内存页。
func (p *Pool) writeFrame(f *Frame, src *storage.Page) {
	f.WLatch()
	defer f.WUnlatch()
	newData := src.Data
	copy(newData[8:20], f.Page.Data[8:20]) // 页 LSN 由日志决定、校验和在落盘时生成，二者都不参与 diff
	var spans [][2]int
	if f.needFull {
		spans = [][2]int{{0, storage.PageSize}}
	} else {
		spans = diff(f.Page.Data[:], newData[:])
	}
	if len(spans) == 0 {
		return
	}
	p.mu.Lock()
	f.seq++
	if !f.dirty {
		f.dirty = true
		f.recLSN = p.log.NextLSN()
	}
	f.needFull = false
	p.mu.Unlock()
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

// FlushAll 把所有脏页写回磁盘 (调用方需保证没有并发写入，例如持有 ckpt 写锁)。
func (p *Pool) FlushAll() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	var batch []*storage.Page
	var frames []*Frame
	var maxLSN uint64
	for f := range p.frames {
		if f.dirty {
			pg := f.Page
			batch = append(batch, &pg)
			frames = append(frames, f)
			if l := pg.LSN(); l > maxLSN {
				maxLSN = l
			}
		}
	}
	if len(batch) > 0 {
		if err := p.log.Flush(maxLSN); err != nil {
			return err
		}
		if err := p.disk.WritePages(batch); err != nil {
			return err
		}
		for _, f := range frames {
			f.dirty = false
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

// LogUndo 把一条逻辑 undo 记录写入日志 (不刷盘)，返回它的 LSN。
// WAL 规则: 必须先于它所描述的数据修改调用，这样任何已落盘的页修改，其 undo 记录一定已先落盘。
func (p *Pool) LogUndo(txnID uint64, info wal.UndoInfo) uint64 {
	return p.log.AppendUndo(&wal.Record{TxnID: txnID, After: info.Marshal()})
}

// Abort 写入回滚记录 (不需要立即刷盘)。
func (p *Pool) Abort(txnID uint64) {
	p.log.Append(&wal.Record{Type: wal.RecAbort, TxnID: txnID})
}

// HoldLogTruncation 暂停 WAL 截断 (热备份期间需要保留自备份起点以来的全部日志)。返回恢复函数。
func (p *Pool) HoldLogTruncation() (release func()) {
	p.mu.Lock()
	p.hold++
	p.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			p.mu.Lock()
			p.hold--
			p.mu.Unlock()
		})
	}
}

// FuzzyCheckpoint 在不阻塞写入的前提下刷脏页: 分批对脏页做"持读锁拷贝 -> 先刷日志 -> 批量写盘 (Doublewrite)"，
// 刷盘期间又被修改的页保持为脏。全部完成并 fsync 后，Checkpoint LSN = 仍脏页的最小 recLSN
// (没有脏页则为当前 NextLSN)，WAL 中 LSN 小于它的前缀被丢弃 (热备份期间不丢弃)。
// 返回 Checkpoint LSN。
func (p *Pool) FuzzyCheckpoint() (uint64, error) {
	p.cpMu.Lock()
	defer p.cpMu.Unlock()
	nextLSN := p.log.NextLSN() // 之后才变脏的页，其 recLSN 一定 >= 它

	p.mu.Lock()
	var frames []*Frame
	for f := range p.frames {
		if f.dirty {
			p.pinLocked(f)
			frames = append(frames, f)
		}
	}
	p.mu.Unlock()

	const batchSize = 32
	var firstErr error
	for i := 0; i < len(frames); i += batchSize {
		end := i + batchSize
		if end > len(frames) {
			end = len(frames)
		}
		batch := frames[i:end]
		copies := make([]*storage.Page, len(batch))
		seqs := make([]uint64, len(batch))
		var maxLSN uint64
		for j, f := range batch {
			f.RLatch()
			p.mu.Lock()
			seqs[j] = f.seq
			p.mu.Unlock()
			pg := f.Page
			f.RUnlatch()
			copies[j] = &pg
			if l := pg.LSN(); l > maxLSN {
				maxLSN = l
			}
		}
		var err error
		if p.slowFlush != nil {
			p.slowFlush()
		}
		if err = p.log.Flush(maxLSN); err == nil {
			err = p.disk.WritePages(copies)
		}
		p.mu.Lock()
		for j, f := range batch {
			if err == nil {
				if f.seq == seqs[j] {
					f.dirty = false
				} else if r := copies[j].LSN() + 1; r > f.recLSN {
					f.recLSN = r // 刷盘后又被修改: 之后的修改 LSN 一定更大
				}
			}
			p.unpinLocked(f)
		}
		p.mu.Unlock()
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if firstErr != nil {
		return 0, firstErr
	}
	if err := p.disk.Sync(); err != nil { // 驱逐路径写出的页也要落盘后才能丢日志
		return 0, err
	}
	p.mu.Lock()
	ckpt := nextLSN
	for f := range p.frames {
		if f.dirty && f.recLSN < ckpt {
			ckpt = f.recLSN
		}
	}
	hold := p.hold > 0
	p.cpCount++
	p.mu.Unlock()
	metrics.Checkpoints.Inc()
	if !hold {
		if _, err := p.log.TruncateBefore(ckpt); err != nil {
			return 0, err
		}
	}
	return ckpt, nil
}

// CheckpointCount 返回已完成的 Fuzzy Checkpoint 次数。
func (p *Pool) CheckpointCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.cpCount
}

// StartCheckpointer 启动后台 Fuzzy Checkpoint: 每隔 interval，或日志超过 logBytes 时执行一次。
// 返回停止函数 (会等待后台协程退出)。
func (p *Pool) StartCheckpointer(interval time.Duration, logBytes int64) (stop func()) {
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		tick := time.NewTicker(interval / 10)
		defer tick.Stop()
		last := time.Now()
		for {
			select {
			case <-done:
				return
			case <-tick.C:
				if time.Since(last) >= interval || (logBytes > 0 && p.log.Size() >= logBytes) {
					p.FuzzyCheckpoint()
					last = time.Now()
				}
			}
		}
	}()
	var once sync.Once
	return func() {
		once.Do(func() {
			close(done)
			wg.Wait()
		})
	}
}
