package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

// 文件头页 (页 0) 布局:
//
//	[0:8]   magic "MYFAKEDB"
//	[8:12]  version
//	[12:16] pageSize
//	[16:20] numPages
//	[20:24] freeHead   空闲链表头 (0 表示空)
//	[24:28] catalogRoot Catalog B+ 树根页
//	[28:36] nextTxnID  事务 ID 分配上界 (Phase 4 使用)
const (
	Magic         = "MYFAKEDB"
	FormatVersion = 2
)

var (
	ErrBadMagic   = errors.New("storage: not a database file (bad magic)")
	ErrBadPageID  = errors.New("storage: invalid page id")
	ErrBadVersion = errors.New("storage: unsupported file format")
	ErrChecksum   = errors.New("storage: page checksum mismatch (corrupt or torn page)")
)

type DiskManager struct {
	mu            sync.Mutex
	f             *os.File
	dw            *os.File // Doublewrite 文件
	dwUsed        int      // 自上次数据文件 fsync 以来已占用的 dw 槽位数
	NoDoublewrite bool     // 关闭 Doublewrite (仅用于对比测试)
	numPages      uint32
	freeHead      PageID
	catalogRoot   PageID
	nextTxnID     uint64
}

// OpenDiskManager 打开数据库文件，不存在则创建并初始化文件头。
func OpenDiskManager(path string) (*DiskManager, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	d := &DiskManager{f: f}
	if d.dw, err = os.OpenFile(path+".dw", os.O_RDWR|os.O_CREATE, 0o644); err != nil {
		f.Close()
		return nil, err
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	if st.Size() == 0 {
		d.dw.Truncate(0)
		d.numPages = 1
		d.nextTxnID = 1
		if err := d.writeHeader(); err != nil {
			d.closeFiles()
			return nil, err
		}
		return d, nil
	}
	var buf [PageSize]byte
	if _, err := f.ReadAt(buf[:], 0); err != nil {
		d.closeFiles()
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return nil, ErrBadMagic
		}
		return nil, err
	}
	if string(buf[0:8]) != Magic {
		d.closeFiles()
		return nil, ErrBadMagic
	}
	if binary.LittleEndian.Uint32(buf[8:12]) != FormatVersion ||
		binary.LittleEndian.Uint32(buf[12:16]) != PageSize {
		d.closeFiles()
		return nil, ErrBadVersion
	}
	d.numPages = binary.LittleEndian.Uint32(buf[16:20])
	d.freeHead = PageID(binary.LittleEndian.Uint32(buf[20:24]))
	d.catalogRoot = PageID(binary.LittleEndian.Uint32(buf[24:28]))
	d.nextTxnID = binary.LittleEndian.Uint64(buf[28:36])
	// 崩溃时文件头可能落后于实际文件长度，以文件长度为准。
	if n := uint32((st.Size() + PageSize - 1) / PageSize); n > d.numPages {
		d.numPages = n
	}
	// 必须在 WAL 重放之前修复撕裂页。
	if err := d.recoverDoublewrite(); err != nil {
		d.closeFiles()
		return nil, err
	}
	return d, nil
}

func (d *DiskManager) closeFiles() {
	d.f.Close()
	if d.dw != nil {
		d.dw.Close()
	}
}

func (d *DiskManager) writeHeader() error {
	var buf [PageSize]byte
	copy(buf[0:8], Magic)
	binary.LittleEndian.PutUint32(buf[8:12], FormatVersion)
	binary.LittleEndian.PutUint32(buf[12:16], PageSize)
	binary.LittleEndian.PutUint32(buf[16:20], d.numPages)
	binary.LittleEndian.PutUint32(buf[20:24], uint32(d.freeHead))
	binary.LittleEndian.PutUint32(buf[24:28], uint32(d.catalogRoot))
	binary.LittleEndian.PutUint64(buf[28:36], d.nextTxnID)
	_, err := d.f.WriteAt(buf[:], 0)
	return err
}

// ReadPage 读取页；超出文件末尾 (已分配但从未写过) 的部分按全零处理。
func (d *DiskManager) ReadPage(id PageID, p *Page) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if id == InvalidPageID || uint32(id) >= d.numPages {
		return fmt.Errorf("%w: %d", ErrBadPageID, id)
	}
	n, err := d.f.ReadAt(p.Data[:], int64(id)*PageSize)
	if err != nil && err != io.EOF {
		return err
	}
	for i := n; i < PageSize; i++ {
		p.Data[i] = 0
	}
	p.ID = id
	if !p.ChecksumOK() {
		return fmt.Errorf("%w: page %d", ErrChecksum, id)
	}
	return nil
}

// AllocatePage 优先复用空闲链表中的页，否则扩展文件。返回的页内容全零。
func (d *DiskManager) AllocatePage() (PageID, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	var zero Page
	if d.freeHead != InvalidPageID {
		id := d.freeHead
		var p Page
		if _, err := d.f.ReadAt(p.Data[:], int64(id)*PageSize); err != nil {
			return 0, err
		}
		d.freeHead = PageID(binary.LittleEndian.Uint32(p.Payload()[0:4]))
		if _, err := d.f.WriteAt(zero.Data[:], int64(id)*PageSize); err != nil {
			return 0, err
		}
		return id, d.writeHeader()
	}
	id := PageID(d.numPages)
	d.numPages++
	// 全零页没有校验和，ChecksumOK 把它视为"已分配但从未写入"的合法页。
	if _, err := d.f.WriteAt(zero.Data[:], int64(id)*PageSize); err != nil {
		return 0, err
	}
	return id, d.writeHeader()
}

// MakeFreePage 构造一个指向 next 的空闲页镜像。
func MakeFreePage(id, next PageID) *Page {
	p := &Page{}
	p.Reset(id)
	p.SetType(PageTypeFree)
	binary.LittleEndian.PutUint32(p.Payload()[0:4], uint32(next))
	return p
}

// FreePage 把页放回空闲链表头部。
func (d *DiskManager) FreePage(id PageID) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if id == InvalidPageID || uint32(id) >= d.numPages {
		return fmt.Errorf("%w: %d", ErrBadPageID, id)
	}
	if err := d.writePagesLocked([]*Page{MakeFreePage(id, d.freeHead)}); err != nil {
		return err
	}
	d.freeHead = id
	return d.writeHeader()
}

// FreeHead 返回空闲链表头。
func (d *DiskManager) FreeHead() PageID {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.freeHead
}

// LinkFree 仅更新文件头，把已经写好空闲镜像的页挂到链表头 (供 BufferPool 使用)。
func (d *DiskManager) LinkFree(id PageID) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.freeHead = id
	return d.writeHeader()
}

func (d *DiskManager) NumPages() uint32 {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.numPages
}

func (d *DiskManager) CatalogRoot() PageID {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.catalogRoot
}

func (d *DiskManager) SetCatalogRoot(id PageID) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.catalogRoot = id
	return d.writeHeader()
}

func (d *DiskManager) NextTxnID() uint64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.nextTxnID
}

func (d *DiskManager) SetNextTxnID(v uint64) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.nextTxnID = v
	return d.writeHeader()
}

// Sync 把数据文件落盘；此后 Doublewrite 槽位可以安全复用。
func (d *DiskManager) Sync() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.syncDataLocked()
}

func (d *DiskManager) syncDataLocked() error {
	if err := d.f.Sync(); err != nil {
		return err
	}
	d.dwUsed = 0
	return nil
}

func (d *DiskManager) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	err := d.f.Sync()
	if e2 := d.f.Close(); err == nil {
		err = e2
	}
	if e2 := d.dw.Close(); err == nil {
		err = e2
	}
	return err
}

// ReadPageRaw 供崩溃恢复使用: 不检查页号上界，读不到的部分按全零处理。
func (d *DiskManager) ReadPageRaw(id PageID, p *Page) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if id == InvalidPageID {
		return fmt.Errorf("%w: %d", ErrBadPageID, id)
	}
	n, err := d.f.ReadAt(p.Data[:], int64(id)*PageSize)
	if err != nil && err != io.EOF {
		return err
	}
	for i := n; i < PageSize; i++ {
		p.Data[i] = 0
	}
	p.ID = id
	if !p.ChecksumOK() {
		return fmt.Errorf("%w: page %d", ErrChecksum, id) // 数据仍已读入 p，由调用方决定如何处理
	}
	return nil
}

// ReadPageConsistent 供热备份使用: 页可能正被并发写入 (读到撕裂的内容)，校验和失败时重读几次。
func (d *DiskManager) ReadPageConsistent(id PageID, p *Page) error {
	var err error
	for i := 0; i < 20; i++ {
		if err = d.ReadPageRaw(id, p); err == nil {
			return nil
		}
		if !errors.Is(err, ErrChecksum) {
			return err
		}
		time.Sleep(time.Millisecond)
	}
	return err
}

// ReadHeader 读取文件头页 (页 0) 的原始内容。
func (d *DiskManager) ReadHeader(buf *[PageSize]byte) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	_, err := d.f.ReadAt(buf[:], 0)
	return err
}
