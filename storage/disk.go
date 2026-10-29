package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
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
	FormatVersion = 1
)

var (
	ErrBadMagic   = errors.New("storage: not a database file (bad magic)")
	ErrBadPageID  = errors.New("storage: invalid page id")
	ErrBadVersion = errors.New("storage: unsupported file format")
)

type DiskManager struct {
	mu          sync.Mutex
	f           *os.File
	numPages    uint32
	freeHead    PageID
	catalogRoot PageID
	nextTxnID   uint64
}

// OpenDiskManager 打开数据库文件，不存在则创建并初始化文件头。
func OpenDiskManager(path string) (*DiskManager, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	d := &DiskManager{f: f}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	if st.Size() == 0 {
		d.numPages = 1
		d.nextTxnID = 1
		if err := d.writeHeader(); err != nil {
			f.Close()
			return nil, err
		}
		return d, nil
	}
	var buf [PageSize]byte
	if _, err := f.ReadAt(buf[:], 0); err != nil {
		f.Close()
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return nil, ErrBadMagic
		}
		return nil, err
	}
	if string(buf[0:8]) != Magic {
		f.Close()
		return nil, ErrBadMagic
	}
	if binary.LittleEndian.Uint32(buf[8:12]) != FormatVersion ||
		binary.LittleEndian.Uint32(buf[12:16]) != PageSize {
		f.Close()
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
	return d, nil
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
	return nil
}

// WritePage 写页；若页号超出当前页数则扩展文件 (崩溃恢复重放日志时会用到)。
func (d *DiskManager) WritePage(p *Page) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if p.ID == InvalidPageID {
		return fmt.Errorf("%w: %d", ErrBadPageID, p.ID)
	}
	if _, err := d.f.WriteAt(p.Data[:], int64(p.ID)*PageSize); err != nil {
		return err
	}
	if uint32(p.ID) >= d.numPages {
		d.numPages = uint32(p.ID) + 1
		return d.writeHeader()
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
		zero.Reset(id)
		if _, err := d.f.WriteAt(zero.Data[:], int64(id)*PageSize); err != nil {
			return 0, err
		}
		return id, d.writeHeader()
	}
	id := PageID(d.numPages)
	d.numPages++
	zero.Reset(id)
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
	p := MakeFreePage(id, d.freeHead)
	if _, err := d.f.WriteAt(p.Data[:], int64(id)*PageSize); err != nil {
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

func (d *DiskManager) Sync() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.f.Sync()
}

func (d *DiskManager) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.f.Sync(); err != nil {
		d.f.Close()
		return err
	}
	return d.f.Close()
}
