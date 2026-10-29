package storage

import "encoding/binary"

const (
	PageSize       = 4096
	PageHeaderSize = 16
	PayloadSize    = PageSize - PageHeaderSize
)

// PageID 是页在文件中的编号。页 0 是文件头页，因此 0 同时用作"无效页"。
type PageID uint32

const InvalidPageID PageID = 0

type PageType uint8

const (
	PageTypeInvalid  PageType = 0
	PageTypeHeader   PageType = 1
	PageTypeFree     PageType = 2
	PageTypeLeaf     PageType = 3
	PageTypeInternal PageType = 4
)

// Page 布局: type(1) pad(3) pageID(4) lsn(8) | payload
type Page struct {
	ID   PageID
	Data [PageSize]byte
}

func (p *Page) Type() PageType     { return PageType(p.Data[0]) }
func (p *Page) SetType(t PageType) { p.Data[0] = byte(t) }
func (p *Page) LSN() uint64        { return binary.LittleEndian.Uint64(p.Data[8:16]) }
func (p *Page) SetLSN(lsn uint64)  { binary.LittleEndian.PutUint64(p.Data[8:16], lsn) }
func (p *Page) Payload() []byte    { return p.Data[PageHeaderSize:] }

// Reset 清零整页并写入页号。
func (p *Page) Reset(id PageID) {
	p.Data = [PageSize]byte{}
	p.ID = id
	binary.LittleEndian.PutUint32(p.Data[4:8], uint32(id))
}

// PageIO 是上层 (B+ 树、Catalog) 读写页的统一接口。
// Phase 1 由 DiskManager 实现，Phase 3 起由 BufferPool 实现。
type PageIO interface {
	ReadPage(id PageID, p *Page) error
	WritePage(p *Page) error
	AllocatePage() (PageID, error)
	FreePage(id PageID) error
}
