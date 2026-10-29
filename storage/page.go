package storage

import (
	"encoding/binary"
	"hash/crc32"
)

const (
	PageSize       = 4096
	PageHeaderSize = 20
	PayloadSize    = PageSize - PageHeaderSize

	checksumOff = 16
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

// Page 布局: type(1) pad(3) pageID(4) lsn(8) checksum(4) | payload
// checksum 是整页 (校验和字段按 0 计) 的 CRC32，由 DiskManager 在落盘时写入、读盘时校验。
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

// ComputeChecksum 计算页的 CRC32 (校验和字段按 0 计)。
func (p *Page) ComputeChecksum() uint32 {
	h := crc32.NewIEEE()
	h.Write(p.Data[:checksumOff])
	h.Write([]byte{0, 0, 0, 0})
	h.Write(p.Data[checksumOff+4:])
	return h.Sum32()
}

// StampChecksum 把当前内容的校验和写入页头。
func (p *Page) StampChecksum() {
	binary.LittleEndian.PutUint32(p.Data[checksumOff:], p.ComputeChecksum())
}

// IsZero 判断整页是否全零 (已分配但从未写入)。
func (p *Page) IsZero() bool {
	for _, b := range p.Data {
		if b != 0 {
			return false
		}
	}
	return true
}

// ChecksumOK 校验页内容；全零页视为合法。
func (p *Page) ChecksumOK() bool {
	if binary.LittleEndian.Uint32(p.Data[checksumOff:]) == p.ComputeChecksum() {
		return true
	}
	return p.IsZero()
}
