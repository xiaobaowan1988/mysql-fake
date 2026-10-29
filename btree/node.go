// Package btree 实现建立在 storage.PageIO 之上的磁盘 B+ 树 (变长 key/value)。
package btree

import (
	"bytes"
	"encoding/binary"
	"errors"
	"sort"

	"mysqlfake/storage"
)

// 叶子节点 Payload: numKeys(2) next(4) | { keyLen(2) key valLen(2) val }*
// 内部节点 Payload: numKeys(2) child0(4) | { keyLen(2) key child(4) }*
// 内部节点中 children[i] 存放 < keys[i] 的键，children[i+1] 存放 >= keys[i] 的键。
const (
	MaxKeySize   = 256
	MaxValueSize = 1024
	leafMeta     = 6
	internalMeta = 6
)

var errCorrupt = errors.New("btree: corrupt node")

type node struct {
	leaf     bool
	keys     [][]byte
	vals     [][]byte         // 叶子
	children []storage.PageID // 内部
	next     storage.PageID   // 叶子链表
}

func (n *node) size() int {
	if n.leaf {
		s := leafMeta
		for i := range n.keys {
			s += 4 + len(n.keys[i]) + len(n.vals[i])
		}
		return s
	}
	s := internalMeta
	for i := range n.keys {
		s += 2 + len(n.keys[i]) + 4
	}
	return s
}

// search 二分查找第一个 >= key 的位置，found 表示该位置键与 key 相等。
func (n *node) search(key []byte) (int, bool) {
	i := sort.Search(len(n.keys), func(i int) bool { return bytes.Compare(n.keys[i], key) >= 0 })
	return i, i < len(n.keys) && bytes.Equal(n.keys[i], key)
}

// childIndex 返回 key 应该落入的子节点下标 (第一个 > key 的分隔键的位置)。
func (n *node) childIndex(key []byte) int {
	return sort.Search(len(n.keys), func(i int) bool { return bytes.Compare(n.keys[i], key) > 0 })
}

func decode(p *storage.Page) (*node, error) {
	b := p.Payload()
	n := &node{}
	switch p.Type() {
	case storage.PageTypeLeaf:
		n.leaf = true
	case storage.PageTypeInternal:
	default:
		return nil, errCorrupt
	}
	cnt := int(binary.LittleEndian.Uint16(b[0:2]))
	ptr := storage.PageID(binary.LittleEndian.Uint32(b[2:6]))
	pos := 6
	if n.leaf {
		n.next = ptr
	} else {
		n.children = append(n.children, ptr)
	}
	for i := 0; i < cnt; i++ {
		if pos+2 > len(b) {
			return nil, errCorrupt
		}
		kl := int(binary.LittleEndian.Uint16(b[pos:]))
		pos += 2
		if pos+kl > len(b) {
			return nil, errCorrupt
		}
		n.keys = append(n.keys, bytes.Clone(b[pos:pos+kl]))
		pos += kl
		if n.leaf {
			if pos+2 > len(b) {
				return nil, errCorrupt
			}
			vl := int(binary.LittleEndian.Uint16(b[pos:]))
			pos += 2
			if pos+vl > len(b) {
				return nil, errCorrupt
			}
			n.vals = append(n.vals, bytes.Clone(b[pos:pos+vl]))
			pos += vl
		} else {
			if pos+4 > len(b) {
				return nil, errCorrupt
			}
			n.children = append(n.children, storage.PageID(binary.LittleEndian.Uint32(b[pos:])))
			pos += 4
		}
	}
	return n, nil
}

// encode 把节点写入页 (保留页头中的 ID)，容量不足返回错误。
func (n *node) encode(p *storage.Page) error {
	if n.size() > storage.PayloadSize {
		return errors.New("btree: node overflow")
	}
	id, lsn := p.ID, p.LSN()
	p.Reset(id)
	p.SetLSN(lsn)
	b := p.Payload()
	binary.LittleEndian.PutUint16(b[0:2], uint16(len(n.keys)))
	pos := 6
	if n.leaf {
		p.SetType(storage.PageTypeLeaf)
		binary.LittleEndian.PutUint32(b[2:6], uint32(n.next))
	} else {
		p.SetType(storage.PageTypeInternal)
		binary.LittleEndian.PutUint32(b[2:6], uint32(n.children[0]))
	}
	for i, k := range n.keys {
		binary.LittleEndian.PutUint16(b[pos:], uint16(len(k)))
		pos += 2
		pos += copy(b[pos:], k)
		if n.leaf {
			binary.LittleEndian.PutUint16(b[pos:], uint16(len(n.vals[i])))
			pos += 2
			pos += copy(b[pos:], n.vals[i])
		} else {
			binary.LittleEndian.PutUint32(b[pos:], uint32(n.children[i+1]))
			pos += 4
		}
	}
	return nil
}
