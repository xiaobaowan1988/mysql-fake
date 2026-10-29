package txn

import (
	"encoding/binary"
	"errors"
)

// 存储在 B+ 树叶子里的行 = 版本头 | 行数据。
//
//	txnID(8) rollPtr(8) flags(1) | tuple bytes
//
// txnID 是最后一次修改该行的事务；rollPtr 是 Undo Log 中上一个版本的 ID (0 表示没有)；
// flags 的最低位表示该版本是"已删除"标记。
const (
	VersionHeaderSize = 17
	flagDeleted       = 1
)

var ErrBadVersion = errors.New("txn: corrupt row version")

type RowVersion struct {
	TxnID   uint64
	RollPtr uint64
	Deleted bool
	Data    []byte
}

func (v RowVersion) Encode() []byte {
	b := make([]byte, VersionHeaderSize, VersionHeaderSize+len(v.Data))
	binary.LittleEndian.PutUint64(b[0:], v.TxnID)
	binary.LittleEndian.PutUint64(b[8:], v.RollPtr)
	if v.Deleted {
		b[16] = flagDeleted
	}
	return append(b, v.Data...)
}

func DecodeVersion(b []byte) (RowVersion, error) {
	if len(b) < VersionHeaderSize {
		return RowVersion{}, ErrBadVersion
	}
	return RowVersion{
		TxnID:   binary.LittleEndian.Uint64(b[0:]),
		RollPtr: binary.LittleEndian.Uint64(b[8:]),
		Deleted: b[16]&flagDeleted != 0,
		Data:    b[VersionHeaderSize:],
	}, nil
}

// ReadView 是某一时刻的活跃事务快照，用来判断某个事务的修改是否可见。
type ReadView struct {
	Creator   uint64
	Active    map[uint64]struct{} // 创建时仍活跃的事务 (不含自己)
	MinActive uint64              // 小于它的事务一定已提交
	UpLimit   uint64              // 大于等于它的事务在创建之后才开始
}

// Visible 判断事务 id 的修改对该视图是否可见。
func (rv *ReadView) Visible(id uint64) bool {
	switch {
	case id == rv.Creator:
		return true
	case id < rv.MinActive:
		return true
	case id >= rv.UpLimit:
		return false
	}
	_, active := rv.Active[id]
	return !active
}
