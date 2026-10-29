package txn

import (
	"sync"

	"mysqlfake/btree"
)

type UndoKind uint8

const (
	UndoInsert UndoKind = iota + 1 // 回滚 = 物理删除该键
	UndoUpdate                     // 回滚 = 写回旧值
	UndoDelete                     // 回滚 = 写回旧值 (旧值是删除前的存储行)
)

// UndoRecord 是一条逻辑 undo 记录。OldValue 是修改前该键的完整存储值 (含版本头)，
// 因此它同时是 MVCC 版本链上的"上一个版本"。
type UndoRecord struct {
	ID       uint64
	TxnID    uint64
	Kind     UndoKind
	Table    string
	Tree     *btree.Tree
	Key      []byte
	OldValue []byte
}

// UndoLog 保存所有 undo 记录 (内存中)，通过自增 ID 寻址；ID 即行上的 Rollback_Pointer。
type UndoLog struct {
	mu   sync.RWMutex
	recs map[uint64]*UndoRecord
	next uint64
}

func NewUndoLog() *UndoLog { return &UndoLog{recs: map[uint64]*UndoRecord{}, next: 1} }

func (u *UndoLog) add(r *UndoRecord) uint64 {
	u.mu.Lock()
	defer u.mu.Unlock()
	r.ID = u.next
	u.next++
	u.recs[r.ID] = r
	return r.ID
}

func (u *UndoLog) Get(id uint64) *UndoRecord {
	u.mu.RLock()
	defer u.mu.RUnlock()
	return u.recs[id]
}

func (u *UndoLog) clear() {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.recs = map[uint64]*UndoRecord{}
}

func (u *UndoLog) Len() int {
	u.mu.RLock()
	defer u.mu.RUnlock()
	return len(u.recs)
}
