package txn

import (
	"errors"
	"fmt"
	"sync"

	"mysqlfake/btree"
)

var ErrTxnFinished = errors.New("txn: transaction already finished")

// IDStore 持久化事务 ID 的分配上界 (由 DiskManager 实现)。
type IDStore interface {
	NextTxnID() uint64
	SetNextTxnID(uint64) error
}

// Logger 把提交/回滚写入 WAL (由 buffer.Pool 实现)。
type Logger interface {
	Commit(txnID uint64) error
	Abort(txnID uint64)
}

const idReserve = 1000

type Manager struct {
	mu       sync.Mutex
	store    IDStore
	log      Logger
	next     uint64
	reserved uint64
	active   map[uint64]*Txn
	Locks    *LockManager
	Undo     *UndoLog
}

func NewManager(store IDStore, log Logger) *Manager {
	base := store.NextTxnID()
	if base == 0 {
		base = 1
	}
	return &Manager{
		store: store, log: log,
		next: base, reserved: base,
		active: map[uint64]*Txn{},
		Locks:  NewLockManager(),
		Undo:   NewUndoLog(),
	}
}

type Txn struct {
	ID    uint64
	mgr   *Manager
	undo  []uint64 // 本事务的 undo 记录，按产生顺序
	view  *ReadView
	wrote bool
	done  bool
}

func (m *Manager) Begin() (*Txn, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.next >= m.reserved {
		if err := m.store.SetNextTxnID(m.reserved + idReserve); err != nil {
			return nil, err
		}
		m.reserved += idReserve
	}
	t := &Txn{ID: m.next, mgr: m}
	m.next++
	m.active[t.ID] = t
	return t, nil
}

// NewReadView 以 creator 的身份对当前活跃事务做快照。
func (m *Manager) NewReadView(creator uint64) *ReadView {
	m.mu.Lock()
	defer m.mu.Unlock()
	rv := &ReadView{Creator: creator, Active: map[uint64]struct{}{}, MinActive: m.next, UpLimit: m.next}
	for id := range m.active {
		if id == creator {
			continue
		}
		rv.Active[id] = struct{}{}
		if id < rv.MinActive {
			rv.MinActive = id
		}
	}
	return rv
}

// View 返回事务的 Read View；首次调用时创建并沿用到事务结束 (REPEATABLE READ)。
func (t *Txn) View() *ReadView {
	if t.view == nil {
		t.view = t.mgr.NewReadView(t.ID)
	}
	return t.view
}

// ResetView 丢弃已有的 Read View (自动提交的语句每条使用新视图)。
func (t *Txn) ResetView() { t.view = nil }

// Lock 获取行锁 (2PL: 一直持有到事务结束)。
func (t *Txn) Lock(res string, mode LockMode) error {
	if t.done {
		return ErrTxnFinished
	}
	return t.mgr.Locks.Lock(t.ID, res, mode)
}

// AddUndo 在修改前记录 undo，返回新版本应写入的 Rollback_Pointer。
func (t *Txn) AddUndo(kind UndoKind, tree *btree.Tree, key, oldValue []byte) uint64 {
	t.wrote = true
	id := t.mgr.Undo.add(&UndoRecord{
		TxnID: t.ID, Kind: kind, Tree: tree,
		Key: append([]byte(nil), key...), OldValue: append([]byte(nil), oldValue...),
	})
	t.undo = append(t.undo, id)
	return id
}

// Savepoint 返回当前 undo 位置，用于语句级回滚。
func (t *Txn) Savepoint() int { return len(t.undo) }

// RollbackTo 逆序执行 savepoint 之后的 undo 记录。
func (t *Txn) RollbackTo(sp int) error {
	for len(t.undo) > sp {
		id := t.undo[len(t.undo)-1]
		rec := t.mgr.Undo.Get(id)
		if rec == nil {
			return fmt.Errorf("txn: undo record %d missing", id)
		}
		var err error
		switch rec.Kind {
		case UndoInsert:
			_, err = rec.Tree.Delete(rec.Key)
		default:
			err = rec.Tree.Put(rec.Key, rec.OldValue)
		}
		if err != nil {
			return err
		}
		// undo 记录不在此处删除: 并发读者可能已经取得指向它的版本指针，
		// 它们会在没有活跃事务时由 purgeIfIdle 统一清理。
		t.undo = t.undo[:len(t.undo)-1]
	}
	return nil
}

// Commit 写提交日志并强制刷盘，然后释放全部锁。
func (t *Txn) Commit() error {
	if t.done {
		return ErrTxnFinished
	}
	var err error
	if t.wrote {
		err = t.mgr.log.Commit(t.ID)
	}
	t.finish()
	return err
}

// Rollback 撤销事务的全部修改并释放锁。
func (t *Txn) Rollback() error {
	if t.done {
		return ErrTxnFinished
	}
	err := t.RollbackTo(0)
	if t.wrote {
		t.mgr.log.Abort(t.ID)
	}
	t.finish()
	return err
}

func (t *Txn) finish() {
	t.done = true
	m := t.mgr
	m.mu.Lock()
	delete(m.active, t.ID)
	idle := len(m.active) == 0
	m.mu.Unlock()
	m.Locks.ReleaseAll(t.ID)
	if idle {
		m.purgeIfIdle()
	}
}

// purgeIfIdle: 没有活跃事务时，所有已提交版本对之后的任何事务都可见，Undo Log 可整体清空。
func (m *Manager) purgeIfIdle() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.active) == 0 {
		m.Undo.clear()
	}
}

func (t *Txn) Done() bool { return t.done }

// Resolve 在版本链上找到对 view 可见的版本。返回行数据；ok=false 表示该行对视图不存在 (未创建或已删除)。
func (m *Manager) Resolve(view *ReadView, stored []byte) (data []byte, ok bool, err error) {
	v, err := DecodeVersion(stored)
	if err != nil {
		return nil, false, err
	}
	for {
		if view.Visible(v.TxnID) {
			if v.Deleted {
				return nil, false, nil
			}
			return v.Data, true, nil
		}
		if v.RollPtr == 0 {
			return nil, false, nil
		}
		rec := m.Undo.Get(v.RollPtr)
		if rec == nil {
			return nil, false, nil
		}
		if v, err = DecodeVersion(rec.OldValue); err != nil {
			return nil, false, err
		}
	}
}

// ActiveCount 返回活跃事务数 (测试用)。
func (m *Manager) ActiveCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.active)
}

// MarkWrote 标记事务有需要持久化的修改 (例如 DDL 直接修改了 Catalog 页)，保证提交时写日志。
func (t *Txn) MarkWrote() { t.wrote = true }

// NextIDForTest 返回下一个将分配的事务 ID (测试用)。
func (m *Manager) NextIDForTest() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.next
}
