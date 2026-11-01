package txn

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"mysqlfake/binlog"
	"mysqlfake/btree"
	"mysqlfake/metrics"
	"mysqlfake/wal"
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
	// LogUndo 写入逻辑 undo 记录，必须先于它所描述的数据修改 (崩溃恢复据此撤销未提交的事务)。
	LogUndo(txnID uint64, info wal.UndoInfo) uint64
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

	// 提交钩子 (按顺序): 复制 -> 本地 Binlog -> 订阅者。
	Binlog     *binlog.Log
	replicator ReplicateFunc

	// 写入静默 (热备份用): quiescing 为真时，事务第一次写入会阻塞；writers 是已经写过数据、尚未结束的事务数
	quiescing bool
	writers   int
	gate      *sync.Cond
	subs      map[int]func(*binlog.Txn)
	nextSub   int
}

// ReplicateFunc 在事务提交前把事务的行事件复制出去 (Raft 等)。返回错误则事务回滚。
type ReplicateFunc func(rec *binlog.Txn) error

// SetReplicator 设置复制钩子 (nil 表示单机)。
func (m *Manager) SetReplicator(f ReplicateFunc) {
	m.mu.Lock()
	m.replicator = f
	m.mu.Unlock()
}

// Subscribe 订阅已提交事务的行事件 (Online DDL 的增量捕获使用)。回调在提交线程内同步执行，
// 此时事务仍持有锁。返回取消订阅函数。
func (m *Manager) Subscribe(f func(*binlog.Txn)) (cancel func()) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.subs == nil {
		m.subs = map[int]func(*binlog.Txn){}
	}
	id := m.nextSub
	m.nextSub++
	m.subs[id] = f
	return func() {
		m.mu.Lock()
		delete(m.subs, id)
		m.mu.Unlock()
	}
}

func NewManager(store IDStore, log Logger) *Manager {
	base := store.NextTxnID()
	if base == 0 {
		base = 1
	}
	m := &Manager{
		store: store, log: log,
		next: base, reserved: base,
		active: map[uint64]*Txn{},
		Locks:  NewLockManager(),
		Undo:   NewUndoLog(),
	}
	m.gate = sync.NewCond(&m.mu)
	return m
}

// noteWrite 在事务第一次写入时登记；写入静默期间在这里排队。
func (t *Txn) noteWrite() {
	if t.wrote {
		return
	}
	m := t.mgr
	m.mu.Lock()
	for m.quiescing {
		m.gate.Wait()
	}
	m.writers++
	m.mu.Unlock()
	t.wrote = true
}

var ErrQuiesceTimeout = errors.New("txn: timed out waiting for open write transactions to finish")

// Quiesce 暂停写入: 新的写入排队，并等待已经写过数据的事务全部结束 (提交或回滚)。
// 返回后日志里不再有未结束事务的修改。release 恢复写入。
func (m *Manager) Quiesce(timeout time.Duration) (release func(), err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.quiescing {
		return nil, errors.New("txn: already quiesced")
	}
	m.quiescing = true
	deadline := time.Now().Add(timeout)
	timer := time.AfterFunc(timeout, func() { m.mu.Lock(); m.gate.Broadcast(); m.mu.Unlock() })
	defer timer.Stop()
	for m.writers > 0 {
		if time.Now().After(deadline) {
			m.quiescing = false
			m.gate.Broadcast()
			return nil, ErrQuiesceTimeout
		}
		m.gate.Wait()
	}
	var once sync.Once
	return func() {
		once.Do(func() {
			m.mu.Lock()
			m.quiescing = false
			m.gate.Broadcast()
			m.mu.Unlock()
		})
	}, nil
}

type Txn struct {
	ID              uint64
	mgr             *Manager
	undo            []uint64       // 本事务的 undo 记录，按产生顺序
	evs             []binlog.Event // 与 undo 一一对应的行事件
	ddl             []binlog.Event // DDL 事件 (自动提交，不参与回滚)
	fromReplication bool           // 由复制日志应用产生，不再向外复制
	onCommit        func() error
	lockWait        atomic.Int64 // 本事务累计的锁等待时间 (纳秒)   // 提交 (复制、日志落盘) 成功之后、释放锁之前执行
	view            *ReadView
	wrote           bool
	done            bool
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
	if mode == LockX {
		// 要拿排他锁就是要写: 在拿锁之前就登记 (热备份的写入静默在此排队)，
		// 避免"已持有行锁的事务在静默处排队、静默却在等待被它挡住的写事务"的死锁
		t.noteWrite()
	}
	d, err := t.mgr.Locks.LockTimed(t.ID, res, mode)
	if d > 0 {
		t.lockWait.Add(int64(d))
	}
	return err
}

// LockWaitTime 返回本事务累计的锁等待时间。
func (t *Txn) LockWaitTime() time.Duration { return time.Duration(t.lockWait.Load()) }

// AddUndo 在修改前记录 undo，返回新版本应写入的 Rollback_Pointer。
// newRow 是修改后的行数据 (UndoDelete 传 nil)，用于生成对应的 Binlog 行事件。
// idxAdds 是这次修改新增的二级索引项 (回滚时会被删除)。
func (t *Txn) AddUndo(kind UndoKind, table string, tree *btree.Tree, key, oldValue, newRow []byte, idxAdds ...IdxRef) uint64 {
	t.noteWrite()
	info := wal.UndoInfo{Kind: uint8(kind), Root: tree.Root(), Key: key, OldValue: oldValue}
	for _, ix := range idxAdds {
		info.Idx = append(info.Idx, wal.UndoIdx{Root: ix.Tree.Root(), Key: ix.Key})
	}
	t.mgr.log.LogUndo(t.ID, info) // 先写 undo 日志，再 (由调用方) 修改数据
	id := t.mgr.Undo.add(&UndoRecord{
		TxnID: t.ID, Kind: kind, Table: table, Tree: tree,
		Key: append([]byte(nil), key...), OldValue: append([]byte(nil), oldValue...),
		IdxAdds: idxAdds,
	})
	t.undo = append(t.undo, id)
	ev := binlog.Event{Op: binlog.OpPut, Table: table, Key: append([]byte(nil), key...), Row: append([]byte(nil), newRow...)}
	if kind == UndoDelete {
		ev = binlog.Event{Op: binlog.OpDelete, Table: table, Key: ev.Key}
	}
	t.evs = append(t.evs, ev)
	return id
}

// AddDDLEvent 记录一条 DDL 事件 (随事务提交写入 Binlog 并复制)。
func (t *Txn) AddDDLEvent(sql string) {
	t.noteWrite()
	t.ddl = append(t.ddl, binlog.Event{Op: binlog.OpDDL, SQL: sql})
}

// SetFromReplication 标记事务由复制日志应用而来: 提交时不再调用复制钩子。
func (t *Txn) SetFromReplication() { t.fromReplication = true }

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
		for _, ix := range rec.IdxAdds { // 撤销本次新增的索引项
			if _, err := ix.Tree.Delete(ix.Key); err != nil {
				return err
			}
		}
		// undo 记录不在此处删除: 并发读者可能已经取得指向它的版本指针，
		// 它们会在没有活跃事务时由 purgeIfIdle 统一清理。
		t.undo = t.undo[:len(t.undo)-1]
		t.evs = t.evs[:len(t.evs)-1]
	}
	return nil
}

// Commit 写提交日志并强制刷盘，然后释放全部锁。
func (t *Txn) Commit() error {
	if t.done {
		return ErrTxnFinished
	}
	if events := append(append([]binlog.Event(nil), t.evs...), t.ddl...); len(events) > 0 {
		rec := &binlog.Txn{TxnID: t.ID, TimeNano: time.Now().UnixNano(), Events: events}
		m := t.mgr
		m.mu.Lock()
		rep := m.replicator
		m.mu.Unlock()
		if rep != nil && !t.fromReplication {
			if err := rep(rec); err != nil {
				t.Rollback()
				return err
			}
		}
		if m.Binlog != nil {
			if err := m.Binlog.Append(rec); err != nil {
				t.Rollback()
				return err
			}
		}
		m.notify(rec)
	}
	var err error
	if t.wrote {
		err = t.mgr.log.Commit(t.ID)
	}
	if t.onCommit != nil && err == nil {
		err = t.onCommit()
	}
	metrics.TxnCommits.Inc()
	t.finish()
	return err
}

// SetOnCommit 注册一个回调，在事务提交成功 (含复制与日志落盘) 之后、释放锁之前执行。
// Online DDL 用它在持有元数据锁 X 的情况下完成 Catalog 切换。
func (t *Txn) SetOnCommit(f func() error) { t.onCommit = f }

func (m *Manager) notify(rec *binlog.Txn) {
	m.mu.Lock()
	fs := make([]func(*binlog.Txn), 0, len(m.subs))
	for _, f := range m.subs {
		fs = append(fs, f)
	}
	m.mu.Unlock()
	for _, f := range fs {
		f(rec)
	}
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
	metrics.TxnRollbacks.Inc()
	t.finish()
	return err
}

func (t *Txn) finish() {
	t.done = true
	m := t.mgr
	m.mu.Lock()
	delete(m.active, t.ID)
	idle := len(m.active) == 0
	if t.wrote {
		m.writers--
		m.gate.Broadcast()
	}
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
func (t *Txn) MarkWrote() { t.noteWrite() }

// LockMDL 对表加元数据锁 (S)，持有到事务结束。写事务必须在读取 TableInfo 之前调用，
// 这样 Online DDL 切换 (X 锁) 时不会有事务仍在向旧结构写入。
func (t *Txn) LockMDL(table string) error {
	return t.Lock(MDLResource(table), LockS)
}

// MDLResource 返回表的元数据锁资源名。
func MDLResource(table string) string { return "mdl:" + strings.ToLower(table) }

// NextID 返回下一个将分配的事务号。
func (m *Manager) NextID() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.next
}
