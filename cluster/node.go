// Package cluster 把单机引擎和 Raft 组合成复制集群的一个节点:
//   - 领导者: 事务提交前把行事件 (Binlog) 经 Raft 复制出去；复制失败则用 Undo 回滚，因此不会留下"提交了但没复制"的事务 (同步模式)。
//   - 跟随者: 只读副本，按 Raft 日志顺序应用事务 (原子、MVCC 可见)。
package cluster

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"mysqlfake/binlog"
	"mysqlfake/db"
	"mysqlfake/metrics"
	"mysqlfake/raft"
)

// Mode 是复制模式。
type Mode int

const (
	ModeSync     Mode = iota // 等待多数派提交 (默认)
	ModeSemiSync             // 至少 SemiSyncAcks 个副本确认，超时退化为异步
	ModeAsync                // 不等待复制
)

var (
	ErrDiverged        = errors.New("cluster: node diverged from the cluster (lost locally committed transactions); resync required")
	ErrCommitUncertain = errors.New("cluster: commit outcome uncertain (replication timed out)")
	ErrReplication     = errors.New("cluster: replication failed")
	ErrNotReady        = errors.New("cluster: leader is still applying entries from earlier terms, retry")
)

type Options struct {
	ID        int
	Peers     []int
	Transport raft.Transport
	Dir       string

	Mode            Mode
	SemiSyncAcks    int           // 默认 1
	SemiSyncTimeout time.Duration // 默认 500ms
	CommitTimeout   time.Duration // 默认 5s

	Raft   func(*raft.Config) // 可选: 调整 Raft 时间参数
	Engine db.Options
}

type pendingTxn struct {
	seen      bool // Apply 循环已看到该条目被提交
	abandoned bool // 提议方放弃 (本地已回滚)，应用循环需要代为应用
}

type Node struct {
	opts Options
	eng  *db.Engine
	rf   *raft.Node

	mu             sync.Mutex
	pending        map[uint64]*pendingTxn
	localCommitted map[uint64]bool // 异步/半同步下已本地提交但尚未确认被 Raft 提交的条目
	applied        uint64
	appliedCh      chan struct{}

	diverged atomic.Bool
	closing  chan struct{}
	wg       sync.WaitGroup
}

func Open(opts Options) (*Node, error) {
	if opts.SemiSyncAcks == 0 {
		opts.SemiSyncAcks = 1
	}
	if opts.SemiSyncTimeout == 0 {
		opts.SemiSyncTimeout = 500 * time.Millisecond
	}
	if opts.CommitTimeout == 0 {
		opts.CommitTimeout = 5 * time.Second
	}
	if err := os.MkdirAll(opts.Dir, 0o755); err != nil {
		return nil, err
	}
	eng, err := db.OpenWithOptions(filepath.Join(opts.Dir, "data.db"), opts.Engine)
	if err != nil {
		return nil, err
	}
	n := &Node{
		opts: opts, eng: eng,
		pending: map[uint64]*pendingTxn{}, localCommitted: map[uint64]bool{},
		appliedCh: make(chan struct{}), closing: make(chan struct{}),
	}
	if b, err := os.ReadFile(filepath.Join(opts.Dir, "applied")); err == nil && len(b) == 8 {
		n.applied = binary.LittleEndian.Uint64(b)
	}
	rcfg := raft.Config{
		ID: opts.ID, Peers: opts.Peers, Transport: opts.Transport,
		Dir:        filepath.Join(opts.Dir, "raft"),
		OnTruncate: n.onTruncate,
	}
	if opts.Raft != nil {
		opts.Raft(&rcfg)
	}
	if n.rf, err = raft.NewNode(rcfg); err != nil {
		eng.Close()
		return nil, err
	}
	n.registerGauges()
	eng.SetWriteGuard(n.writeGuard)
	eng.SetReplicator(n.replicate)
	n.wg.Add(1)
	go n.applyLoop()
	return n, nil
}

func (n *Node) Engine() *db.Engine { return n.eng }
func (n *Node) Raft() *raft.Node   { return n.rf }

// NewSession 创建会话。写语句仅在租约有效的领导者上被接受，其余节点返回 *raft.NotLeaderError。
func (n *Node) NewSession() *db.Session { return n.eng.NewSession() }

func (n *Node) IsLeader() bool { return n.rf.IsLeader() }
func (n *Node) LeaderID() int  { return n.rf.LeaderHint() }

// Diverged 表示本节点曾本地提交了后来被集群覆盖的事务，必须重新同步后才能再接受写入。
func (n *Node) Diverged() bool { return n.diverged.Load() }

func (n *Node) AppliedIndex() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.applied
}

// WaitApplied 等待本节点应用到至少 index (只读副本读取时用来保证"读到自己想看到的写入")。
func (n *Node) WaitApplied(index uint64, timeout time.Duration) error {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		n.mu.Lock()
		ok, ch := n.applied >= index, n.appliedCh
		n.mu.Unlock()
		if ok {
			return nil
		}
		select {
		case <-ch:
		case <-timer.C:
			return raft.ErrTimeout
		case <-n.closing:
			return raft.ErrStopped
		}
	}
}

func (n *Node) writeGuard() error {
	if n.diverged.Load() {
		return ErrDiverged
	}
	s := n.rf.Status()
	if !s.LeaseValid {
		return &raft.NotLeaderError{Leader: n.rf.LeaderHint()}
	}
	// 新领导者必须先应用完之前任期已提交的条目，否则会基于过期数据写入。
	if s.ReadyIndex == 0 || n.AppliedIndex() < s.ReadyIndex {
		return ErrNotReady
	}
	return nil
}

func (n *Node) onTruncate(from uint64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for idx := range n.localCommitted {
		if idx >= from {
			n.diverged.Store(true)
		}
	}
}

// replicate 是事务提交前的复制钩子 (txn.ReplicateFunc)。
func (n *Node) replicate(rec *binlog.Txn) error {
	rec.Origin = n.opts.ID
	st := &pendingTxn{}
	n.mu.Lock()
	n.pending[rec.TxnID] = st
	n.mu.Unlock()
	drop := func() {
		n.mu.Lock()
		delete(n.pending, rec.TxnID)
		n.mu.Unlock()
	}
	idx, term, err := n.rf.Propose(rec.Encode())
	if err != nil {
		drop()
		return err
	}
	switch n.opts.Mode {
	case ModeSync:
		err = n.rf.WaitCommitted(idx, term, n.opts.CommitTimeout)
	case ModeSemiSync:
		err = n.rf.WaitReplicated(idx, term, n.opts.SemiSyncAcks, n.opts.SemiSyncTimeout)
		if errors.Is(err, raft.ErrTimeout) {
			err = nil // 退化为异步
		}
		if err == nil {
			n.markLocalCommitted(idx)
		}
	case ModeAsync:
		n.markLocalCommitted(idx)
	}
	switch {
	case err == nil:
		return nil
	case errors.Is(err, raft.ErrOverwritten):
		drop()
		return fmt.Errorf("%w: leadership lost before the transaction was committed", ErrReplication)
	case errors.Is(err, raft.ErrTimeout):
		n.mu.Lock()
		defer n.mu.Unlock()
		if st.seen { // 超时的同时已经提交: 视为成功
			return nil
		}
		st.abandoned = true // 本地将回滚；若条目最终提交，由应用循环代为应用
		return ErrCommitUncertain
	}
	drop()
	return err
}

func (n *Node) markLocalCommitted(idx uint64) {
	n.mu.Lock()
	n.localCommitted[idx] = true
	n.mu.Unlock()
}

func (n *Node) applyLoop() {
	defer n.wg.Done()
	for e := range n.rf.ApplyCh() {
		if e.Index > n.AppliedIndex() && !e.Noop {
			rec, err := binlog.DecodeTxn(e.Data)
			if err != nil {
				n.diverged.Store(true)
				continue
			}
			if !n.skipOwn(rec) {
				if err := n.applyWithRetry(rec); err != nil {
					n.diverged.Store(true)
					fmt.Fprintf(os.Stderr, "cluster node %d: cannot apply entry %d: %v\n", n.opts.ID, e.Index, err)
				}
			}
		}
		n.mu.Lock()
		delete(n.localCommitted, e.Index)
		if e.Index > n.applied {
			n.applied = e.Index
			close(n.appliedCh)
			n.appliedCh = make(chan struct{})
		}
		n.mu.Unlock()
		n.saveApplied(e.Index)
	}
}

// skipOwn: 本节点作为领导者提议的事务已在本地提交，不再重复应用，除非提议方已放弃并回滚。
func (n *Node) skipOwn(rec *binlog.Txn) bool {
	if rec.Origin != n.opts.ID {
		return false
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	st := n.pending[rec.TxnID]
	if st == nil {
		return false
	}
	delete(n.pending, rec.TxnID)
	if st.abandoned {
		return false
	}
	st.seen = true
	return true
}

func (n *Node) applyWithRetry(rec *binlog.Txn) error {
	var err error
	for i := 0; i < 5; i++ {
		if err = n.eng.ApplyBinlog(rec); err == nil {
			return nil
		}
		select {
		case <-n.closing:
			return err
		case <-time.After(50 * time.Millisecond):
		}
	}
	return err
}

func (n *Node) saveApplied(idx uint64) {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], idx)
	os.WriteFile(filepath.Join(n.opts.Dir, "applied"), b[:], 0o644)
}

// Close 停止 Raft、应用循环并关闭引擎。
func (n *Node) Close() error {
	close(n.closing)
	n.rf.Stop()
	n.wg.Wait()
	return n.eng.Close()
}

func (n *Node) registerGauges() {
	g := metrics.Default.GaugeFunc
	g("mysqlfake_raft_term", "Current Raft term.", func() float64 { return float64(n.rf.Status().Term) })
	g("mysqlfake_raft_is_leader", "1 if this node is the leader with a valid lease.", func() float64 {
		if n.rf.IsLeader() {
			return 1
		}
		return 0
	})
	g("mysqlfake_raft_commit_index", "Raft commit index.", func() float64 { return float64(n.rf.Status().CommitIndex) })
	g("mysqlfake_raft_last_index", "Last Raft log index.", func() float64 { return float64(n.rf.Status().LastIndex) })
	g("mysqlfake_raft_applied_index", "Last Raft log index applied to the local database.", func() float64 { return float64(n.AppliedIndex()) })
	g("mysqlfake_replication_lag_entries", "Committed entries not yet applied locally.", func() float64 {
		return float64(n.rf.Status().CommitIndex) - float64(n.AppliedIndex())
	})
	g("mysqlfake_node_diverged", "1 if this node lost locally committed transactions and must be resynced.", func() float64 {
		if n.Diverged() {
			return 1
		}
		return 0
	})
}
