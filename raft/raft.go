// Package raft 实现 Raft 一致性协议 (选主、日志复制、提交、持久化)，并带有领导者租约:
//   - 领导者只有在最近一个租约周期内收到多数派确认时才有效，否则降级、拒绝写入；
//   - 跟随者在租约周期内收到过领导者消息就不投票 (领导者粘性)，避免被隔离节点回归时打断/脑裂。
package raft

import (
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

type Entry struct {
	Index uint64
	Term  uint64
	Data  []byte
	Noop  bool // 新领导者任期开始时写入的空操作
}

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

func (s State) String() string { return [...]string{"follower", "candidate", "leader"}[s] }

var (
	ErrStopped     = errors.New("raft: node stopped")
	ErrOverwritten = errors.New("raft: entry was overwritten by another leader")
	ErrTimeout     = errors.New("raft: timeout")
)

// NotLeaderError 提示当前已知的领导者 (0 表示未知)。
type NotLeaderError struct{ Leader int }

func (e *NotLeaderError) Error() string {
	if e.Leader == 0 {
		return "raft: not leader (no leader known)"
	}
	return fmt.Sprintf("raft: not leader (leader is node %d)", e.Leader)
}

type Config struct {
	ID        int
	Peers     []int // 全部节点，包含自己
	Transport Transport
	Dir       string // 持久化目录，空表示仅内存

	HeartbeatInterval  time.Duration
	LeaseTimeout       time.Duration
	ElectionTimeoutMin time.Duration // 必须 >= LeaseTimeout
	ElectionTimeoutMax time.Duration

	// OnTruncate 在本节点日志从 from 起被冲突截断时调用 (持有内部锁，勿阻塞)。
	OnTruncate func(from uint64)
}

func (c *Config) defaults() {
	if c.HeartbeatInterval == 0 {
		c.HeartbeatInterval = 50 * time.Millisecond
	}
	if c.LeaseTimeout == 0 {
		c.LeaseTimeout = 6 * c.HeartbeatInterval
	}
	if c.ElectionTimeoutMin == 0 {
		c.ElectionTimeoutMin = c.LeaseTimeout + c.HeartbeatInterval
	}
	if c.ElectionTimeoutMax == 0 {
		c.ElectionTimeoutMax = 2 * c.ElectionTimeoutMin
	}
}

type Node struct {
	cfg Config
	st  *store

	mu       sync.Mutex
	state    State
	term     uint64
	votedFor int
	leader   int
	log      []Entry // log[0] 是哨兵 (Index 0, Term 0)
	commit   uint64
	applied  uint64

	readyIndex uint64 // 当前任期领导者空操作条目的位置
	nextIndex  map[int]uint64
	matchIndex map[int]uint64
	lastAck    map[int]time.Time // 对端最近一次确认我们领导地位的 RPC 的发送时间
	inflight   map[int]bool

	lastHeard time.Time // 跟随者最近一次收到合法领导者消息
	deadline  time.Time // 选举超时时刻
	lastBcast time.Time
	changeCh  chan struct{}

	applyCh     chan Entry
	applyNotify chan struct{}
	stopCh      chan struct{}
	wg          sync.WaitGroup
	stopped     bool
}

// NewNode 创建并启动节点。已提交的日志条目通过 ApplyCh 按序投递 (重启后会从头重新投递，
// 消费者需要按幂等方式处理或自行记录已应用位置)。
func NewNode(cfg Config) (*Node, error) {
	cfg.defaults()
	n := &Node{
		cfg:         cfg,
		log:         []Entry{{}},
		nextIndex:   map[int]uint64{},
		matchIndex:  map[int]uint64{},
		lastAck:     map[int]time.Time{},
		inflight:    map[int]bool{},
		changeCh:    make(chan struct{}),
		applyCh:     make(chan Entry, 1024),
		applyNotify: make(chan struct{}, 1),
		stopCh:      make(chan struct{}),
	}
	if cfg.Dir != "" {
		st, term, voted, entries, err := openStore(cfg.Dir)
		if err != nil {
			return nil, err
		}
		n.st, n.term, n.votedFor = st, term, voted
		for _, e := range entries {
			if e.Index == uint64(len(n.log)) {
				n.log = append(n.log, e)
			}
		}
	}
	n.resetDeadlineLocked()
	cfg.Transport.Register(n)
	n.wg.Add(2)
	go n.tickLoop()
	go n.applyLoop()
	return n, nil
}

func (n *Node) ApplyCh() <-chan Entry { return n.applyCh }

func (n *Node) majority() int { return len(n.cfg.Peers)/2 + 1 }

func (n *Node) lastIndexLocked() uint64 { return uint64(len(n.log) - 1) }
func (n *Node) lastTermLocked() uint64  { return n.log[len(n.log)-1].Term }

func (n *Node) changedLocked() {
	close(n.changeCh)
	n.changeCh = make(chan struct{})
}

func (n *Node) resetDeadlineLocked() {
	span := n.cfg.ElectionTimeoutMax - n.cfg.ElectionTimeoutMin
	d := n.cfg.ElectionTimeoutMin
	if span > 0 {
		d += time.Duration(rand.Int63n(int64(span)))
	}
	n.deadline = time.Now().Add(d)
}

func (n *Node) persistStateLocked() {
	if n.st != nil {
		if err := n.st.saveState(n.term, n.votedFor); err != nil {
			panic(fmt.Sprintf("raft: cannot persist state: %v", err))
		}
	}
}

func (n *Node) persistAppendLocked(es []Entry) {
	if n.st != nil {
		if err := n.st.append(es); err != nil {
			panic(fmt.Sprintf("raft: cannot persist log: %v", err))
		}
	}
}

func (n *Node) becomeFollowerLocked(term uint64, leader int) {
	if term > n.term {
		n.term, n.votedFor = term, 0
		n.persistStateLocked()
	}
	n.state = Follower
	n.leader = leader
	n.resetDeadlineLocked()
	n.changedLocked()
}

func (n *Node) tickLoop() {
	defer n.wg.Done()
	t := time.NewTicker(n.cfg.HeartbeatInterval / 5)
	defer t.Stop()
	for {
		select {
		case <-n.stopCh:
			return
		case <-t.C:
			n.tick()
		}
	}
}

func (n *Node) tick() {
	n.mu.Lock()
	defer n.mu.Unlock()
	now := time.Now()
	if n.state == Leader {
		if !n.leaseValidLocked(now) {
			n.becomeFollowerLocked(n.term, 0) // 失去多数派联系: 降级，停止接受写入
			return
		}
		if now.Sub(n.lastBcast) >= n.cfg.HeartbeatInterval {
			n.broadcastLocked()
		}
		return
	}
	if now.After(n.deadline) {
		n.startElectionLocked()
	}
}

func (n *Node) leaseValidLocked(now time.Time) bool {
	cnt := 1
	for _, p := range n.cfg.Peers {
		if p != n.cfg.ID && now.Sub(n.lastAck[p]) < n.cfg.LeaseTimeout {
			cnt++
		}
	}
	return cnt >= n.majority()
}

func (n *Node) startElectionLocked() {
	n.state = Candidate
	n.term++
	n.votedFor = n.cfg.ID
	n.leader = 0
	n.persistStateLocked()
	n.resetDeadlineLocked()
	n.changedLocked()
	term := n.term
	args := &VoteArgs{Term: term, Candidate: n.cfg.ID, LastIndex: n.lastIndexLocked(), LastTerm: n.lastTermLocked()}
	votes := 1
	sent := time.Now()
	acks := map[int]time.Time{}
	if votes >= n.majority() {
		n.becomeLeaderLocked(acks)
		return
	}
	for _, p := range n.cfg.Peers {
		if p == n.cfg.ID {
			continue
		}
		go func(p int) {
			r, err := n.cfg.Transport.Vote(p, args)
			if err != nil {
				return
			}
			n.mu.Lock()
			defer n.mu.Unlock()
			if n.state != Candidate || n.term != term {
				return
			}
			if r.Term > n.term {
				n.becomeFollowerLocked(r.Term, 0)
				return
			}
			if r.Granted {
				votes++
				acks[p] = sent
				if votes >= n.majority() {
					n.becomeLeaderLocked(acks)
				}
			}
		}(p)
	}
}

func (n *Node) becomeLeaderLocked(acks map[int]time.Time) {
	n.state = Leader
	n.leader = n.cfg.ID
	for _, p := range n.cfg.Peers {
		n.nextIndex[p] = n.lastIndexLocked() + 1
		n.matchIndex[p] = 0
		n.inflight[p] = false
		n.lastAck[p] = acks[p]
	}
	// 新任期的空操作条目: 用来提交之前任期遗留的条目
	e := Entry{Index: n.lastIndexLocked() + 1, Term: n.term, Noop: true}
	n.log = append(n.log, e)
	n.persistAppendLocked([]Entry{e})
	n.matchIndex[n.cfg.ID] = e.Index
	n.readyIndex = e.Index
	n.changedLocked()
	n.advanceCommitLocked()
	n.broadcastLocked()
}

func (n *Node) broadcastLocked() {
	n.lastBcast = time.Now()
	for _, p := range n.cfg.Peers {
		if p != n.cfg.ID && !n.inflight[p] {
			n.inflight[p] = true
			go n.replicate(p)
		}
	}
}

// replicate 持续向 p 发送 AppendEntries，直到 p 追平或失去领导地位。
func (n *Node) replicate(p int) {
	for {
		n.mu.Lock()
		if n.state != Leader {
			n.inflight[p] = false
			n.mu.Unlock()
			return
		}
		next := n.nextIndex[p]
		if next < 1 {
			next = 1
		}
		prev := n.log[next-1]
		var entries []Entry
		for i := next; i <= n.lastIndexLocked() && len(entries) < 256; i++ {
			entries = append(entries, n.log[i])
		}
		term := n.term
		args := &AppendArgs{Term: term, Leader: n.cfg.ID, PrevIndex: prev.Index, PrevTerm: prev.Term, Entries: entries, Commit: n.commit}
		sent := time.Now()
		n.mu.Unlock()

		r, err := n.cfg.Transport.Append(p, args)

		n.mu.Lock()
		if err != nil || n.state != Leader || n.term != term {
			n.inflight[p] = false
			n.mu.Unlock()
			return
		}
		if r.Term > n.term {
			n.becomeFollowerLocked(r.Term, 0)
			n.inflight[p] = false
			n.mu.Unlock()
			return
		}
		n.lastAck[p] = sent
		more := false
		if r.Success {
			if m := args.PrevIndex + uint64(len(entries)); m > n.matchIndex[p] {
				n.matchIndex[p] = m
			}
			n.nextIndex[p] = n.matchIndex[p] + 1
			n.changedLocked() // 唤醒 WaitReplicated
			n.advanceCommitLocked()
			more = n.nextIndex[p] <= n.lastIndexLocked()
		} else {
			ni := r.ConflictIndex
			if ni < 1 || ni >= next {
				ni = next - 1
			}
			if ni < 1 {
				ni = 1
			}
			n.nextIndex[p] = ni
			more = true
		}
		if !more {
			n.inflight[p] = false
			n.mu.Unlock()
			return
		}
		n.mu.Unlock()
	}
}

func (n *Node) advanceCommitLocked() {
	for idx := n.lastIndexLocked(); idx > n.commit; idx-- {
		if n.log[idx].Term != n.term {
			break
		}
		cnt := 0
		for _, p := range n.cfg.Peers {
			if n.matchIndex[p] >= idx {
				cnt++
			}
		}
		if cnt >= n.majority() {
			n.commit = idx
			n.changedLocked()
			select {
			case n.applyNotify <- struct{}{}:
			default:
			}
			return
		}
	}
}

// HandleVote 处理 RequestVote。
func (n *Node) HandleVote(a *VoteArgs) *VoteReply {
	n.mu.Lock()
	defer n.mu.Unlock()
	now := time.Now()
	// 领导者粘性: 租约期内见过领导者 (或自己是有效领导者) 时拒绝投票，也不更新任期。
	if a.Candidate != n.leader {
		if (n.state == Leader && n.leaseValidLocked(now)) ||
			(n.state != Leader && n.leader != 0 && now.Sub(n.lastHeard) < n.cfg.LeaseTimeout) {
			return &VoteReply{Term: n.term}
		}
	}
	if a.Term < n.term {
		return &VoteReply{Term: n.term}
	}
	if a.Term > n.term {
		n.becomeFollowerLocked(a.Term, 0)
	}
	upToDate := a.LastTerm > n.lastTermLocked() || (a.LastTerm == n.lastTermLocked() && a.LastIndex >= n.lastIndexLocked())
	if (n.votedFor == 0 || n.votedFor == a.Candidate) && upToDate {
		n.votedFor = a.Candidate
		n.persistStateLocked()
		n.resetDeadlineLocked()
		return &VoteReply{Term: n.term, Granted: true}
	}
	return &VoteReply{Term: n.term}
}

// HandleAppend 处理 AppendEntries (同时充当心跳)。
func (n *Node) HandleAppend(a *AppendArgs) *AppendReply {
	n.mu.Lock()
	defer n.mu.Unlock()
	if a.Term < n.term {
		return &AppendReply{Term: n.term}
	}
	if a.Term > n.term || n.state != Follower {
		n.becomeFollowerLocked(a.Term, a.Leader)
	}
	n.leader = a.Leader
	n.lastHeard = time.Now()
	n.resetDeadlineLocked()

	last := n.lastIndexLocked()
	if a.PrevIndex > last {
		return &AppendReply{Term: n.term, ConflictIndex: last + 1}
	}
	if n.log[a.PrevIndex].Term != a.PrevTerm {
		ci := a.PrevIndex
		for ci > 1 && n.log[ci-1].Term == n.log[a.PrevIndex].Term {
			ci--
		}
		return &AppendReply{Term: n.term, ConflictIndex: ci}
	}
	truncated := false
	var fresh []Entry
	for i, e := range a.Entries {
		idx := a.PrevIndex + 1 + uint64(i)
		if idx <= n.lastIndexLocked() {
			if n.log[idx].Term == e.Term {
				continue
			}
			if idx <= n.commit {
				panic("raft: attempt to truncate committed entries")
			}
			if n.cfg.OnTruncate != nil {
				n.cfg.OnTruncate(idx)
			}
			n.log = n.log[:idx]
			truncated = true
		}
		n.log = append(n.log, e)
		fresh = append(fresh, e)
	}
	if truncated {
		if n.st != nil {
			if err := n.st.rewrite(n.log[1:]); err != nil {
				panic(fmt.Sprintf("raft: cannot rewrite log: %v", err))
			}
		}
	} else if len(fresh) > 0 {
		n.persistAppendLocked(fresh)
	}
	if a.Commit > n.commit {
		newest := a.PrevIndex + uint64(len(a.Entries))
		c := a.Commit
		if newest < c {
			c = newest
		}
		if c > n.commit {
			n.commit = c
			select {
			case n.applyNotify <- struct{}{}:
			default:
			}
		}
	}
	n.changedLocked()
	return &AppendReply{Term: n.term, Success: true}
}

func (n *Node) applyLoop() {
	defer n.wg.Done()
	for {
		select {
		case <-n.stopCh:
			return
		case <-n.applyNotify:
		}
		for {
			n.mu.Lock()
			if n.applied >= n.commit {
				n.mu.Unlock()
				break
			}
			e := n.log[n.applied+1]
			n.applied++
			n.mu.Unlock()
			select {
			case n.applyCh <- e:
			case <-n.stopCh:
				return
			}
		}
	}
}

// Propose 追加一条命令 (仅领导者且租约有效时)。返回条目的位置，之后用 WaitCommitted 等待提交。
func (n *Node) Propose(data []byte) (index, term uint64, err error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped {
		return 0, 0, ErrStopped
	}
	if n.state != Leader || !n.leaseValidLocked(time.Now()) {
		return 0, 0, &NotLeaderError{Leader: n.leaderHintLocked()}
	}
	e := Entry{Index: n.lastIndexLocked() + 1, Term: n.term, Data: data}
	n.log = append(n.log, e)
	n.persistAppendLocked([]Entry{e})
	n.matchIndex[n.cfg.ID] = e.Index
	n.advanceCommitLocked()
	n.broadcastLocked()
	return e.Index, e.Term, nil
}

func (n *Node) leaderHintLocked() int {
	if n.state == Leader {
		return 0
	}
	return n.leader
}

func (n *Node) wait(timeout time.Duration, cond func() (done bool, err error)) error {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		n.mu.Lock()
		done, err := cond()
		ch := n.changeCh
		stopped := n.stopped
		n.mu.Unlock()
		if done || err != nil {
			return err
		}
		if stopped {
			return ErrStopped
		}
		select {
		case <-ch:
		case <-timer.C:
			return ErrTimeout
		case <-n.stopCh:
			return ErrStopped
		}
	}
}

// WaitCommitted 等待 (index, term) 条目被提交；若该位置已被其他领导者的条目覆盖返回 ErrOverwritten。
func (n *Node) WaitCommitted(index, term uint64, timeout time.Duration) error {
	return n.wait(timeout, func() (bool, error) {
		if index > n.lastIndexLocked() || n.log[index].Term != term {
			return false, ErrOverwritten
		}
		return n.commit >= index, nil
	})
}

// WaitReplicated 等待至少 k 个跟随者已持有 (index, term) 条目 (半同步复制使用)。
func (n *Node) WaitReplicated(index, term uint64, k int, timeout time.Duration) error {
	return n.wait(timeout, func() (bool, error) {
		if index > n.lastIndexLocked() || n.log[index].Term != term {
			return false, ErrOverwritten
		}
		if n.state != Leader || n.term != term {
			return false, &NotLeaderError{Leader: n.leader}
		}
		cnt := 0
		for _, p := range n.cfg.Peers {
			if p != n.cfg.ID && n.matchIndex[p] >= index {
				cnt++
			}
		}
		return cnt >= k, nil
	})
}

type Status struct {
	ID          int
	State       State
	Term        uint64
	Leader      int
	CommitIndex uint64
	LastIndex   uint64
	LeaseValid  bool
	// ReadyIndex 是本任期领导者空操作条目的位置；CommitIndex >= ReadyIndex 说明之前任期的条目都已提交，
	// 此时领导者应用完它们之后才可以安全接受写入。
	ReadyIndex uint64
}

func (n *Node) Status() Status {
	n.mu.Lock()
	defer n.mu.Unlock()
	s := Status{ID: n.cfg.ID, State: n.state, Term: n.term, Leader: n.leader, CommitIndex: n.commit, LastIndex: n.lastIndexLocked()}
	s.LeaseValid = n.state == Leader && n.leaseValidLocked(time.Now())
	if n.state == Leader && n.commit >= n.readyIndex {
		s.ReadyIndex = n.readyIndex
	}
	return s
}

// IsLeader 表示本节点当前是租约有效的领导者。
func (n *Node) IsLeader() bool { return n.Status().LeaseValid }

// LeaderHint 返回当前已知的领导者 (未知为 0)。
func (n *Node) LeaderHint() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.state == Leader {
		return n.cfg.ID
	}
	return n.leader
}

// Entry 返回指定位置的日志条目 (测试/诊断用)。
func (n *Node) Entry(index uint64) (Entry, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if index == 0 || index > n.lastIndexLocked() {
		return Entry{}, false
	}
	return n.log[index], true
}

func (n *Node) Stop() {
	n.mu.Lock()
	if n.stopped {
		n.mu.Unlock()
		return
	}
	n.stopped = true
	close(n.stopCh)
	n.changedLocked()
	n.mu.Unlock()
	n.wg.Wait()
	close(n.applyCh)
	n.cfg.Transport.Close()
	if n.st != nil {
		n.st.close()
	}
}
