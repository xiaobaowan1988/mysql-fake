// Package txn 实现事务管理: 锁管理器、Undo Log、Read View 与行版本。
package txn

import (
	"errors"
	"sync"
	"time"

	"mysqlfake/metrics"
)

type LockMode uint8

const (
	LockS LockMode = iota + 1 // 共享锁
	LockX                     // 排他锁
)

var (
	ErrDeadlock    = errors.New("deadlock found when trying to get lock; transaction rolled back")
	ErrLockTimeout = errors.New("lock wait timeout exceeded")
)

type lockReq struct {
	txn     uint64
	mode    LockMode
	granted bool
	upgrade bool // S→X 升级请求 (同一事务另有一个已授予的 S 请求)
	ch      chan struct{}
}

type lockQueue struct{ reqs []*lockReq }

// LockManager 管理行级 S/X 锁。等待者按 FIFO 排队，新的等待边加入时用
// Wait-for Graph 做环路检测，成环则让当前请求者作为牺牲者返回 ErrDeadlock。
type LockManager struct {
	mu      sync.Mutex
	queues  map[string]*lockQueue
	held    map[uint64]map[string]struct{} // 事务涉及的资源 (含等待中的)
	Timeout time.Duration                  // 等待超时兜底
}

func NewLockManager() *LockManager {
	return &LockManager{
		queues:  map[string]*lockQueue{},
		held:    map[uint64]map[string]struct{}{},
		Timeout: 30 * time.Second,
	}
}

func compatible(a, b LockMode) bool { return a == LockS && b == LockS }

// Lock 为事务获取资源上的锁，必要时阻塞。
func (lm *LockManager) Lock(txn uint64, res string, mode LockMode) error {
	_, err := lm.LockTimed(txn, res, mode)
	return err
}

// LockTimed 与 Lock 相同，并返回因等待而阻塞的时间 (没有等待则为 0)。
func (lm *LockManager) LockTimed(txn uint64, res string, mode LockMode) (time.Duration, error) {
	lm.mu.Lock()
	q := lm.queues[res]
	if q == nil {
		q = &lockQueue{}
		lm.queues[res] = q
	}
	var mine *lockReq
	for _, r := range q.reqs {
		if r.txn == txn && r.granted {
			mine = r
		}
	}
	var req *lockReq
	switch {
	case mine != nil && (mine.mode == LockX || mode == LockS):
		lm.mu.Unlock()
		return 0, nil // 已持有足够强的锁
	case mine != nil: // S -> X 升级
		if lm.onlyHolder(q, txn) {
			mine.mode = LockX
			lm.mu.Unlock()
			return 0, nil
		}
		req = &lockReq{txn: txn, mode: LockX, upgrade: true, ch: make(chan struct{})}
		// 升级请求排在所有已授予请求之后、普通等待者之前
		i := 0
		for i < len(q.reqs) && q.reqs[i].granted {
			i++
		}
		q.reqs = append(q.reqs, nil)
		copy(q.reqs[i+1:], q.reqs[i:])
		q.reqs[i] = req
	default:
		req = &lockReq{txn: txn, mode: mode, ch: make(chan struct{})}
		if lm.grantable(q, req) {
			req.granted = true
			q.reqs = append(q.reqs, req)
			lm.track(txn, res)
			lm.mu.Unlock()
			return 0, nil
		}
		q.reqs = append(q.reqs, req)
	}
	lm.track(txn, res)
	if lm.hasCycle(txn) {
		lm.removeReq(q, req)
		lm.gc(res, q)
		lm.mu.Unlock()
		metrics.Deadlocks.Inc()
		return 0, ErrDeadlock
	}
	timeout := lm.Timeout
	lm.mu.Unlock()

	metrics.LockWaits.Inc()
	start := time.Now()
	done := func() time.Duration {
		d := time.Since(start)
		metrics.LockWaitTime.Observe(d.Seconds())
		return d
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-req.ch:
		return done(), nil
	case <-timer.C:
	}
	lm.mu.Lock()
	defer lm.mu.Unlock()
	select {
	case <-req.ch: // 超时的同时被授予
		return done(), nil
	default:
	}
	lm.removeReq(q, req)
	lm.grantWaiters(q)
	lm.gc(res, q)
	metrics.LockTimeouts.Inc()
	return done(), ErrLockTimeout
}

func (lm *LockManager) track(txn uint64, res string) {
	s := lm.held[txn]
	if s == nil {
		s = map[string]struct{}{}
		lm.held[txn] = s
	}
	s[res] = struct{}{}
}

func (lm *LockManager) onlyHolder(q *lockQueue, txn uint64) bool {
	for _, r := range q.reqs {
		if r.granted && r.txn != txn {
			return false
		}
	}
	return true
}

// grantable: 与所有已授予的锁相容，且前面没有排队的等待者 (保证 FIFO 公平)。
func (lm *LockManager) grantable(q *lockQueue, req *lockReq) bool {
	for _, r := range q.reqs {
		if r.txn == req.txn {
			continue
		}
		if r.granted {
			if !compatible(r.mode, req.mode) {
				return false
			}
		} else {
			return false
		}
	}
	return true
}

func (lm *LockManager) removeReq(q *lockQueue, req *lockReq) {
	for i, r := range q.reqs {
		if r == req {
			q.reqs = append(q.reqs[:i], q.reqs[i+1:]...)
			return
		}
	}
}

func (lm *LockManager) gc(res string, q *lockQueue) {
	if len(q.reqs) == 0 {
		delete(lm.queues, res)
	}
}

// grantWaiters 按队列顺序授予等待者，遇到第一个无法授予的就停止。
func (lm *LockManager) grantWaiters(q *lockQueue) {
	for i := 0; i < len(q.reqs); i++ {
		r := q.reqs[i]
		if r.granted {
			continue
		}
		ok := true
		for _, o := range q.reqs {
			if o != r && o.granted && o.txn != r.txn && !compatible(o.mode, r.mode) {
				ok = false
				break
			}
		}
		if !ok {
			return
		}
		if r.upgrade { // 用升级后的 X 替换同事务原来的 S
			for j, o := range q.reqs {
				if o.txn == r.txn && o.granted {
					q.reqs = append(q.reqs[:j], q.reqs[j+1:]...)
					if j < i {
						i--
					}
					break
				}
			}
		}
		r.granted = true
		close(r.ch)
	}
}

// ReleaseAll 释放事务持有/等待的全部锁 (事务结束时调用)。
func (lm *LockManager) ReleaseAll(txn uint64) {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	for res := range lm.held[txn] {
		q := lm.queues[res]
		if q == nil {
			continue
		}
		kept := q.reqs[:0]
		for _, r := range q.reqs {
			if r.txn != txn {
				kept = append(kept, r)
			}
		}
		q.reqs = kept
		lm.grantWaiters(q)
		lm.gc(res, q)
	}
	delete(lm.held, txn)
}

// Holds 返回事务在资源上持有的锁模式 (测试/调试用)。
func (lm *LockManager) Holds(txn uint64, res string) (LockMode, bool) {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	if q := lm.queues[res]; q != nil {
		for _, r := range q.reqs {
			if r.txn == txn && r.granted {
				return r.mode, true
			}
		}
	}
	return 0, false
}

// hasCycle 构建 Wait-for Graph (等待者 -> 阻塞它的事务) 并检测从 start 出发是否回到 start。
func (lm *LockManager) hasCycle(start uint64) bool {
	edges := map[uint64]map[uint64]bool{}
	for _, q := range lm.queues {
		for i, r := range q.reqs {
			if r.granted {
				continue
			}
			for j, o := range q.reqs {
				if o.txn == r.txn || compatible(o.mode, r.mode) && o.granted {
					continue
				}
				// 阻塞者: 冲突的已授予请求，或排在前面的等待者
				if o.granted || j < i {
					if edges[r.txn] == nil {
						edges[r.txn] = map[uint64]bool{}
					}
					edges[r.txn][o.txn] = true
				}
			}
		}
	}
	visited := map[uint64]bool{}
	var dfs func(u uint64) bool
	dfs = func(u uint64) bool {
		for v := range edges[u] {
			if v == start {
				return true
			}
			if !visited[v] {
				visited[v] = true
				if dfs(v) {
					return true
				}
			}
		}
		return false
	}
	return dfs(start)
}
