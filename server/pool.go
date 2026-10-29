package server

import (
	"sync"
	"time"
)

// WorkerPool 是动态大小的工作池: 任务排队，积压且还有空闲配额时扩容，
// 空闲超过 idle 的工作协程在数量高于 min 时退出。任务队列无界，Submit 永不阻塞 (事件循环依赖这一点)。
type WorkerPool struct {
	min, max int
	idle     time.Duration

	mu      sync.Mutex
	cond    *sync.Cond
	queue   []func()
	workers int
	idleW   int
	closed  bool
	wg      sync.WaitGroup

	spawned, retired, executed int64
	peak                       int
}

func NewWorkerPool(min, max int, idle time.Duration) *WorkerPool {
	if min < 0 {
		min = 0
	}
	if max < 1 {
		max = 1
	}
	if min > max {
		min = max
	}
	if idle <= 0 {
		idle = time.Second
	}
	p := &WorkerPool{min: min, max: max, idle: idle}
	p.cond = sync.NewCond(&p.mu)
	p.mu.Lock()
	for i := 0; i < min; i++ {
		p.spawnLocked()
	}
	p.mu.Unlock()
	return p
}

// Submit 提交任务；池已关闭时返回 false。
func (p *WorkerPool) Submit(task func()) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return false
	}
	p.queue = append(p.queue, task)
	// 积压多于空闲工作协程，且未达上限: 扩容
	if len(p.queue) > p.idleW && p.workers < p.max {
		p.spawnLocked()
	}
	p.cond.Signal()
	return true
}

func (p *WorkerPool) spawnLocked() {
	p.workers++
	p.spawned++
	if p.workers > p.peak {
		p.peak = p.workers
	}
	p.wg.Add(1)
	go p.work()
}

func (p *WorkerPool) work() {
	defer p.wg.Done()
	p.mu.Lock()
	defer p.mu.Unlock()
	for {
		for len(p.queue) == 0 {
			if p.closed {
				p.workers--
				return
			}
			// 空闲等待: 用定时器唤醒来实现空闲超时
			p.idleW++
			timer := time.AfterFunc(p.idle, func() {
				p.mu.Lock()
				p.cond.Broadcast()
				p.mu.Unlock()
			})
			start := time.Now()
			p.cond.Wait()
			timer.Stop()
			p.idleW--
			if len(p.queue) == 0 && !p.closed && time.Since(start) >= p.idle && p.workers > p.min {
				p.workers--
				p.retired++
				return
			}
		}
		task := p.queue[0]
		p.queue[0] = nil
		p.queue = p.queue[1:]
		p.mu.Unlock()
		task()
		p.mu.Lock()
		p.executed++
	}
}

type PoolStats struct {
	Workers, Idle, Queued int
	Spawned, Retired      int64
	Executed              int64
	Peak                  int
}

func (p *WorkerPool) Stats() PoolStats {
	p.mu.Lock()
	defer p.mu.Unlock()
	return PoolStats{Workers: p.workers, Idle: p.idleW, Queued: len(p.queue), Spawned: p.spawned,
		Retired: p.retired, Executed: p.executed, Peak: p.peak}
}

// Close 停止接收任务，执行完已排队的任务后等待全部工作协程退出。
func (p *WorkerPool) Close() {
	p.mu.Lock()
	p.closed = true
	p.cond.Broadcast()
	p.mu.Unlock()
	p.wg.Wait()
}
