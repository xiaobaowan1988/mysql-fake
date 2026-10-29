package server

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestPoolScalesUpAndDown(t *testing.T) {
	p := NewWorkerPool(1, 8, 80*time.Millisecond)
	defer p.Close()
	if s := p.Stats(); s.Workers != 1 {
		t.Fatalf("initial workers %d", s.Workers)
	}
	var running, peak atomic.Int32
	var wg sync.WaitGroup
	release := make(chan struct{})
	var once sync.Once
	closeRelease := func() { once.Do(func() { close(release) }) }
	defer closeRelease()
	for i := 0; i < 20; i++ {
		wg.Add(1)
		p.Submit(func() {
			defer wg.Done()
			n := running.Add(1)
			for {
				old := peak.Load()
				if n <= old || peak.CompareAndSwap(old, n) {
					break
				}
			}
			<-release
			running.Add(-1)
		})
	}
	// 积压后应扩容到上限，但不超过上限
	deadline := time.Now().Add(2 * time.Second)
	for (p.Stats().Workers < 8 || p.Stats().Queued > 12) && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if s := p.Stats(); s.Workers != 8 || s.Queued != 12 {
		t.Fatalf("expected 8 workers and 12 queued, got %+v", s)
	}
	closeRelease()
	wg.Wait()
	if peak.Load() > 8 {
		t.Fatalf("exceeded max concurrency: %d", peak.Load())
	}
	// 空闲后缩容回 min
	deadline = time.Now().Add(3 * time.Second)
	for p.Stats().Workers > 1 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	s := p.Stats()
	if s.Workers != 1 || s.Retired < 7 || s.Executed != 20 {
		t.Fatalf("expected scale-down to 1 worker: %+v", s)
	}
}

func TestPoolCloseDrainsAndRejects(t *testing.T) {
	p := NewWorkerPool(0, 2, time.Second)
	var n atomic.Int32
	for i := 0; i < 50; i++ {
		p.Submit(func() { time.Sleep(time.Millisecond); n.Add(1) })
	}
	p.Close()
	if n.Load() != 50 {
		t.Fatalf("close must drain queued tasks, ran %d", n.Load())
	}
	if p.Submit(func() {}) {
		t.Fatal("submit after close must fail")
	}
}
