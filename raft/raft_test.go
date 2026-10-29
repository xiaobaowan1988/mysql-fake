package raft

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

type testCluster struct {
	t     *testing.T
	net   *Network
	ids   []int
	nodes map[int]*Node
	dirs  map[int]string
	mu    sync.Mutex
	got   map[int][]string // 每个节点按序应用的命令
	trunc map[int][]uint64
}

func fastCfg(c *Config) {
	c.HeartbeatInterval = 20 * time.Millisecond
	c.LeaseTimeout = 120 * time.Millisecond
	c.ElectionTimeoutMin = 150 * time.Millisecond
	c.ElectionTimeoutMax = 300 * time.Millisecond
}

func newCluster(t *testing.T, n int, persist bool) *testCluster {
	c := &testCluster{t: t, net: NewNetwork(), nodes: map[int]*Node{}, dirs: map[int]string{},
		got: map[int][]string{}, trunc: map[int][]uint64{}}
	for i := 1; i <= n; i++ {
		c.ids = append(c.ids, i)
		if persist {
			c.dirs[i] = t.TempDir()
		}
	}
	for _, id := range c.ids {
		c.start(id)
	}
	t.Cleanup(func() {
		for _, nd := range c.nodes {
			nd.Stop()
		}
	})
	return c
}

func (c *testCluster) start(id int) {
	cfg := Config{ID: id, Peers: c.ids, Transport: c.net.Transport(id), Dir: c.dirs[id]}
	fastCfg(&cfg)
	cfg.OnTruncate = func(from uint64) {
		c.mu.Lock()
		c.trunc[id] = append(c.trunc[id], from)
		c.mu.Unlock()
	}
	nd, err := NewNode(cfg)
	if err != nil {
		c.t.Fatal(err)
	}
	c.nodes[id] = nd
	c.mu.Lock()
	c.got[id] = nil // 重启后会重新投递全部已提交条目
	c.mu.Unlock()
	go func() {
		for e := range nd.ApplyCh() {
			if !e.Noop {
				c.mu.Lock()
				c.got[id] = append(c.got[id], string(e.Data))
				c.mu.Unlock()
			}
		}
	}()
}

func (c *testCluster) stop(id int) {
	c.nodes[id].Stop()
	delete(c.nodes, id)
}

func (c *testCluster) applied(id int) []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.got[id]...)
}

func (c *testCluster) waitLeader(exclude ...int) *Node {
	c.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var leaders []*Node
		for id, nd := range c.nodes {
			skip := false
			for _, x := range exclude {
				if x == id {
					skip = true
				}
			}
			if !skip && nd.IsLeader() {
				leaders = append(leaders, nd)
			}
		}
		if len(leaders) == 1 {
			return leaders[0]
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.t.Fatal("no leader elected")
	return nil
}

func (c *testCluster) propose(l *Node, cmd string) error {
	idx, term, err := l.Propose([]byte(cmd))
	if err != nil {
		return err
	}
	return l.WaitCommitted(idx, term, 3*time.Second)
}

func (c *testCluster) waitApplied(id, n int) {
	c.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if len(c.applied(id)) >= n {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.t.Fatalf("node %d applied %d, want %d", id, len(c.applied(id)), n)
}

func TestElectionAndReplication(t *testing.T) {
	c := newCluster(t, 3, false)
	l := c.waitLeader()
	for i := 0; i < 20; i++ {
		if err := c.propose(l, fmt.Sprintf("cmd-%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	for _, id := range c.ids {
		c.waitApplied(id, 20)
		got := c.applied(id)
		for i := 0; i < 20; i++ {
			if got[i] != fmt.Sprintf("cmd-%d", i) {
				t.Fatalf("node %d entry %d = %s", id, i, got[i])
			}
		}
	}
	// 跟随者拒绝写入并提示领导者
	for _, id := range c.ids {
		if id == l.cfg.ID {
			continue
		}
		_, _, err := c.nodes[id].Propose([]byte("x"))
		nl, ok := err.(*NotLeaderError)
		if !ok || nl.Leader != l.cfg.ID {
			t.Fatalf("follower propose: %v", err)
		}
	}
}

func TestLeaderFailover(t *testing.T) {
	c := newCluster(t, 3, false)
	l := c.waitLeader()
	for i := 0; i < 5; i++ {
		c.propose(l, fmt.Sprintf("a%d", i))
	}
	old := l.cfg.ID
	c.net.SetDown(old, true)
	nl := c.waitLeader(old)
	if nl.cfg.ID == old {
		t.Fatal("same leader")
	}
	for i := 0; i < 5; i++ {
		if err := c.propose(nl, fmt.Sprintf("b%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	// 旧领导者恢复后降级并追平
	c.net.SetDown(old, false)
	c.waitApplied(old, 10)
	want := c.applied(nl.cfg.ID)
	for _, id := range c.ids {
		c.waitApplied(id, 10)
		got := c.applied(id)
		for i := range want[:10] {
			if got[i] != want[i] {
				t.Fatalf("node %d diverged at %d: %s vs %s", id, i, got[i], want[i])
			}
		}
	}
	if c.nodes[old].IsLeader() {
		t.Fatal("old leader must have stepped down")
	}
}

func TestIsolatedLeaderLosesLeaseAndGetsOverwritten(t *testing.T) {
	c := newCluster(t, 3, false)
	l := c.waitLeader()
	c.propose(l, "committed")
	old := l.cfg.ID
	c.net.Isolate(old, c.ids)
	// 租约还没过期时旧领导者仍会接受写入，但无法提交
	idx, term, err := l.Propose([]byte("doomed"))
	if err != nil {
		t.Fatal(err)
	}
	if err := l.WaitCommitted(idx, term, 100*time.Millisecond); err == nil {
		t.Fatal("isolated leader must not commit")
	}
	// 租约过期后: 降级，拒绝写入
	deadline := time.Now().Add(2 * time.Second)
	for l.IsLeader() && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if l.IsLeader() {
		t.Fatal("isolated leader kept its lease")
	}
	if _, _, err := l.Propose([]byte("x")); err == nil {
		t.Fatal("propose must fail without lease")
	}
	// 多数派一侧选出新领导者并提交
	nl := c.waitLeader(old)
	if err := c.propose(nl, "newer"); err != nil {
		t.Fatal(err)
	}
	// 恢复网络: doomed 被覆盖，所有节点收敛
	c.net.Heal()
	if err := l.WaitCommitted(idx, term, 3*time.Second); err != ErrOverwritten {
		t.Fatalf("expected overwritten, got %v", err)
	}
	c.mu.Lock()
	tr := c.trunc[old]
	c.mu.Unlock()
	if len(tr) == 0 || tr[0] > idx {
		t.Fatalf("OnTruncate not reported: %v idx=%d", tr, idx)
	}
	for _, id := range c.ids {
		c.waitApplied(id, 2)
		if got := c.applied(id); got[0] != "committed" || got[1] != "newer" {
			t.Fatalf("node %d: %v", id, got)
		}
	}
}

func TestElectionSafetyUnderChaos(t *testing.T) {
	c := newCluster(t, 5, false)
	leaders := map[uint64]int{} // term -> leader
	var bad string
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			for _, id := range c.ids {
				c.mu.Lock()
				nd := c.nodes[id]
				c.mu.Unlock()
				if nd == nil {
					continue
				}
				s := nd.Status()
				if s.State == Leader {
					if prev, ok := leaders[s.Term]; ok && prev != id {
						bad = fmt.Sprintf("term %d has leaders %d and %d", s.Term, prev, id)
					}
					leaders[s.Term] = id
				}
			}
			time.Sleep(2 * time.Millisecond)
		}
	}()
	for round := 0; round < 8; round++ {
		victim := c.ids[round%len(c.ids)]
		c.net.Isolate(victim, c.ids)
		time.Sleep(250 * time.Millisecond)
		c.net.Heal()
		time.Sleep(200 * time.Millisecond)
	}
	close(stop)
	wg.Wait()
	if bad != "" {
		t.Fatal(bad)
	}
	l := c.waitLeader()
	if err := c.propose(l, "final"); err != nil {
		t.Fatal(err)
	}
}

func TestMajorityRequired(t *testing.T) {
	c := newCluster(t, 5, false)
	l := c.waitLeader()
	others := []int{}
	for _, id := range c.ids {
		if id != l.cfg.ID {
			others = append(others, id)
		}
	}
	c.net.SetDown(others[0], true)
	c.net.SetDown(others[1], true)
	if err := c.propose(l, "two-down"); err != nil { // 5 节点宕 2 个仍有多数派
		t.Fatal(err)
	}
	c.net.SetDown(others[2], true) // 只剩 leader + 1
	idx, term, err := l.Propose([]byte("no-quorum"))
	if err == nil {
		if err := l.WaitCommitted(idx, term, 300*time.Millisecond); err == nil {
			t.Fatal("must not commit without a majority")
		}
	}
}

func TestPersistenceAndRestart(t *testing.T) {
	c := newCluster(t, 3, true)
	l := c.waitLeader()
	for i := 0; i < 10; i++ {
		if err := c.propose(l, fmt.Sprintf("p%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	for _, id := range c.ids {
		c.waitApplied(id, 10)
	}
	// 重启一个跟随者: 日志从磁盘恢复，之后继续追平新条目
	var victim int
	for _, id := range c.ids {
		if id != l.cfg.ID {
			victim = id
			break
		}
	}
	c.stop(victim)
	for i := 10; i < 15; i++ {
		if err := c.propose(l, fmt.Sprintf("p%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	c.start(victim)
	c.waitApplied(victim, 15)
	got := c.applied(victim)
	for i := 0; i < 15; i++ {
		if got[i] != fmt.Sprintf("p%d", i) {
			t.Fatalf("restarted node entry %d = %s", i, got[i])
		}
	}
	// 全部重启: 数据不丢，且能选出新领导者继续写
	for _, id := range c.ids {
		c.stop(id)
	}
	for _, id := range c.ids {
		c.start(id)
	}
	l = c.waitLeader()
	if err := c.propose(l, "after-restart"); err != nil {
		t.Fatal(err)
	}
	for _, id := range c.ids {
		c.waitApplied(id, 16)
		if got := c.applied(id); got[15] != "after-restart" || got[0] != "p0" {
			t.Fatalf("node %d: %v", id, got)
		}
	}
}

func TestSingleNodeCluster(t *testing.T) {
	c := newCluster(t, 1, false)
	l := c.waitLeader()
	if err := c.propose(l, "solo"); err != nil {
		t.Fatal(err)
	}
	c.waitApplied(1, 1)
}

func TestWaitReplicated(t *testing.T) {
	c := newCluster(t, 3, false)
	l := c.waitLeader()
	idx, term, err := l.Propose([]byte("x"))
	if err != nil {
		t.Fatal(err)
	}
	if err := l.WaitReplicated(idx, term, 2, 2*time.Second); err != nil {
		t.Fatal(err)
	}
}

func TestTCPTransport(t *testing.T) {
	ids := []int{1, 2, 3}
	trs := map[int]*TCPTransport{}
	addrs := map[int]string{}
	for _, id := range ids {
		tr, err := NewTCPTransport(id, "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		trs[id] = tr
		addrs[id] = tr.Addr()
	}
	nodes := map[int]*Node{}
	var mu sync.Mutex
	got := map[int]int{}
	for _, id := range ids {
		trs[id].SetPeers(addrs)
		cfg := Config{ID: id, Peers: ids, Transport: trs[id]}
		fastCfg(&cfg)
		nd, err := NewNode(cfg)
		if err != nil {
			t.Fatal(err)
		}
		nodes[id] = nd
		go func(id int, nd *Node) {
			for e := range nd.ApplyCh() {
				if !e.Noop {
					mu.Lock()
					got[id]++
					mu.Unlock()
				}
			}
		}(id, nd)
	}
	defer func() {
		for _, nd := range nodes {
			nd.Stop()
		}
	}()
	var l *Node
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline) && l == nil; time.Sleep(10 * time.Millisecond) {
		for _, nd := range nodes {
			if nd.IsLeader() {
				l = nd
			}
		}
	}
	if l == nil {
		t.Fatal("no leader over TCP")
	}
	idx, term, err := l.Propose([]byte("over-tcp"))
	if err != nil {
		t.Fatal(err)
	}
	if err := l.WaitCommitted(idx, term, 3*time.Second); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		ok := got[1] == 1 && got[2] == 1 && got[3] == 1
		mu.Unlock()
		if ok {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("not all nodes applied: %v", got)
}
