package cluster

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"mysqlfake/db"
	"mysqlfake/raft"
)

type tc struct {
	t     *testing.T
	net   *raft.Network
	ids   []int
	dirs  map[int]string
	nodes map[int]*Node
	mode  Mode
}

func newTC(t *testing.T, n int, mode Mode) *tc {
	c := &tc{t: t, net: raft.NewNetwork(), dirs: map[int]string{}, nodes: map[int]*Node{}, mode: mode}
	root := t.TempDir()
	for i := 1; i <= n; i++ {
		c.ids = append(c.ids, i)
		c.dirs[i] = filepath.Join(root, fmt.Sprintf("n%d", i))
	}
	for _, id := range c.ids {
		c.start(id)
	}
	t.Cleanup(func() {
		for _, nd := range c.nodes {
			nd.Close()
		}
	})
	return c
}

func (c *tc) start(id int) {
	nd, err := Open(Options{
		ID: id, Peers: c.ids, Transport: c.net.Transport(id), Dir: c.dirs[id], Mode: c.mode,
		SemiSyncTimeout: 200 * time.Millisecond, CommitTimeout: 800 * time.Millisecond,
		Raft: func(cfg *raft.Config) {
			cfg.HeartbeatInterval = 20 * time.Millisecond
			cfg.LeaseTimeout = 120 * time.Millisecond
			cfg.ElectionTimeoutMin = 150 * time.Millisecond
			cfg.ElectionTimeoutMax = 300 * time.Millisecond
		},
	})
	if err != nil {
		c.t.Fatal(err)
	}
	c.nodes[id] = nd
}

func (c *tc) stop(id int) {
	c.nodes[id].Close()
	delete(c.nodes, id)
}

// leader 等待出现唯一的、已就绪的领导者。
func (c *tc) leader(exclude ...int) *Node {
	c.t.Helper()
	for deadline := time.Now().Add(6 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		var found *Node
		cnt := 0
		for id, nd := range c.nodes {
			skip := false
			for _, x := range exclude {
				skip = skip || x == id
			}
			if !skip && nd.writeGuard() == nil {
				found = nd
				cnt++
			}
		}
		if cnt == 1 {
			return found
		}
	}
	c.t.Fatal("no ready leader")
	return nil
}

func (c *tc) exec(n *Node, q string) (*db.Result, error) { return n.Engine().Exec(q) }

func (c *tc) must(n *Node, q string) *db.Result {
	c.t.Helper()
	r, err := c.exec(n, q)
	if err != nil {
		c.t.Fatalf("%s: %v", q, err)
	}
	return r
}

func dump(t *testing.T, n *Node, q string) string {
	t.Helper()
	r, err := n.Engine().Exec(q)
	if err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	var parts []string
	for _, row := range r.Rows {
		var cs []string
		for _, v := range row {
			cs = append(cs, v.String())
		}
		parts = append(parts, strings.Join(cs, ","))
	}
	return strings.Join(parts, ";")
}

// converge 等待所有节点的指定查询结果一致并等于 want。
func (c *tc) converge(q, want string) {
	c.t.Helper()
	for deadline := time.Now().Add(8 * time.Second); ; time.Sleep(20 * time.Millisecond) {
		ok := true
		var last string
		for _, nd := range c.nodes {
			r, err := nd.Engine().Exec(q)
			if err != nil {
				ok = false
				continue
			}
			var parts []string
			for _, row := range r.Rows {
				var cs []string
				for _, v := range row {
					cs = append(cs, v.String())
				}
				parts = append(parts, strings.Join(cs, ","))
			}
			last = strings.Join(parts, ";")
			if last != want {
				ok = false
			}
		}
		if ok {
			return
		}
		if time.Now().After(deadline) {
			c.t.Fatalf("cluster did not converge on %q: last=%q want=%q", q, last, want)
		}
	}
}

func TestReplicationAndReadReplica(t *testing.T) {
	c := newTC(t, 3, ModeSync)
	l := c.leader()
	c.must(l, "CREATE TABLE acc (id INT PRIMARY KEY, bal INT)")
	c.must(l, "INSERT INTO acc VALUES (1,100),(2,200)")
	c.must(l, "UPDATE acc SET bal = bal + 5 WHERE id = 1")
	c.must(l, "DELETE FROM acc WHERE id = 2")
	c.converge("SELECT * FROM acc", "1,105")

	// 只读副本: 可读，写被拒绝并提示领导者
	for id, nd := range c.nodes {
		if nd == l {
			continue
		}
		if got := dump(t, nd, "SELECT bal FROM acc WHERE id = 1"); got != "105" {
			t.Fatalf("replica %d: %s", id, got)
		}
		_, err := nd.Engine().Exec("INSERT INTO acc VALUES (9,9)")
		var nl *raft.NotLeaderError
		if !errors.As(err, &nl) || nl.Leader != l.opts.ID {
			t.Fatalf("replica write: %v", err)
		}
		if _, err := nd.Engine().Exec("CREATE TABLE x (a INT PRIMARY KEY)"); err == nil {
			t.Fatal("replica DDL must be rejected")
		}
		// 等待追平到指定位置
		if err := nd.WaitApplied(l.AppliedIndex(), 3*time.Second); err != nil {
			t.Fatal(err)
		}
	}
	// 回滚的事务不会被复制
	s := l.NewSession()
	s.Exec("BEGIN")
	s.Exec("INSERT INTO acc VALUES (7,7)")
	s.Exec("ROLLBACK")
	c.must(l, "INSERT INTO acc VALUES (8,8)")
	c.converge("SELECT id FROM acc", "1;8")
}

func idsQuery(n int) string {
	var ids []string
	for i := 0; i < n; i++ {
		ids = append(ids, fmt.Sprint(i))
	}
	return strings.Join(ids, ";")
}

func TestLeaderFailoverKeepsData(t *testing.T) {
	c := newTC(t, 3, ModeSync)
	l := c.leader()
	c.must(l, "CREATE TABLE t (id INT PRIMARY KEY, v INT)")
	for i := 0; i < 10; i++ {
		c.must(l, fmt.Sprintf("INSERT INTO t VALUES (%d,%d)", i, i))
	}
	c.converge("SELECT id FROM t", idsQuery(10))

	old := l.opts.ID
	c.net.SetDown(old, true) // 领导者宕机
	nl := c.leader(old)
	for i := 10; i < 20; i++ {
		c.must(nl, fmt.Sprintf("INSERT INTO t VALUES (%d,%d)", i, i))
	}
	c.net.SetDown(old, false) // 恢复后追平，并且不再是领导者
	c.converge("SELECT id FROM t", idsQuery(20))
	if c.nodes[old].IsLeader() {
		t.Fatal("recovered node must be a follower")
	}

	// 真正重启旧领导者 (进程级): 从磁盘恢复后继续追平
	c.stop(old)
	for i := 20; i < 25; i++ {
		c.must(nl, fmt.Sprintf("INSERT INTO t VALUES (%d,%d)", i, i))
	}
	c.start(old)
	c.converge("SELECT id FROM t", idsQuery(25))
}

func TestPartitionedLeaderCannotCommitNoSplitBrain(t *testing.T) {
	c := newTC(t, 3, ModeSync)
	l := c.leader()
	c.must(l, "CREATE TABLE t (id INT PRIMARY KEY, v INT)")
	c.must(l, "INSERT INTO t VALUES (1,1)")
	c.converge("SELECT id FROM t", "1")

	old := l.opts.ID
	c.net.Isolate(old, c.ids)
	// 被隔离的旧领导者: 写入要么被租约拒绝，要么复制失败回滚，绝不会成功
	if _, err := c.exec(l, "INSERT INTO t VALUES (100,100)"); err == nil {
		t.Fatal("isolated leader must not commit")
	}
	nl := c.leader(old)
	c.must(nl, "INSERT INTO t VALUES (2,2)")
	c.net.Heal()
	c.converge("SELECT id FROM t", "1;2") // 旧领导者没有留下 id=100
	if got := dump(t, c.nodes[old], "SELECT id FROM t WHERE id = 100"); got != "" {
		t.Fatalf("failed txn leaked: %q", got)
	}
}

func TestAsyncModeDivergenceIsDetectedAndFenced(t *testing.T) {
	c := newTC(t, 3, ModeAsync)
	l := c.leader()
	c.must(l, "CREATE TABLE t (id INT PRIMARY KEY, v INT)")
	c.converge("SELECT id FROM t", "")
	old := l.opts.ID
	c.net.Isolate(old, c.ids)
	// 异步: 租约未过期前旧领导者可以"提交"，但没有复制出去
	if _, err := c.exec(l, "INSERT INTO t VALUES (100,100)"); err != nil {
		t.Skipf("lease already expired in this run: %v", err)
	}
	nl := c.leader(old)
	c.must(nl, "INSERT INTO t VALUES (1,1)")
	c.net.Heal()
	c.converge2(func() bool { return c.nodes[old].Diverged() }, "old leader must be marked diverged")
	if _, err := c.exec(c.nodes[old], "INSERT INTO t VALUES (200,200)"); !errors.Is(err, ErrDiverged) && !isNotLeader(err) {
		t.Fatalf("diverged node must reject writes, got %v", err)
	}
}

func isNotLeader(err error) bool {
	var nl *raft.NotLeaderError
	return errors.As(err, &nl)
}

func (c *tc) converge2(cond func() bool, msg string) {
	c.t.Helper()
	for deadline := time.Now().Add(8 * time.Second); time.Now().Before(deadline); time.Sleep(20 * time.Millisecond) {
		if cond() {
			return
		}
	}
	c.t.Fatal(msg)
}

func TestSemiSyncDegradesWhenReplicasDown(t *testing.T) {
	c := newTC(t, 3, ModeSemiSync)
	l := c.leader()
	c.must(l, "CREATE TABLE t (id INT PRIMARY KEY, v INT)")
	c.must(l, "INSERT INTO t VALUES (1,1)") // 副本在线: 等到确认
	for _, id := range c.ids {
		if id != l.opts.ID {
			c.net.SetDown(id, true)
		}
	}
	start := time.Now()
	// 副本全挂: 超时后退化为异步，提交仍成功 (租约还有效时)
	if _, err := c.exec(l, "INSERT INTO t VALUES (2,2)"); err == nil {
		if time.Since(start) < 150*time.Millisecond {
			t.Fatalf("semi-sync should have waited for the ack timeout, took %v", time.Since(start))
		}
	}
}

func TestConcurrentWritersConverge(t *testing.T) {
	c := newTC(t, 3, ModeSync)
	l := c.leader()
	c.must(l, "CREATE TABLE b (id INT PRIMARY KEY, bal INT)")
	c.must(l, "INSERT INTO b VALUES (0,1000),(1,1000),(2,1000),(3,1000)")
	var wg sync.WaitGroup
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			s := l.NewSession()
			for i := 0; i < 15; i++ {
				from, to := (g+i)%4, (g+i+1)%4
				for {
					s.Exec("BEGIN")
					_, e1 := s.Exec(fmt.Sprintf("UPDATE b SET bal = bal - 3 WHERE id = %d", from))
					var e2 error
					if e1 == nil {
						_, e2 = s.Exec(fmt.Sprintf("UPDATE b SET bal = bal + 3 WHERE id = %d", to))
					}
					if e1 != nil || e2 != nil {
						s.Exec("ROLLBACK")
						continue // 死锁重试
					}
					if _, err := s.Exec("COMMIT"); err != nil {
						t.Errorf("commit: %v", err)
					}
					break
				}
			}
		}(g)
	}
	wg.Wait()
	want := dump(t, l, "SELECT id, bal FROM b")
	c.converge("SELECT id, bal FROM b", want)
	total := 0
	r := c.must(l, "SELECT bal FROM b")
	for _, row := range r.Rows {
		total += int(row[0].Int)
	}
	if total != 4000 {
		t.Fatalf("total=%d", total)
	}
}

func TestFullClusterRestart(t *testing.T) {
	c := newTC(t, 3, ModeSync)
	l := c.leader()
	c.must(l, "CREATE TABLE t (id INT PRIMARY KEY, v VARCHAR(10))")
	c.must(l, "INSERT INTO t VALUES (1,'a'),(2,'b')")
	c.must(l, "UPDATE t SET v = 'z' WHERE id = 2")
	c.converge("SELECT * FROM t", "1,a;2,z")
	for _, id := range c.ids {
		c.stop(id)
	}
	for _, id := range c.ids {
		c.start(id)
	}
	l = c.leader()
	c.must(l, "INSERT INTO t VALUES (3,'c')")
	c.converge("SELECT * FROM t", "1,a;2,z;3,c")
}
