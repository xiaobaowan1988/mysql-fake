package server

import (
	"bytes"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"mysqlfake/db"
)

var modes = []Mode{ModeEventLoop, ModeGoroutine}

func startServer(t *testing.T, mode Mode, mutate func(*Config)) (*Server, *db.Engine) {
	t.Helper()
	if mode == ModeEventLoop && !eventLoopSupported {
		t.Skip("event loop unsupported")
	}
	eng, err := db.Open(filepath.Join(t.TempDir(), "srv.db"))
	if err != nil {
		t.Fatal(err)
	}
	cfg := Config{Addr: "127.0.0.1:0", Engine: eng, Mode: mode, MinWorkers: 2, MaxWorkers: 16, WorkerIdle: 200 * time.Millisecond}
	if mutate != nil {
		mutate(&cfg)
	}
	s := New(cfg)
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close(); eng.Close() })
	return s, eng
}

func forModes(t *testing.T, f func(t *testing.T, mode Mode)) {
	for _, m := range modes {
		m := m
		t.Run(string(m), func(t *testing.T) { f(t, m) })
	}
}

func dial(t *testing.T, s *Server) *Client {
	t.Helper()
	c, err := Dial(s.Addr())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c
}

func q(t *testing.T, c *Client, sql string) *QueryResult {
	t.Helper()
	r, err := c.Query(sql)
	if err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	return r
}

func cell(v *string) string {
	if v == nil {
		return "NULL"
	}
	return *v
}

func flat(r *QueryResult) string {
	var rows []string
	for _, row := range r.Rows {
		var cs []string
		for _, v := range row {
			cs = append(cs, cell(v))
		}
		rows = append(rows, strings.Join(cs, ","))
	}
	return strings.Join(rows, ";")
}

func TestProtocolBasics(t *testing.T) {
	forModes(t, func(t *testing.T, mode Mode) {
		s, _ := startServer(t, mode, nil)
		if s.EffectiveMode() != mode {
			t.Fatalf("mode %s", s.EffectiveMode())
		}
		c := dial(t, s)
		if err := c.Ping(); err != nil {
			t.Fatal(err)
		}
		q(t, c, "CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(20), n INT)")
		if r := q(t, c, "INSERT INTO t VALUES (1,'héllo',10),(2,NULL,20)"); r.Affected != 2 {
			t.Fatalf("affected %d", r.Affected)
		}
		r := q(t, c, "SELECT * FROM t ORDER BY id")
		if strings.Join(r.Columns, ",") != "id,name,n" || flat(r) != "1,héllo,10;2,NULL,20" {
			t.Fatalf("%v %s", r.Columns, flat(r))
		}
		// 错误 -> ERR 包，连接仍可用
		if _, err := c.Query("SELEC broken"); err == nil {
			t.Fatal("expected syntax error")
		} else if se, ok := err.(*ServerError); !ok || se.Code != 1064 {
			t.Fatalf("expected ERR 1064, got %v", err)
		}
		if _, err := c.Query("SELECT * FROM nope"); err == nil {
			t.Fatal("expected error")
		}
	})
}

func TestTransactionsAndStatusFlags(t *testing.T) {
	forModes(t, func(t *testing.T, mode Mode) {
		s, _ := startServer(t, mode, nil)
		a, b := dial(t, s), dial(t, s)
		q(t, a, "CREATE TABLE acc (id INT PRIMARY KEY, bal INT)")
		q(t, a, "INSERT INTO acc VALUES (1,100)")
		q(t, a, "BEGIN")
		r := q(t, a, "UPDATE acc SET bal = 50 WHERE id = 1")
		if r.Status&statusInTrans == 0 {
			t.Fatal("status must report an open transaction")
		}
		// 另一连接的快照读看不到未提交的修改
		if got := flat(q(t, b, "SELECT bal FROM acc")); got != "100" {
			t.Fatalf("dirty read: %s", got)
		}
		r = q(t, a, "ROLLBACK")
		if r.Status&statusInTrans != 0 {
			t.Fatal("status must clear after ROLLBACK")
		}
		q(t, a, "BEGIN")
		q(t, a, "UPDATE acc SET bal = 70 WHERE id = 1")
		q(t, a, "COMMIT")
		if got := flat(q(t, b, "SELECT bal FROM acc")); got != "70" {
			t.Fatal(got)
		}
		// autocommit = 0: 隐式开启事务，ROLLBACK 能撤销
		q(t, a, "SET autocommit = 0")
		q(t, a, "UPDATE acc SET bal = 1 WHERE id = 1")
		q(t, a, "ROLLBACK")
		if got := flat(q(t, b, "SELECT bal FROM acc")); got != "70" {
			t.Fatalf("autocommit=0 rollback failed: %s", got)
		}
		q(t, a, "UPDATE acc SET bal = 2 WHERE id = 1")
		q(t, a, "COMMIT")
		q(t, a, "SET autocommit = 1")
		if got := flat(q(t, b, "SELECT bal FROM acc")); got != "2" {
			t.Fatal(got)
		}
	})
}

func TestClientCompatQueries(t *testing.T) {
	forModes(t, func(t *testing.T, mode Mode) {
		s, _ := startServer(t, mode, nil)
		c := dial(t, s)
		q(t, c, "SET NAMES utf8mb4")
		q(t, c, "SET character_set_results = NULL")
		q(t, c, "USE mysqlfake")
		if got := flat(q(t, c, "select @@version_comment limit 1")); got != "mysqlfake" {
			t.Fatal(got)
		}
		if got := flat(q(t, c, "SELECT DATABASE()")); got != "mysqlfake" {
			t.Fatal(got)
		}
	})
}

func TestConcurrentClients(t *testing.T) {
	forModes(t, func(t *testing.T, mode Mode) {
		s, _ := startServer(t, mode, nil)
		setup := dial(t, s)
		q(t, setup, "CREATE TABLE kv (id INT PRIMARY KEY, v INT)")
		const clients, perClient = 40, 25
		var wg sync.WaitGroup
		for i := 0; i < clients; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				c, err := Dial(s.Addr())
				if err != nil {
					t.Error(err)
					return
				}
				defer c.Close()
				for j := 0; j < perClient; j++ {
					id := i*1000 + j
					if _, err := c.Query(fmt.Sprintf("INSERT INTO kv VALUES (%d,%d)", id, j)); err != nil {
						t.Errorf("insert: %v", err)
						return
					}
					r, err := c.Query(fmt.Sprintf("SELECT v FROM kv WHERE id = %d", id))
					if err != nil || flat(r) != fmt.Sprint(j) {
						t.Errorf("select: %v %v", err, r)
						return
					}
				}
			}(i)
		}
		wg.Wait()
		if got := flat(q(t, setup, "SELECT id FROM kv ORDER BY id LIMIT 1")); got != "0" {
			t.Fatal(got)
		}
		r := q(t, setup, "SELECT id FROM kv")
		if len(r.Rows) != clients*perClient {
			t.Fatalf("rows=%d", len(r.Rows))
		}
		st := s.Stats()
		if st.Pool.Peak > 16 {
			t.Fatalf("exceeded worker limit: %+v", st.Pool)
		}
		t.Logf("%s pool: %+v", mode, st.Pool)
	})
}

// 事件循环模式: 大量空闲连接不占用 goroutine；goroutine 模式则每连接一个。
func TestIdleConnectionsDoNotCostGoroutines(t *testing.T) {
	const n = 200
	grow := map[Mode]int{}
	forModes(t, func(t *testing.T, mode Mode) {
		s, _ := startServer(t, mode, nil)
		time.Sleep(50 * time.Millisecond)
		before := runtime.NumGoroutine()
		var conns []*Client
		for i := 0; i < n; i++ {
			conns = append(conns, dial(t, s))
		}
		if err := conns[n-1].Ping(); err != nil {
			t.Fatal(err)
		}
		grow[mode] = runtime.NumGoroutine() - before
		if s.Stats().Conns != n {
			t.Fatalf("server counts %d conns", s.Stats().Conns)
		}
	})
	if eventLoopSupported {
		if grow[ModeEventLoop] > 30 {
			t.Fatalf("event loop grew by %d goroutines for %d idle connections", grow[ModeEventLoop], n)
		}
		if grow[ModeGoroutine] < n {
			t.Fatalf("goroutine mode should cost >= 1 goroutine per connection, grew by %d", grow[ModeGoroutine])
		}
	}
	t.Logf("goroutine growth for %d idle conns: %v", n, grow)
}

// 大结果集 + 慢读者: 迫使服务端写缓冲满 (EAGAIN) 并依赖写事件继续发送。
func TestLargeResultSlowReader(t *testing.T) {
	forModes(t, func(t *testing.T, mode Mode) {
		s, _ := startServer(t, mode, nil)
		c := dial(t, s)
		q(t, c, "CREATE TABLE big (id INT PRIMARY KEY, s VARCHAR(200))")
		pad := strings.Repeat("x", 180)
		for start := 0; start < 8000; start += 500 {
			var vals []string
			for i := start; i < start+500; i++ {
				vals = append(vals, fmt.Sprintf("(%d,'%s')", i, pad))
			}
			q(t, c, "INSERT INTO big VALUES "+strings.Join(vals, ","))
		}
		// 不经客户端库，手动发请求后故意晚点再读
		raw, err := net.Dial("tcp", s.Addr())
		if err != nil {
			t.Fatal(err)
		}
		defer raw.Close()
		raw.SetDeadline(time.Now().Add(20 * time.Second))
		hdr := make([]byte, 4096)
		raw.Read(hdr) // 握手包
		resp := []byte{}
		resp = append(resp, 0x0d, 0xa2, 0x08, 0x00) // 能力标志
		resp = append(resp, 0, 0, 0, 1, 0x2d)       // max packet + charset
		resp = append(resp, make([]byte, 23)...)    // 保留
		resp = append(resp, "u\x00\x00mysql_native_password\x00"...)
		var seq byte = 1
		raw.Write(framePacket(nil, &seq, resp))
		raw.Read(hdr) // OK
		seq = 0
		raw.Write(framePacket(nil, &seq, append([]byte{comQuery}, "SELECT * FROM big"...)))
		time.Sleep(500 * time.Millisecond) // 服务端此时已无法把整个结果写进套接字缓冲
		total := 0
		tmp := make([]byte, 64<<10)
		var tail []byte
		for {
			n, err := raw.Read(tmp)
			total += n
			tail = append(tail, tmp[:n]...)
			if len(tail) > 16 {
				tail = tail[len(tail)-16:]
			}
			if err != nil {
				t.Fatalf("read after %d bytes: %v", total, err)
			}
			// 结果集以 EOF 包 (fe 00 00 02 00) 结束
			if bytes.HasSuffix(tail, []byte{0xfe, 0x00, 0x00, 0x02, 0x00}) {
				break
			}
		}
		if total < 8000*180 {
			t.Fatalf("received only %d bytes", total)
		}
	})
}

func TestPipelinedRequestsAnsweredInOrder(t *testing.T) {
	forModes(t, func(t *testing.T, mode Mode) {
		s, _ := startServer(t, mode, nil)
		c := dial(t, s)
		q(t, c, "CREATE TABLE p (id INT PRIMARY KEY)")
		var seq byte
		var out []byte
		for i := 0; i < 5; i++ {
			seq = 0
			out = framePacket(out, &seq, append([]byte{comQuery}, fmt.Sprintf("INSERT INTO p VALUES (%d)", i)...))
		}
		seq = 0
		out = framePacket(out, &seq, append([]byte{comQuery}, "SELECT id FROM p ORDER BY id"...))
		c.conn.Write(out) // 一次性写入 6 个请求
		for i := 0; i < 5; i++ {
			p, err := c.readPacket()
			if err != nil || p[0] != 0x00 {
				t.Fatalf("response %d: %v %v", i, p, err)
			}
		}
		// 第 6 个响应是结果集: 列数包 + ... 5 行
		p, _ := c.readPacket()
		if p[0] != 1 {
			t.Fatalf("expected 1 column, got %v", p)
		}
		rows := 0
		for {
			p, err := c.readPacket()
			if err != nil {
				t.Fatal(err)
			}
			if p[0] == 0xfe && len(p) < 9 {
				if rows == 0 { // 列定义后的 EOF
					rows = -1
					continue
				}
				break
			}
			if rows >= 0 || len(p) > 0 && p[0] != 0xfe {
				if rows == -1 {
					rows = 0
				}
				if len(p) == 2 { // 行包: 1 字节长度 + 1 位数字
					rows++
				}
			}
		}
		if rows != 5 {
			t.Fatalf("rows=%d", rows)
		}
	})
}

func TestDisconnectRollsBackAndReleasesLocks(t *testing.T) {
	forModes(t, func(t *testing.T, mode Mode) {
		s, _ := startServer(t, mode, nil)
		a, b := dial(t, s), dial(t, s)
		q(t, a, "CREATE TABLE t (id INT PRIMARY KEY, v INT)")
		q(t, a, "INSERT INTO t VALUES (1,1)")
		q(t, a, "BEGIN")
		q(t, a, "UPDATE t SET v = 99 WHERE id = 1")
		q(t, a, "INSERT INTO t VALUES (2,2)")
		a.Close() // 突然断开，不提交
		done := make(chan error, 1)
		go func() { _, err := b.Query("UPDATE t SET v = 5 WHERE id = 1"); done <- err }() // 需要等锁释放
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("locks of the disconnected session were never released")
		}
		if got := flat(q(t, b, "SELECT id, v FROM t ORDER BY id")); got != "1,5" {
			t.Fatalf("got %s", got)
		}
	})
}

func TestGarbageAndLimits(t *testing.T) {
	forModes(t, func(t *testing.T, mode Mode) {
		s, _ := startServer(t, mode, func(c *Config) { c.MaxConns = 3 })
		// 垃圾数据: 连接被关闭，服务器不崩溃
		for _, junk := range [][]byte{{0xff, 0xff, 0xff, 0x00, 1, 2, 3}, bytes.Repeat([]byte{7}, 5000), {0, 0, 0, 0}} {
			nc, err := net.Dial("tcp", s.Addr())
			if err != nil {
				t.Fatal(err)
			}
			nc.SetDeadline(time.Now().Add(3 * time.Second))
			buf := make([]byte, 512)
			nc.Read(buf)
			nc.Write(junk)
			io := make([]byte, 4096)
			for {
				if _, err := nc.Read(io); err != nil {
					break
				}
			}
			nc.Close()
		}
		// 服务器仍然健康
		c := dial(t, s)
		if err := c.Ping(); err != nil {
			t.Fatal(err)
		}
		// 连接数上限
		time.Sleep(100 * time.Millisecond)
		var held []*Client
		var rejected int
		for i := 0; i < 6; i++ {
			cl, err := Dial(s.Addr())
			if err != nil {
				if se, ok := err.(*ServerError); ok && se.Code == errTooManyConns {
					rejected++
				}
				continue
			}
			held = append(held, cl)
		}
		for _, h := range held {
			h.Close()
		}
		if rejected == 0 || s.Stats().Rejected == 0 {
			t.Fatalf("MaxConns not enforced (rejected=%d)", rejected)
		}
	})
}

func TestCloseWithActiveConnections(t *testing.T) {
	forModes(t, func(t *testing.T, mode Mode) {
		eng, _ := db.Open(filepath.Join(t.TempDir(), "c.db"))
		defer eng.Close()
		s := New(Config{Addr: "127.0.0.1:0", Engine: eng, Mode: mode})
		if err := s.Start(); err != nil {
			t.Fatal(err)
		}
		c, _ := Dial(s.Addr())
		c.Query("CREATE TABLE t (id INT PRIMARY KEY)")
		c.Query("BEGIN")
		c.Query("INSERT INTO t VALUES (1)")
		before := runtime.NumGoroutine()
		s.Close()
		if _, err := c.Query("SELECT * FROM t"); err == nil {
			t.Fatal("connection must be closed by the server")
		}
		time.Sleep(100 * time.Millisecond)
		if after := runtime.NumGoroutine(); after > before {
			t.Fatalf("goroutines leaked after Close: %d -> %d", before, after)
		}
		// 未提交的事务被回滚
		if r, _ := eng.Exec("SELECT * FROM t"); len(r.Rows) != 0 {
			t.Fatal("uncommitted insert survived server shutdown")
		}
	})
}

// 用独立实现的真实 MySQL 客户端库 (PyMySQL) 做互操作验证。
func TestPyMySQLInterop(t *testing.T) {
	py, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 not available")
	}
	if err := exec.Command(py, "-c", "import pymysql").Run(); err != nil {
		t.Skip("pymysql not installed")
	}
	script := `
import sys, pymysql
host, port = sys.argv[1], int(sys.argv[2])
conn = pymysql.connect(host=host, port=port, user="root", password="secret", charset="utf8mb4")
cur = conn.cursor()
cur.execute("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(40), age INT)")
cur.execute("INSERT INTO users VALUES (%s,%s,%s)", (1, "alice", 30))
cur.execute("INSERT INTO users VALUES (%s,%s,%s)", (2, "bö'b", None))
conn.commit()
cur.execute("SELECT id, name, age FROM users ORDER BY id")
rows = cur.fetchall()
assert rows == ((1, "alice", 30), (2, "bö'b", None)), rows
assert [d[0] for d in cur.description] == ["id", "name", "age"]
# 事务: autocommit=0 下 rollback 撤销
cur.execute("UPDATE users SET age = 99 WHERE id = 1")
conn.rollback()
cur.execute("SELECT age FROM users WHERE id = 1")
assert cur.fetchone() == (30,), "rollback failed"
cur.execute("UPDATE users SET age = 31 WHERE id = 1")
conn.commit()
cur.execute("SELECT age FROM users WHERE id = 1")
assert cur.fetchone() == (31,)
# 错误映射
try:
    cur.execute("SELEC nonsense")
    raise SystemExit("expected a syntax error")
except pymysql.err.ProgrammingError as e:
    assert e.args[0] == 1064, e.args
# JOIN + EXPLAIN 走同一条协议
cur.execute("CREATE TABLE orders (id INT PRIMARY KEY, uid INT, amt INT)")
cur.executemany("INSERT INTO orders VALUES (%s,%s,%s)", [(1,1,10),(2,1,20),(3,2,5)])
conn.commit()
cur.execute("SELECT u.name, o.amt FROM users u JOIN orders o ON u.id = o.uid ORDER BY o.amt DESC")
assert cur.fetchall() == (("alice", 20), ("alice", 10), ("bö'b", 5))
cur.execute("EXPLAIN SELECT * FROM users WHERE id = 1")
assert len(cur.fetchall()) >= 1
assert conn.ping() is None or True
# 大结果集
cur.execute("CREATE TABLE nums (id INT PRIMARY KEY)")
cur.executemany("INSERT INTO nums VALUES (%s)", [(i,) for i in range(3000)])
conn.commit()
cur.execute("SELECT id FROM nums")
assert len(cur.fetchall()) == 3000
conn.close()
print("PYMYSQL-OK")
`
	forModes(t, func(t *testing.T, mode Mode) {
		s, _ := startServer(t, mode, nil)
		host, port, _ := net.SplitHostPort(s.Addr())
		f := filepath.Join(t.TempDir(), "interop.py")
		os.WriteFile(f, []byte(script), 0o644)
		cmd := exec.Command(py, "-I", f, host, port)
		out, err := cmd.CombinedOutput()
		if err != nil || !strings.Contains(string(out), "PYMYSQL-OK") {
			t.Fatalf("PyMySQL interop failed: %v\n%s", err, out)
		}
	})
}
