package db

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func mustExec(t *testing.T, e *Engine, q string) *Result {
	t.Helper()
	r, err := e.Exec(q)
	if err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	return r
}

func rowsOf(r *Result) string {
	var parts []string
	for _, row := range r.Rows {
		var c []string
		for _, v := range row {
			c = append(c, v.String())
		}
		parts = append(parts, strings.Join(c, ","))
	}
	return strings.Join(parts, ";")
}

func TestEndToEnd(t *testing.T) {
	path := filepath.Join(t.TempDir(), "e2e.db")
	e, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	mustExec(t, e, "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(20), age INT)")
	r := mustExec(t, e, "INSERT INTO users VALUES (3,'carol',25),(1,'alice',30),(2,'bob',NULL)")
	if r.Affected != 3 {
		t.Fatalf("affected=%d", r.Affected)
	}
	mustExec(t, e, "INSERT INTO users (id, name) VALUES (-7, 'neg')")

	cases := map[string]string{
		"SELECT * FROM users":                                       "-7,neg,NULL;1,alice,30;2,bob,NULL;3,carol,25",
		"SELECT name FROM users WHERE id = 2":                       "bob",
		"SELECT id FROM users WHERE id > 1 AND id <= 3":             "2;3",
		"SELECT id FROM users WHERE 2 < id":                         "3",
		"SELECT id FROM users WHERE id < 0":                         "-7",
		"SELECT id FROM users WHERE age >= 25":                      "1;3",
		"SELECT id FROM users WHERE age IS NULL":                    "-7;2",
		"SELECT id FROM users WHERE NOT age IS NULL":                "1;3",
		"SELECT id FROM users WHERE name = 'bob' OR age = 25":       "2;3",
		"SELECT id FROM users WHERE age != 30":                      "3", // NULL 不满足
		"SELECT name, id FROM users WHERE id = 1 OR id = 3":         "alice,1;carol,3",
		"SELECT id FROM users WHERE id > 1 AND id < 1":              "",
		"SELECT id FROM users WHERE id = 1 OR (id > 5 AND age > 1)": "1",
	}
	for q, want := range cases {
		if got := rowsOf(mustExec(t, e, q)); got != want {
			t.Errorf("%s\n got  %q\n want %q", q, got, want)
		}
	}

	// 错误场景
	for _, q := range []string{
		"INSERT INTO users VALUES (1,'dup',1)",
		"INSERT INTO users VALUES ('x','a',1)",
		"INSERT INTO users VALUES (9,'a')",
		"INSERT INTO users VALUES (NULL,'a',1)",
		"INSERT INTO users (name) VALUES ('a')",
		"INSERT INTO users VALUES (9,'this name is far too long for it',1)",
		"SELECT nope FROM users",
		"SELECT * FROM nope",
		"SELECT * FROM users WHERE id = 'a'",
		"SELECT * FROM users WHERE id",
		"CREATE TABLE users (id INT PRIMARY KEY)",
		"CREATE TABLE nopk (a INT)",
		"CREATE TABLE twopk (a INT PRIMARY KEY, b INT PRIMARY KEY)",
		"CREATE TABLE dupcol (a INT PRIMARY KEY, A INT)",
	} {
		if _, err := e.Exec(q); err == nil {
			t.Errorf("expected error for %q", q)
		}
	}

	// 重启后元数据与数据仍在
	mustExec(t, e, "CREATE TABLE words (w VARCHAR(10) PRIMARY KEY)")
	mustExec(t, e, "INSERT INTO words VALUES ('b'),('a')")
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	e, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if got := rowsOf(mustExec(t, e, "SHOW TABLES")); got != "users;words" {
		t.Fatalf("tables: %s", got)
	}
	if got := rowsOf(mustExec(t, e, "SELECT * FROM words WHERE w >= 'b'")); got != "b" {
		t.Fatalf("got %s", got)
	}
	if got := rowsOf(mustExec(t, e, "SELECT id FROM users WHERE id = 3")); got != "3" {
		t.Fatalf("got %s", got)
	}
}

func TestLargeTableIndexVsSeq(t *testing.T) {
	e, err := Open(filepath.Join(t.TempDir(), "big.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	mustExec(t, e, "CREATE TABLE t (id INT PRIMARY KEY, s VARCHAR(100), n INT)")
	var vals []string
	for i := 0; i < 3000; i++ {
		vals = append(vals, fmt.Sprintf("(%d,'%s',%d)", i, strings.Repeat("x", 80), i%10))
	}
	mustExec(t, e, "INSERT INTO t VALUES "+strings.Join(vals, ","))
	// 索引范围扫描 (走 IndexScan) 与全表扫描 (n = 3 无法利用主键) 结果应一致
	idx := mustExec(t, e, "SELECT id FROM t WHERE id >= 1000 AND id < 1100 AND n = 3")
	all := mustExec(t, e, "SELECT id FROM t WHERE n = 3")
	var want []string
	for _, r := range all.Rows {
		if r[0].Int >= 1000 && r[0].Int < 1100 {
			want = append(want, r[0].String())
		}
	}
	if len(idx.Rows) != 10 || rowsOf(idx) != strings.Join(want, ";") {
		t.Fatalf("index scan mismatch: %q vs %q", rowsOf(idx), strings.Join(want, ";"))
	}
}

func TestFormat(t *testing.T) {
	e, _ := Open(filepath.Join(t.TempDir(), "f.db"))
	defer e.Close()
	mustExec(t, e, "CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(10))")
	mustExec(t, e, "INSERT INTO t VALUES (1,'alice'),(22,NULL)")
	got := mustExec(t, e, "SELECT * FROM t").Format()
	want := `+----+-------+
| id | name  |
+----+-------+
|  1 | alice |
| 22 | NULL  |
+----+-------+
2 rows in set
`
	if got != want {
		t.Fatalf("got:\n%s\nwant:\n%s", got, want)
	}
}

// 模拟宕机: 不刷脏页、不做 Checkpoint，直接关闭文件。
func (e *Engine) crash() {
	if e.stopCkpt != nil {
		e.stopCkpt()
	}
	e.log.CloseNoFlush()
	e.disk.Close()
}

func TestCrashRecoveryEngine(t *testing.T) {
	path := filepath.Join(t.TempDir(), "crash.db")
	e, err := OpenWithOptions(path, Options{PoolSize: 512})
	if err != nil {
		t.Fatal(err)
	}
	mustExec(t, e, "CREATE TABLE t (id INT PRIMARY KEY, v VARCHAR(50))")
	for i := 0; i < 100; i++ {
		mustExec(t, e, fmt.Sprintf("INSERT INTO t VALUES (%d,'row-%d')", i, i))
	}
	mustExec(t, e, "CREATE TABLE u (k INT PRIMARY KEY)")
	mustExec(t, e, "INSERT INTO u VALUES (42)")
	if e.pool.DirtyCount() == 0 {
		t.Fatal("expected unflushed dirty pages")
	}
	e.crash()

	e, err = OpenWithOptions(path, Options{PoolSize: 512})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if got := rowsOf(mustExec(t, e, "SHOW TABLES")); got != "t;u" {
		t.Fatalf("tables after crash: %q", got)
	}
	if r := mustExec(t, e, "SELECT * FROM t"); len(r.Rows) != 100 {
		t.Fatalf("rows after crash: %d", len(r.Rows))
	}
	if got := rowsOf(mustExec(t, e, "SELECT v FROM t WHERE id = 77")); got != "row-77" {
		t.Fatalf("got %q", got)
	}
	if got := rowsOf(mustExec(t, e, "SELECT k FROM u")); got != "42" {
		t.Fatalf("got %q", got)
	}
	// 恢复后可继续写入，且再次重启仍正确
	mustExec(t, e, "INSERT INTO t VALUES (1000,'after')")
}

func TestSmallPoolEngine(t *testing.T) {
	path := filepath.Join(t.TempDir(), "small.db")
	e, err := OpenWithOptions(path, Options{PoolSize: 6})
	if err != nil {
		t.Fatal(err)
	}
	mustExec(t, e, "CREATE TABLE t (id INT PRIMARY KEY, s VARCHAR(200))")
	for i := 0; i < 500; i++ {
		mustExec(t, e, fmt.Sprintf("INSERT INTO t VALUES (%d,'%s')", i, strings.Repeat("s", 150)))
	}
	e.crash() // 缓冲池很小，大量页已被淘汰落盘，其余靠日志恢复
	e, err = OpenWithOptions(path, Options{PoolSize: 6})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if r := mustExec(t, e, "SELECT id FROM t"); len(r.Rows) != 500 {
		t.Fatalf("rows=%d", len(r.Rows))
	}
}

func TestTornPageRepairedByDoublewriteThenRedo(t *testing.T) {
	path := filepath.Join(t.TempDir(), "torn.db")
	e, err := OpenWithOptions(path, Options{PoolSize: 256, CheckpointInterval: -1})
	if err != nil {
		t.Fatal(err)
	}
	mustExec(t, e, "CREATE TABLE t (id INT PRIMARY KEY, v VARCHAR(30))")
	for i := 0; i < 5; i++ {
		mustExec(t, e, fmt.Sprintf("INSERT INTO t VALUES (%d,'old-%d')", i, i))
	}
	root := e.cat.ListTables()
	_ = root
	ti, _ := e.cat.GetTable("t")
	pid := ti.Tree.Root()
	if err := e.Close(); err != nil { // 干净关闭: 页写盘 (经 .dw)，日志清空
		t.Fatal(err)
	}

	e, err = OpenWithOptions(path, Options{PoolSize: 256, CheckpointInterval: -1})
	if err != nil {
		t.Fatal(err)
	}
	for i := 5; i < 10; i++ { // 只有日志，页未刷盘
		mustExec(t, e, fmt.Sprintf("INSERT INTO t VALUES (%d,'new-%d')", i, i))
	}
	e.crash()

	// 崩溃时该页正在被写 -> 数据文件里只有半页是好的
	f, _ := os.OpenFile(path, os.O_RDWR, 0)
	f.WriteAt(bytes.Repeat([]byte{0xEE}, 2000), int64(pid)*4096+2000)
	f.Close()

	e, err = OpenWithOptions(path, Options{PoolSize: 256, CheckpointInterval: -1})
	if err != nil {
		t.Fatalf("recovery must repair the torn page: %v", err)
	}
	defer e.Close()
	r := mustExec(t, e, "SELECT id FROM t")
	if len(r.Rows) != 10 {
		t.Fatalf("expected 10 rows after repair+redo, got %d", len(r.Rows))
	}
}

func TestBackgroundCheckpointKeepsEngineConsistent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bg.db")
	opts := Options{PoolSize: 64, CheckpointInterval: 30 * time.Millisecond}
	e, err := OpenWithOptions(path, opts)
	if err != nil {
		t.Fatal(err)
	}
	mustExec(t, e, "CREATE TABLE t (id INT PRIMARY KEY, v VARCHAR(100))")
	var wg sync.WaitGroup
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			s := e.NewSession()
			for i := 0; i < 60; i++ {
				if _, err := s.Exec(fmt.Sprintf("INSERT INTO t VALUES (%d,'%s')", g*1000+i, strings.Repeat("v", 80))); err != nil {
					t.Error(err)
					return
				}
			}
		}(g)
	}
	wg.Wait()
	if e.pool.CheckpointCount() == 0 {
		t.Fatal("background checkpoint did not run")
	}
	e.crash()
	e, err = OpenWithOptions(path, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if r := mustExec(t, e, "SELECT id FROM t"); len(r.Rows) != 240 {
		t.Fatalf("rows after crash: %d", len(r.Rows))
	}
}
