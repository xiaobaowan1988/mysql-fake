package db

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"mysqlfake/txn"
)

func newEngine(t *testing.T) *Engine {
	t.Helper()
	e, err := Open(filepath.Join(t.TempDir(), "txn.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { e.Close() })
	return e
}

func sexec(t *testing.T, s *Session, q string) *Result {
	t.Helper()
	r, err := s.Exec(q)
	if err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	return r
}

func setupAccounts(t *testing.T, e *Engine) {
	t.Helper()
	mustExec(t, e, "CREATE TABLE acc (id INT PRIMARY KEY, bal INT, name VARCHAR(20))")
	mustExec(t, e, "INSERT INTO acc VALUES (1,100,'a'),(2,200,'b'),(3,300,'c')")
}

func execAsync(s *Session, q string) chan error {
	ch := make(chan error, 1)
	go func() { _, err := s.Exec(q); ch <- err }()
	return ch
}

func blocked(t *testing.T, ch chan error) {
	t.Helper()
	select {
	case err := <-ch:
		t.Fatalf("expected statement to block, got %v", err)
	case <-time.After(80 * time.Millisecond):
	}
}

func done(t *testing.T, ch chan error) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(3 * time.Second):
		t.Fatal("statement still blocked")
		return nil
	}
}

func TestUpdateDeleteBasic(t *testing.T) {
	e := newEngine(t)
	setupAccounts(t, e)
	if r := mustExec(t, e, "UPDATE acc SET bal = bal + 5, name = 'z' WHERE id >= 2"); r.Affected != 2 {
		t.Fatalf("affected %d", r.Affected)
	}
	if got := rowsOf(mustExec(t, e, "SELECT * FROM acc")); got != "1,100,a;2,205,z;3,305,z" {
		t.Fatalf("got %s", got)
	}
	mustExec(t, e, "UPDATE acc SET name = NULL WHERE id = 1")
	if got := rowsOf(mustExec(t, e, "SELECT name FROM acc WHERE name IS NULL")); got != "NULL" {
		t.Fatalf("got %s", got)
	}
	if r := mustExec(t, e, "DELETE FROM acc WHERE bal > 300"); r.Affected != 1 {
		t.Fatalf("affected %d", r.Affected)
	}
	if got := rowsOf(mustExec(t, e, "SELECT id FROM acc")); got != "1;2" {
		t.Fatalf("got %s", got)
	}
	// 删除后可以用相同主键重新插入
	mustExec(t, e, "INSERT INTO acc VALUES (3,1,'again')")
	if got := rowsOf(mustExec(t, e, "SELECT bal FROM acc WHERE id = 3")); got != "1" {
		t.Fatalf("got %s", got)
	}
	if _, err := e.Exec("INSERT INTO acc VALUES (3,1,'dup')"); err == nil {
		t.Fatal("expected duplicate error")
	}
	for _, q := range []string{
		"UPDATE acc SET id = 9", "UPDATE acc SET nope = 1", "UPDATE acc SET bal = 'x'",
		"UPDATE acc SET name = 1", "UPDATE acc SET bal = 1, bal = 2", "UPDATE acc SET bal = name + 1",
		"DELETE FROM nope", "UPDATE acc SET bal = 1 WHERE nope = 1",
	} {
		if _, err := e.Exec(q); err == nil {
			t.Errorf("expected error for %q", q)
		}
	}
	if got := rowsOf(mustExec(t, e, "SELECT id FROM acc WHERE bal - 1 = 99")); got != "1" {
		t.Fatalf("arithmetic in where: %s", got)
	}
}

func TestRollback(t *testing.T) {
	e := newEngine(t)
	setupAccounts(t, e)
	s := e.NewSession()
	before := rowsOf(mustExec(t, e, "SELECT * FROM acc"))
	sexec(t, s, "BEGIN")
	sexec(t, s, "INSERT INTO acc VALUES (4,400,'d')")
	sexec(t, s, "UPDATE acc SET bal = 0 WHERE id = 1")
	sexec(t, s, "DELETE FROM acc WHERE id = 2")
	sexec(t, s, "UPDATE acc SET name = 'x' WHERE id = 1") // 同一行多次修改
	sexec(t, s, "DELETE FROM acc WHERE id = 4")
	sexec(t, s, "INSERT INTO acc VALUES (4,444,'d2')") // 在自己的删除标记上重新插入
	if got := rowsOf(sexec(t, s, "SELECT id, bal FROM acc")); got != "1,0;3,300;4,444" {
		t.Fatalf("own changes should be visible: %s", got)
	}
	sexec(t, s, "ROLLBACK")
	if got := rowsOf(mustExec(t, e, "SELECT * FROM acc")); got != before {
		t.Fatalf("rollback did not restore:\n got  %s\n want %s", got, before)
	}
	if s.InTxn() || e.mgr.ActiveCount() != 0 || e.mgr.Undo.Len() != 0 {
		t.Fatal("txn state not cleaned up")
	}
}

func TestStatementLevelRollback(t *testing.T) {
	e := newEngine(t)
	setupAccounts(t, e)
	s := e.NewSession()
	sexec(t, s, "BEGIN")
	sexec(t, s, "INSERT INTO acc VALUES (10,1,'x')")
	if _, err := s.Exec("INSERT INTO acc VALUES (11,1,'y'),(12,1,'y'),(1,1,'dup')"); err == nil {
		t.Fatal("expected duplicate error")
	}
	if !s.InTxn() {
		t.Fatal("txn should survive a failed statement")
	}
	if got := rowsOf(sexec(t, s, "SELECT id FROM acc WHERE id >= 10")); got != "10" {
		t.Fatalf("failed statement must be rolled back, got %s", got)
	}
	sexec(t, s, "COMMIT")
	if got := rowsOf(mustExec(t, e, "SELECT id FROM acc WHERE id >= 10")); got != "10" {
		t.Fatalf("got %s", got)
	}
	// 自动提交下的多行 INSERT 失败: 整条语句原子回滚
	if _, err := e.Exec("INSERT INTO acc VALUES (20,1,'a'),(21,1,'b'),(1,1,'dup')"); err == nil {
		t.Fatal("expected error")
	}
	if got := rowsOf(mustExec(t, e, "SELECT id FROM acc WHERE id >= 20")); got != "" {
		t.Fatalf("autocommit statement must be atomic, got %s", got)
	}
}

func TestSnapshotIsolation(t *testing.T) {
	e := newEngine(t)
	setupAccounts(t, e)
	r, w := e.NewSession(), e.NewSession()

	sexec(t, r, "BEGIN")
	if got := rowsOf(sexec(t, r, "SELECT bal FROM acc WHERE id = 1")); got != "100" {
		t.Fatal(got)
	}
	// 其他会话提交 UPDATE / DELETE / INSERT
	sexec(t, w, "UPDATE acc SET bal = 111 WHERE id = 1")
	sexec(t, w, "DELETE FROM acc WHERE id = 2")
	sexec(t, w, "INSERT INTO acc VALUES (9,9,'new')")
	// 可重复读: 已建立的快照看不到这些修改
	if got := rowsOf(sexec(t, r, "SELECT id, bal FROM acc")); got != "1,100;2,200;3,300" {
		t.Fatalf("snapshot violated: %s", got)
	}
	if got := rowsOf(sexec(t, r, "SELECT bal FROM acc WHERE id = 1")); got != "100" {
		t.Fatalf("point read violated: %s", got)
	}
	// 当前读能看到最新数据
	if got := rowsOf(sexec(t, r, "SELECT id FROM acc FOR SHARE")); got != "1;3;9" {
		t.Fatalf("current read: %s", got)
	}
	sexec(t, r, "COMMIT")
	if got := rowsOf(sexec(t, r, "SELECT id, bal FROM acc")); got != "1,111;3,300;9,9" {
		t.Fatalf("new autocommit statement sees latest: %s", got)
	}
}

func TestUncommittedInvisibleAndNoBlockingReads(t *testing.T) {
	e := newEngine(t)
	setupAccounts(t, e)
	w, r := e.NewSession(), e.NewSession()
	sexec(t, w, "BEGIN")
	sexec(t, w, "UPDATE acc SET bal = 999 WHERE id = 1")
	sexec(t, w, "INSERT INTO acc VALUES (7,7,'u')")
	sexec(t, w, "DELETE FROM acc WHERE id = 3")
	// 读者无锁，且只看到已提交的数据 (通过 undo 链回到旧版本)
	if got := rowsOf(sexec(t, r, "SELECT id, bal FROM acc")); got != "1,100;2,200;3,300" {
		t.Fatalf("dirty read: %s", got)
	}
	if got := rowsOf(sexec(t, w, "SELECT id, bal FROM acc")); got != "1,999;2,200;7,7" {
		t.Fatalf("own view: %s", got)
	}
	sexec(t, w, "ROLLBACK")
	if got := rowsOf(sexec(t, r, "SELECT id, bal FROM acc")); got != "1,100;2,200;3,300" {
		t.Fatalf("after rollback: %s", got)
	}
}

func TestWriteWriteBlocksNoLostUpdate(t *testing.T) {
	e := newEngine(t)
	setupAccounts(t, e)
	s1, s2 := e.NewSession(), e.NewSession()
	sexec(t, s1, "BEGIN")
	sexec(t, s1, "UPDATE acc SET bal = bal + 1 WHERE id = 1")
	ch := execAsync(s2, "UPDATE acc SET bal = bal + 10 WHERE id = 1")
	blocked(t, ch)
	sexec(t, s1, "COMMIT")
	if err := done(t, ch); err != nil {
		t.Fatal(err)
	}
	if got := rowsOf(mustExec(t, e, "SELECT bal FROM acc WHERE id = 1")); got != "111" {
		t.Fatalf("lost update: %s", got)
	}
}

func TestBlockedWriterSeesDeleteAfterWait(t *testing.T) {
	e := newEngine(t)
	setupAccounts(t, e)
	s1, s2 := e.NewSession(), e.NewSession()
	sexec(t, s1, "BEGIN")
	sexec(t, s1, "DELETE FROM acc WHERE id = 1")
	ch := execAsync(s2, "UPDATE acc SET bal = 0 WHERE id = 1")
	blocked(t, ch)
	sexec(t, s1, "COMMIT")
	done(t, ch)
	if got := rowsOf(mustExec(t, e, "SELECT id FROM acc")); got != "2;3" {
		t.Fatalf("update must not resurrect a deleted row: %s", got)
	}
	// 回滚的情形: 等待者在对方回滚后继续并更新成功
	s3 := e.NewSession()
	sexec(t, s1, "BEGIN")
	sexec(t, s1, "DELETE FROM acc WHERE id = 2")
	ch = execAsync(s3, "UPDATE acc SET bal = 5 WHERE id = 2")
	blocked(t, ch)
	sexec(t, s1, "ROLLBACK")
	done(t, ch)
	if got := rowsOf(mustExec(t, e, "SELECT bal FROM acc WHERE id = 2")); got != "5" {
		t.Fatalf("got %s", got)
	}
}

func TestInsertSameKeyBlocksUntilCommit(t *testing.T) {
	e := newEngine(t)
	setupAccounts(t, e)
	s1, s2 := e.NewSession(), e.NewSession()
	sexec(t, s1, "BEGIN")
	sexec(t, s1, "INSERT INTO acc VALUES (50,1,'x')")
	ch := execAsync(s2, "INSERT INTO acc VALUES (50,2,'y')")
	blocked(t, ch)
	sexec(t, s1, "ROLLBACK")
	if err := done(t, ch); err != nil { // 对方回滚后，第二个插入成功
		t.Fatal(err)
	}
	if got := rowsOf(mustExec(t, e, "SELECT bal FROM acc WHERE id = 50")); got != "2" {
		t.Fatalf("got %s", got)
	}
}

func TestSharedAndExclusiveLockingReads(t *testing.T) {
	e := newEngine(t)
	setupAccounts(t, e)
	s1, s2, s3 := e.NewSession(), e.NewSession(), e.NewSession()
	sexec(t, s1, "BEGIN")
	sexec(t, s1, "SELECT * FROM acc WHERE id = 1 FOR SHARE")
	sexec(t, s2, "BEGIN")
	sexec(t, s2, "SELECT * FROM acc WHERE id = 1 FOR SHARE") // S-S 相容
	ch := execAsync(s3, "UPDATE acc SET bal = 1 WHERE id = 1")
	blocked(t, ch)
	sexec(t, s1, "COMMIT")
	blocked(t, ch)
	sexec(t, s2, "COMMIT")
	done(t, ch)

	sexec(t, s1, "BEGIN")
	sexec(t, s1, "SELECT * FROM acc WHERE id = 2 FOR UPDATE")
	// 普通快照读不受影响
	if got := rowsOf(sexec(t, s3, "SELECT bal FROM acc WHERE id = 2")); got != "200" {
		t.Fatal(got)
	}
	ch = execAsync(s2, "SELECT * FROM acc WHERE id = 2 FOR SHARE")
	blocked(t, ch)
	sexec(t, s1, "ROLLBACK")
	done(t, ch)
}

func TestDeadlockVictimRolledBack(t *testing.T) {
	e := newEngine(t)
	setupAccounts(t, e)
	s1, s2 := e.NewSession(), e.NewSession()
	sexec(t, s1, "BEGIN")
	sexec(t, s2, "BEGIN")
	sexec(t, s1, "UPDATE acc SET bal = 1 WHERE id = 1")
	sexec(t, s2, "UPDATE acc SET bal = 2 WHERE id = 2")
	ch := execAsync(s1, "UPDATE acc SET bal = 11 WHERE id = 2") // s1 等 s2
	blocked(t, ch)
	_, err := s2.Exec("UPDATE acc SET bal = 22 WHERE id = 1") // 成环: s2 是牺牲者
	if !errors.Is(err, txn.ErrDeadlock) {
		t.Fatalf("expected deadlock, got %v", err)
	}
	if s2.InTxn() {
		t.Fatal("victim transaction must be rolled back")
	}
	if err := done(t, ch); err != nil { // s2 回滚释放锁，s1 继续
		t.Fatal(err)
	}
	sexec(t, s1, "COMMIT")
	if got := rowsOf(mustExec(t, e, "SELECT id, bal FROM acc WHERE id <= 2")); got != "1,1;2,11" {
		t.Fatalf("got %s", got)
	}
}

func TestDDLImplicitlyCommits(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE x (a INT PRIMARY KEY)")
	s := e.NewSession()
	sexec(t, s, "BEGIN")
	sexec(t, s, "INSERT INTO x VALUES (1)")
	sexec(t, s, "CREATE TABLE y (a INT PRIMARY KEY)") // 隐式提交 INSERT
	if s.InTxn() {
		t.Fatal("DDL must end the transaction")
	}
	sexec(t, s, "ROLLBACK") // 已无事务，空操作
	if r := mustExec(t, e, "SELECT * FROM x"); len(r.Rows) != 1 {
		t.Fatal("DDL must have committed the insert")
	}
	// 会话关闭时回滚未提交事务
	sexec(t, s, "BEGIN")
	sexec(t, s, "INSERT INTO x VALUES (2)")
	s.Close()
	if r := mustExec(t, e, "SELECT * FROM x"); len(r.Rows) != 1 {
		t.Fatal("Close must roll back")
	}
}

func TestConcurrentTransfersKeepInvariant(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE b (id INT PRIMARY KEY, bal INT)")
	const n = 8
	var vals []string
	for i := 0; i < n; i++ {
		vals = append(vals, fmt.Sprintf("(%d,1000)", i))
	}
	mustExec(t, e, "INSERT INTO b VALUES "+strings.Join(vals, ","))

	var wg sync.WaitGroup
	var deadlocks int64
	var mu sync.Mutex
	for g := 0; g < 6; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			s := e.NewSession()
			defer s.Close()
			for i := 0; i < 40; i++ {
				from, to := (g+i)%n, (g*3+i*5+1)%n
				if from == to {
					continue
				}
				for {
					_, err1 := s.Exec("BEGIN")
					_, err2 := s.Exec(fmt.Sprintf("UPDATE b SET bal = bal - 7 WHERE id = %d", from))
					var err3 error
					if err2 == nil {
						_, err3 = s.Exec(fmt.Sprintf("UPDATE b SET bal = bal + 7 WHERE id = %d", to))
					}
					if err1 != nil || err2 != nil || err3 != nil {
						err := errors.Join(err1, err2, err3)
						if errors.Is(err, txn.ErrDeadlock) {
							mu.Lock()
							deadlocks++
							mu.Unlock()
							continue // 事务已被回滚，重试
						}
						t.Errorf("unexpected: %v", err)
						s.Exec("ROLLBACK")
						return
					}
					if _, err := s.Exec("COMMIT"); err != nil {
						t.Error(err)
					}
					break
				}
			}
		}(g)
	}
	// 同时有读者做快照读: 任何时刻总额都必须是 8000
	stop := make(chan struct{})
	var rwg sync.WaitGroup
	rwg.Add(1)
	go func() {
		defer rwg.Done()
		s := e.NewSession()
		for {
			select {
			case <-stop:
				return
			default:
			}
			sum := 0
			r, err := s.Exec("SELECT bal FROM b")
			if err != nil {
				t.Error(err)
				return
			}
			for _, row := range r.Rows {
				sum += int(row[0].Int)
			}
			if sum != n*1000 {
				t.Errorf("snapshot read saw inconsistent total %d", sum)
				return
			}
		}
	}()
	wg.Wait()
	close(stop)
	rwg.Wait()
	r := mustExec(t, e, "SELECT bal FROM b")
	sum := 0
	for _, row := range r.Rows {
		sum += int(row[0].Int)
	}
	if sum != n*1000 {
		t.Fatalf("total=%d want %d", sum, n*1000)
	}
	if e.mgr.ActiveCount() != 0 || e.mgr.Undo.Len() != 0 {
		t.Fatalf("leftover txn state: active=%d undo=%d", e.mgr.ActiveCount(), e.mgr.Undo.Len())
	}
	t.Logf("deadlocks resolved: %d", deadlocks)
}

func TestTxnCrashRecoveryAndIDsRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "txncrash.db")
	e, err := OpenWithOptions(path, Options{PoolSize: 512})
	if err != nil {
		t.Fatal(err)
	}
	setupAccounts(t, e)
	s := e.NewSession()
	sexec(t, s, "BEGIN")
	sexec(t, s, "UPDATE acc SET bal = 1 WHERE id = 1")
	sexec(t, s, "DELETE FROM acc WHERE id = 2")
	sexec(t, s, "INSERT INTO acc VALUES (4,4,'d')")
	sexec(t, s, "COMMIT")
	lastID := e.mgr.NextID()
	e.crash()

	e, err = OpenWithOptions(path, Options{PoolSize: 512})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if got := rowsOf(mustExec(t, e, "SELECT id, bal FROM acc")); got != "1,1;3,300;4,4" {
		t.Fatalf("committed txn lost: %s", got)
	}
	if e.mgr.NextID() < lastID {
		t.Fatalf("txn ids must not be reused after restart: %d < %d", e.mgr.NextID(), lastID)
	}
	// 重启后旧版本可见，新事务可继续读写
	mustExec(t, e, "UPDATE acc SET bal = bal + 1 WHERE id = 1")
	if got := rowsOf(mustExec(t, e, "SELECT bal FROM acc WHERE id = 1")); got != "2" {
		t.Fatalf("got %s", got)
	}
}
