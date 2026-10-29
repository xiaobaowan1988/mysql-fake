package db

import (
	"path/filepath"
	"testing"
)

func TestBinlogReplayReproducesState(t *testing.T) {
	a := newEngine(t)
	setupAccounts(t, a)
	mustExec(t, a, "UPDATE acc SET bal = bal + 1 WHERE id <= 2")
	mustExec(t, a, "DELETE FROM acc WHERE id = 3")
	s := a.NewSession()
	sexec(t, s, "BEGIN")
	sexec(t, s, "INSERT INTO acc VALUES (4,4,'d')")
	sexec(t, s, "DELETE FROM acc WHERE id = 1")
	sexec(t, s, "ROLLBACK") // 回滚的事务不产生 Binlog
	if _, err := a.Exec("INSERT INTO acc VALUES (1,1,'dup')"); err == nil {
		t.Fatal("expected dup")
	}
	mustExec(t, a, "INSERT INTO acc VALUES (3,33,'again')") // 在删除标记上重新插入

	recs, err := a.Binlog().ReadFrom(1)
	if err != nil {
		t.Fatal(err)
	}
	// create table, insert, update, delete, insert
	if len(recs) != 5 {
		t.Fatalf("binlog has %d txns", len(recs))
	}
	for i, r := range recs {
		if r.Seq != uint64(i+1) || r.TimeNano == 0 {
			t.Fatalf("bad record %d: %+v", i, r)
		}
	}

	b, err := Open(filepath.Join(t.TempDir(), "replica.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	for round := 0; round < 2; round++ { // 第二轮验证幂等
		for _, r := range recs {
			if err := b.ApplyBinlog(r); err != nil {
				t.Fatal(err)
			}
		}
		want := rowsOf(mustExec(t, a, "SELECT * FROM acc"))
		if got := rowsOf(mustExec(t, b, "SELECT * FROM acc")); got != want {
			t.Fatalf("round %d replica differs:\n got  %s\n want %s", round, got, want)
		}
	}
	// 应用产生的事务写入本地 Binlog
	if b.Binlog().LastSeq() == 0 {
		t.Fatal("applied txns must be binlogged")
	}
}

func TestWriteGuardAndReplicatorFailure(t *testing.T) {
	e := newEngine(t)
	setupAccounts(t, e)
	e.SetWriteGuard(func() error { return errTest })
	if _, err := e.Exec("INSERT INTO acc VALUES (9,9,'x')"); err != errTest {
		t.Fatalf("guard not applied: %v", err)
	}
	if _, err := e.Exec("SELECT * FROM acc FOR UPDATE"); err != errTest {
		t.Fatalf("locking read must be guarded: %v", err)
	}
	if _, err := e.Exec("CREATE TABLE z (a INT PRIMARY KEY)"); err != errTest {
		t.Fatalf("ddl must be guarded: %v", err)
	}
	if r := mustExec(t, e, "SELECT * FROM acc"); len(r.Rows) != 3 { // 普通读不受限
		t.Fatal("reads must be allowed")
	}
	e.SetWriteGuard(nil)

	// 复制失败 -> 事务回滚，且 DDL 的本地建表被撤销
	e.SetReplicator(func(*binlogTxn) error { return errTest })
	if _, err := e.Exec("INSERT INTO acc VALUES (9,9,'x')"); err != errTest {
		t.Fatalf("got %v", err)
	}
	if _, err := e.Exec("CREATE TABLE z (a INT PRIMARY KEY)"); err != errTest {
		t.Fatalf("got %v", err)
	}
	e.SetReplicator(nil)
	if r := mustExec(t, e, "SELECT * FROM acc WHERE id = 9"); len(r.Rows) != 0 {
		t.Fatal("failed replication must roll back")
	}
	mustExec(t, e, "CREATE TABLE z (a INT PRIMARY KEY)") // 之前的失败没有留下残留
	n := e.Binlog().LastSeq()
	if recs, _ := e.Binlog().ReadFrom(n); len(recs) != 1 {
		t.Fatal("unexpected binlog tail")
	}
}
