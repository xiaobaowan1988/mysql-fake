package db

import (
	"os"
	"path/filepath"
	"testing"
)

// Phase 10 (崩溃时回滚未提交事务) 的验收测试。
//
// 标记了 pending(...) 的测试对应尚未实现的步骤: 默认跳过，设置 MYFAKE_PENDING=1 可以运行它们，
// 用来确认它们现在确实失败。每完成一个步骤，就删掉对应测试里的 pending 调用。

func pending(t *testing.T, step string) {
	t.Helper()
	if os.Getenv("MYFAKE_PENDING") == "" {
		t.Skipf("Phase %s 尚未实现 (MYFAKE_PENDING=1 可强制运行)", step)
	}
}

func openRecoveryEngine(t *testing.T, path string) *Engine {
	t.Helper()
	// 关掉后台检查点和统计线程: 让日志/页的状态完全由测试控制
	e, err := OpenWithOptions(path, Options{PoolSize: 512, CheckpointInterval: -1, StatsInterval: -1})
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func queryRows(t *testing.T, e *Engine, q string) string {
	t.Helper()
	return rowsOf(mustExec(t, e, q))
}

// 场景: 失败者先改了数据 (没有提交)，之后另一个事务提交——Commit 会把整个 WAL 缓冲 (含失败者的记录)
// 一起 fsync，所以崩溃后日志里有失败者的修改。恢复必须撤销它们，同时保留赢家的提交。
func TestUndoRecovery_LoserRolledBack(t *testing.T) {
	pending(t, "10.2")
	path := filepath.Join(t.TempDir(), "undo1.db")
	e := openRecoveryEngine(t, path)
	setupAccounts(t, e) // (1,100,'a') (2,200,'b') (3,300,'c')，已提交

	loser, winner := e.NewSession(), e.NewSession()
	sexec(t, loser, "BEGIN")
	sexec(t, loser, "UPDATE acc SET bal = 0 WHERE id = 1")    // UndoUpdate
	sexec(t, loser, "DELETE FROM acc WHERE id = 3")           // UndoDelete
	sexec(t, loser, "INSERT INTO acc VALUES (4,400,'d')")     // UndoInsert
	sexec(t, winner, "UPDATE acc SET bal = 999 WHERE id = 2") // 自动提交: fsync 日志
	// 失败者的快照在这次读取时才建立，所以能看到赢家已提交的 999，以及它自己的三处修改
	if got := rowsOf(sexec(t, loser, "SELECT id, bal FROM acc")); got != "1,0;2,999;4,400" {
		t.Fatalf("test setup: loser should see its own changes, got %s", got)
	}
	e.crash()

	e = openRecoveryEngine(t, path)
	defer e.Close()
	if got := queryRows(t, e, "SELECT id, bal FROM acc"); got != "1,100;2,999;3,300" {
		t.Fatalf("after recovery: got %s, want loser undone and winner kept: 1,100;2,999;3,300", got)
	}
	// 恢复后的数据库必须能继续正常工作: 失败者持有的锁不存在了，被撤销的插入也不会造成主键冲突
	mustExec(t, e, "UPDATE acc SET bal = bal + 1 WHERE id = 1")
	mustExec(t, e, "INSERT INTO acc VALUES (4,1,'x')")
	if got := queryRows(t, e, "SELECT id, bal FROM acc"); got != "1,101;2,999;3,300;4,1" {
		t.Fatalf("database unusable after recovery: %s", got)
	}
}

// 对照组 (现在就应该通过，之后也必须保持通过): 显式 ROLLBACK 的事务，其回滚过程中对 B+ 树的修改
// 同样记了日志，重做历史就能得到回滚后的状态，不需要再撤销。
func TestUndoRecovery_ExplicitRollbackStaysRolledBack(t *testing.T) {
	path := filepath.Join(t.TempDir(), "undo2.db")
	e := openRecoveryEngine(t, path)
	setupAccounts(t, e)

	s, winner := e.NewSession(), e.NewSession()
	sexec(t, s, "BEGIN")
	sexec(t, s, "UPDATE acc SET bal = 0 WHERE id = 1")
	sexec(t, s, "INSERT INTO acc VALUES (4,400,'d')")
	sexec(t, s, "ROLLBACK")
	sexec(t, winner, "UPDATE acc SET bal = 999 WHERE id = 2")
	e.crash()

	e = openRecoveryEngine(t, path)
	defer e.Close()
	if got := queryRows(t, e, "SELECT id, bal FROM acc"); got != "1,100;2,999;3,300" {
		t.Fatalf("got %s", got)
	}
}

// 二级索引: 失败者的 UPDATE 在索引树里新增了一项 (索引项只增不删)，撤销时必须把这一项删掉。
func TestUndoRecovery_SecondaryIndexEntriesUndone(t *testing.T) {
	pending(t, "10.2")
	path := filepath.Join(t.TempDir(), "undo3.db")
	e := openRecoveryEngine(t, path)
	setupAccounts(t, e)
	mustExec(t, e, "CREATE INDEX idx_bal ON acc(bal)")

	loser, winner := e.NewSession(), e.NewSession()
	sexec(t, loser, "BEGIN")
	sexec(t, loser, "UPDATE acc SET bal = 5 WHERE id = 1") // 索引新增 (5,1)
	sexec(t, winner, "UPDATE acc SET name = 'z' WHERE id = 2")
	e.crash()

	e = openRecoveryEngine(t, path)
	defer e.Close()
	if got := queryRows(t, e, "SELECT id, bal FROM acc"); got != "1,100;2,200;3,300" {
		t.Fatalf("row data: got %s", got)
	}
	ti, err := e.cat.GetTable("acc")
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for it := ti.Indexes[0].Tree.Scan(nil, nil); it.Next(); {
		n++
	}
	if n != 3 {
		t.Fatalf("secondary index has %d entries, want 3 (the loser's new entry must be removed)", n)
	}
}

// 检查点把失败者修改过的脏页刷盘，并丢弃日志前缀: 如果 undo 信息随之被丢掉，就再也撤销不了了。
func TestUndoRecovery_SurvivesFuzzyCheckpoint(t *testing.T) {
	pending(t, "10.3")
	path := filepath.Join(t.TempDir(), "undo4.db")
	e := openRecoveryEngine(t, path)
	setupAccounts(t, e)

	loser, winner := e.NewSession(), e.NewSession()
	sexec(t, loser, "BEGIN")
	sexec(t, loser, "UPDATE acc SET bal = 0 WHERE id = 1")
	if _, err := e.pool.FuzzyCheckpoint(); err != nil { // 含失败者修改的页落盘，日志前缀被截断
		t.Fatal(err)
	}
	sexec(t, winner, "UPDATE acc SET bal = 999 WHERE id = 2")
	e.crash()

	e = openRecoveryEngine(t, path)
	defer e.Close()
	if got := queryRows(t, e, "SELECT id, bal FROM acc"); got != "1,100;2,999;3,300" {
		t.Fatalf("got %s, want 1,100;2,999;3,300", got)
	}
}

// 优雅关闭时还有打开的事务: Close 会做 Checkpoint (刷全部脏页并截断日志)，同样不能丢掉 undo 信息。
func TestUndoRecovery_GracefulCloseWithOpenTxn(t *testing.T) {
	pending(t, "10.3")
	path := filepath.Join(t.TempDir(), "undo5.db")
	e := openRecoveryEngine(t, path)
	setupAccounts(t, e)

	loser := e.NewSession()
	sexec(t, loser, "BEGIN")
	sexec(t, loser, "UPDATE acc SET bal = 0 WHERE id = 1")
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	e = openRecoveryEngine(t, path)
	defer e.Close()
	if got := queryRows(t, e, "SELECT id, bal FROM acc"); got != "1,100;2,200;3,300" {
		t.Fatalf("got %s, want the open transaction's change undone", got)
	}
}
