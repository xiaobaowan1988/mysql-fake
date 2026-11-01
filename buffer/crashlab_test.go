package buffer

import (
	"os"
	"testing"

	"mysqlfake/btree"
	"mysqlfake/storage"
	"mysqlfake/wal"
)

// 崩溃实验室: 把 WAL 的核心规则写成可重复的实验。每个实验: 构造场景 -> crash() 模拟断电 -> 重新打开 (自动恢复) -> 断言。

func keysAfterRecovery(t *testing.T, path string, root storage.PageID) int {
	e := openEnv(t, path, 64)
	defer e.d.Close()
	return countTree(t, btree.Open(e.pool, root))
}

// 先建好一棵已落盘的空树 (Checkpoint)，这样后面观察到的变化都来自实验本身。
func baseline(t *testing.T) (*env, *btree.Tree, storage.PageID) {
	e := newEnv(t, 64)
	tr, _ := btree.Create(e.pool)
	if err := e.pool.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	return e, tr, tr.Root()
}

func TestLab1_CommittedSurvivesCrash(t *testing.T) {
	e, tr, root := baseline(t)
	fillTree(t, tr, 0, 50)
	e.pool.Commit(1) // 日志 fsync
	if e.pool.DirtyCount() == 0 {
		t.Fatal("test setup: expected dirty pages that were never flushed")
	}
	e.crash()
	if n := keysAfterRecovery(t, e.path, root); n != 50 {
		t.Fatalf("committed data lost after crash: %d keys, want 50", n)
	}
}

func TestLab2_UncommittedIsLost(t *testing.T) {
	e, tr, root := baseline(t)
	fillTree(t, tr, 0, 50) // 没有 Commit: 日志只在内存缓冲区里
	e.crash()
	if n := keysAfterRecovery(t, e.path, root); n != 0 {
		t.Fatalf("uncommitted data whose log never reached disk survived: %d keys", n)
	}
}

func TestLab3_LostCommitRecordButUpdatesReplayed(t *testing.T) {
	e, tr, root := baseline(t)
	fillTree(t, tr, 0, 50)
	e.pool.Commit(1)
	e.crash()
	// 把日志文件尾巴砍掉 10 字节: 最后一条 (提交记录, 33 字节) 变成残缺记录
	st, _ := os.Stat(e.path + ".wal")
	os.Truncate(e.path+".wal", st.Size()-10)
	recs, _ := wal.ReadFile(e.path + ".wal")
	last := recs[len(recs)-1]
	if last.Type != wal.RecUpdate {
		t.Fatalf("test setup: the torn commit record should have been dropped, last record type=%d", last.Type)
	}
	// 记录已知限制: wal.Recover (Redo) 只重放 Update、不看提交记录，所以没有提交记录的修改也会留下 (PLAN.md Phase 3 已知限制)；
	// 引擎层靠 Undo 记录在 Redo 之后撤销这样的失败者 (Phase 10，见 db/undo_recovery_test.go)，缓冲池/WAL 这一层不知道事务。
	if n := keysAfterRecovery(t, e.path, root); n != 50 {
		t.Fatalf("documented behaviour changed: got %d keys, want 50", n)
	}
}

func TestLab4_PageAheadOfLog(t *testing.T) {
	e, tr, root := baseline(t)
	fillTree(t, tr, 0, 1) // 日志在内存缓冲区, 没有 Flush
	// 违反 WAL 规则: 绕过缓冲池, 直接把脏页写到磁盘, 而对应的日志没有落盘
	var p storage.Page
	e.pool.ReadPage(root, &p)
	if err := e.d.WritePage(&p); err != nil {
		t.Fatal(err)
	}
	if p.LSN() <= e.lm.FlushedLSN() {
		t.Fatalf("test setup: the page must be ahead of the durable log (page LSN %d, flushed %d)", p.LSN(), e.lm.FlushedLSN())
	}
	e.crash()
	// 违反 WAL 的后果: 一条没有日志可查的修改留在了数据文件里
	if n := keysAfterRecovery(t, e.path, root); n != 1 {
		t.Fatalf("expected the orphan modification to remain, got %d keys", n)
	}
}

func TestLab5_ReplayIsIdempotentAndSkipsByLSN(t *testing.T) {
	e, tr, root := baseline(t)
	fillTree(t, tr, 0, 50)
	e.pool.Commit(1)
	e.crash()
	saved, _ := os.ReadFile(e.path + ".wal")

	// 第一次恢复: 重放
	d, _ := storage.OpenDiskManager(e.path)
	lm, _ := wal.Open(e.path + ".wal")
	n1, _ := wal.Recover(d, lm)
	lm.Close()
	d.Close()
	if n1 == 0 {
		t.Fatal("first recovery should replay records")
	}

	// 假装"恢复完成前又断电": 日志还在 (还原成旧内容), 页已经是最新的
	os.WriteFile(e.path+".wal", saved, 0o644)
	d, _ = storage.OpenDiskManager(e.path)
	lm, _ = wal.Open(e.path + ".wal")
	n2, _ := wal.Recover(d, lm)
	lm.Close()
	d.Close()
	if n2 != 0 {
		t.Fatalf("second recovery replayed %d records; page LSNs should make it skip all of them", n2)
	}
	if n := keysAfterRecovery(t, e.path, root); n != 50 {
		t.Fatalf("repeated recovery damaged the data: %d keys", n)
	}
}
