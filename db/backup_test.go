package db

import (
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"mysqlfake/backup"
)

func restoreOpen(t *testing.T, chain []string, binlogPath string, target RecoveryTarget) (*Engine, int) {
	t.Helper()
	dst := filepath.Join(t.TempDir(), "restored")
	n, err := RestoreBackups(dst, chain, binlogPath, target)
	if err != nil {
		t.Fatal(err)
	}
	e, err := OpenWithOptions(filepath.Join(dst, "data.db"), Options{CheckpointInterval: -1, StatsInterval: -1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { e.Close() })
	return e, n
}

func newBackupEngine(t *testing.T) *Engine {
	t.Helper()
	e, err := OpenWithOptions(filepath.Join(t.TempDir(), "src.db"), Options{CheckpointInterval: -1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { e.Close() })
	return e
}

// 备份期间持续有转账事务: 还原出来的库必须是自洽的 (总额不变、索引与表一致、能正常读写)。
func TestHotBackupUnderConcurrentTransfers(t *testing.T) {
	e := newBackupEngine(t)
	mustExec(t, e, "CREATE TABLE acc (id INT PRIMARY KEY, bal INT, tag INT)")
	const n = 2000
	bulkInsert(t, e, "acc", n, func(i int) string { return fmt.Sprintf("(%d,1000,%d)", i, i%10) })
	mustExec(t, e, "CREATE INDEX itag ON acc (tag)")
	e.SetBackupHook(func(stage string) {
		if stage == "page" {
			time.Sleep(5 * time.Millisecond) // 让备份与写入充分重叠
		}
	})
	stop := make(chan struct{})
	var ops atomic.Int64
	var wg sync.WaitGroup
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			s := e.NewSession()
			rng := rand.New(rand.NewSource(int64(g)))
			for {
				select {
				case <-stop:
					return
				default:
				}
				a, b := rng.Intn(n), rng.Intn(n)
				if a == b {
					continue
				}
				for {
					s.Exec("BEGIN")
					_, e1 := s.Exec(fmt.Sprintf("UPDATE acc SET bal = bal - 5, tag = %d WHERE id = %d", rng.Intn(10), a))
					_, e2 := s.Exec(fmt.Sprintf("UPDATE acc SET bal = bal + 5 WHERE id = %d", b))
					if e1 != nil || e2 != nil {
						s.Exec("ROLLBACK")
						continue
					}
					s.Exec("COMMIT")
					break
				}
				ops.Add(1)
			}
		}(g)
	}
	for ops.Load() < 100 {
		time.Sleep(time.Millisecond)
	}
	before := ops.Load()
	dir := filepath.Join(t.TempDir(), "full")
	m, err := e.Backup(dir, "")
	if err != nil {
		t.Fatal(err)
	}
	during := ops.Load() - before
	close(stop)
	wg.Wait()
	if during < 20 {
		t.Fatalf("only %d transfers overlapped with the backup", during)
	}
	t.Logf("backup copied %d pages while %d transfers ran; end_lsn=%d binlog=%d", m.PagesCopied, during, m.EndLSN, m.BinlogSeqEnd)

	r, applied := restoreOpen(t, []string{dir}, "", RecoveryTarget{})
	_ = applied
	sum := mustExec(t, r, "SELECT bal FROM acc")
	total := int64(0)
	for _, row := range sum.Rows {
		total += row[0].Int
	}
	if len(sum.Rows) != n || total != int64(n)*1000 {
		t.Fatalf("restored database is inconsistent: %d rows, total %d (want %d rows, %d)", len(sum.Rows), total, n, n*1000)
	}
	for tag := 0; tag < 10; tag++ { // 索引和表一致
		queryBoth(t, r, fmt.Sprintf("SELECT id FROM acc WHERE tag = %d", tag))
	}
	mustExec(t, r, "UPDATE acc SET bal = bal + 1 WHERE id = 1") // 可继续读写
}

func TestIncrementalChain(t *testing.T) {
	e := newBackupEngine(t)
	mustExec(t, e, "CREATE TABLE t (id INT PRIMARY KEY, v INT, s VARCHAR(40))")
	bulkInsert(t, e, "t", 3000, func(i int) string { return fmt.Sprintf("(%d,%d,'%s')", i, i, strings.Repeat("a", 30)) })
	state := func() string { return rowsOf(mustExec(t, e, "SELECT id, v FROM t ORDER BY id")) }

	d1 := filepath.Join(t.TempDir(), "b1")
	s1 := state()
	m1, err := e.Backup(d1, "")
	if err != nil {
		t.Fatal(err)
	}
	mustExec(t, e, "UPDATE t SET v = v + 1000 WHERE id < 100")
	mustExec(t, e, "DELETE FROM t WHERE id >= 2900")
	bulkInsert(t, e, "t", 3100, func(i int) string { return fmt.Sprintf("(%d,%d,'new')", 5000+i, i) })
	s2 := state()
	d2 := filepath.Join(t.TempDir(), "b2")
	m2, err := e.Backup(d2, d1)
	if err != nil {
		t.Fatal(err)
	}
	if m2.Kind != "incremental" || m2.ParentID != m1.ID || m2.PagesCopied >= m1.PagesCopied {
		t.Fatalf("incremental backup should copy fewer pages than the full one: full=%d inc=%d", m1.PagesCopied, m2.PagesCopied)
	}
	mustExec(t, e, "UPDATE t SET s = 'changed' WHERE id >= 5000 AND id < 5050")
	mustExec(t, e, "CREATE INDEX iv ON t (v)")
	s3 := state()
	d3 := filepath.Join(t.TempDir(), "b3")
	if _, err := e.Backup(d3, d2); err != nil {
		t.Fatal(err)
	}
	for i, tc := range []struct {
		chain []string
		want  string
	}{{[]string{d1}, s1}, {[]string{d1, d2}, s2}, {[]string{d1, d2, d3}, s3}} {
		r, _ := restoreOpen(t, tc.chain, "", RecoveryTarget{})
		if got := rowsOf(mustExec(t, r, "SELECT id, v FROM t ORDER BY id")); got != tc.want {
			t.Fatalf("chain %d restored the wrong state", i)
		}
	}
	r, _ := restoreOpen(t, []string{d1, d2, d3}, "", RecoveryTarget{})
	if got := rowsOf(mustExec(t, r, "SELECT s FROM t WHERE id = 5010")); got != "changed" {
		t.Fatalf("got %s", got)
	}
	cols, idx, _ := r.Describe("t")
	if len(cols) != 3 || len(idx) != 1 || idx[0] != "iv" {
		t.Fatalf("schema change in the last increment not restored: %v %v", cols, idx)
	}
	queryBoth(t, r, "SELECT id FROM t WHERE v = 12")
}

func TestPointInTimeRecovery(t *testing.T) {
	e := newBackupEngine(t)
	mustExec(t, e, "CREATE TABLE t (id INT PRIMARY KEY, v INT)")
	bulkInsert(t, e, "t", 100, func(i int) string { return fmt.Sprintf("(%d,0)", i) })
	dir := filepath.Join(t.TempDir(), "base")
	if _, err := e.Backup(dir, ""); err != nil {
		t.Fatal(err)
	}
	// 备份之后的一系列事务 (含 DDL)，每一步结束后记录期望状态
	type step struct {
		sql   string
		state string
	}
	var steps []step
	do := func(q string) {
		time.Sleep(2 * time.Millisecond) // 让每个事务的提交时间戳不同
		mustExec(t, e, q)
		steps = append(steps, step{q, rowsOf(mustExec(t, e, "SELECT * FROM t ORDER BY id"))})
	}
	do("INSERT INTO t VALUES (1000, 5)")
	do("UPDATE t SET v = 1 WHERE id < 10")
	do("ALTER TABLE t ADD COLUMN w INT DEFAULT 3")
	do("DELETE FROM t WHERE id >= 90 AND id < 100")
	do("CREATE INDEX iv ON t (v)")
	do("UPDATE t SET w = 8 WHERE id = 1000")
	do("INSERT INTO t VALUES (2000, 9, 9)")
	recs, err := e.Binlog().ReadFrom(1)
	if err != nil {
		t.Fatal(err)
	}
	binPath := e.path + ".binlog"
	// 找到每一步对应的 Binlog 记录 (最后 len(steps) 条)
	tail := recs[len(recs)-len(steps):]
	for k := range steps {
		// 按序号还原到第 k 步之后
		r, applied := restoreOpen(t, []string{dir}, binPath, RecoveryTarget{Seq: tail[k].Seq})
		if got := rowsOf(mustExec(t, r, "SELECT * FROM t ORDER BY id")); got != steps[k].state {
			t.Fatalf("PITR to seq %d (%s): wrong state\n got  %.120s\n want %.120s", tail[k].Seq, steps[k].sql, got, steps[k].state)
		}
		if applied != k+1 {
			t.Fatalf("applied %d txns, want %d", applied, k+1)
		}
		// 按时间点还原: 目标落在第 k 个事务与第 k+1 个事务之间
		if k+1 < len(tail) {
			mid := time.Unix(0, tail[k].TimeNano+(tail[k+1].TimeNano-tail[k].TimeNano)/2)
			r2, _ := restoreOpen(t, []string{dir}, binPath, RecoveryTarget{Time: mid})
			if got := rowsOf(mustExec(t, r2, "SELECT * FROM t ORDER BY id")); got != steps[k].state {
				t.Fatalf("PITR to time between txn %d and %d: wrong state", k, k+1)
			}
		}
	}
	// 目标早于备份完成时间 -> 拒绝
	_, err = RestoreBackups(filepath.Join(t.TempDir(), "x"), []string{dir}, binPath, RecoveryTarget{Time: time.Now().Add(-time.Hour)})
	if !errors.Is(err, backup.ErrTargetTime) {
		t.Fatalf("expected ErrTargetTime, got %v", err)
	}
	// 目标远在日志之后 -> 重放全部
	r, applied := restoreOpen(t, []string{dir}, binPath, RecoveryTarget{Time: time.Now().Add(time.Hour)})
	if applied != len(steps) || rowsOf(mustExec(t, r, "SELECT * FROM t ORDER BY id")) != steps[len(steps)-1].state {
		t.Fatalf("full replay applied %d", applied)
	}
	queryBoth(t, r, "SELECT id FROM t WHERE v = 1")
}

func TestBackupHoldsWALAndValidatesChains(t *testing.T) {
	e := newBackupEngine(t)
	mustExec(t, e, "CREATE TABLE t (id INT PRIMARY KEY, v VARCHAR(100))")
	bulkInsert(t, e, "t", 3000, func(i int) string { return fmt.Sprintf("(%d,'%s')", i, strings.Repeat("x", 80)) })
	var shrank atomic.Bool
	hooked := false
	e.SetBackupHook(func(stage string) {
		if stage == "page" && !hooked {
			hooked = true
			size := e.log.Size()
			mustExecNoFail(e, "UPDATE t SET v = 'y' WHERE id < 50")
			e.pool.FuzzyCheckpoint() // 备份期间的 Checkpoint 不得截断 WAL
			if e.log.Size() < size {
				shrank.Store(true)
			}
		}
	})
	dir := filepath.Join(t.TempDir(), "b")
	r, err := e.Exec("BACKUP TO '" + dir + "'") // 通过 SQL 触发
	if err != nil || len(r.Rows) != 1 || r.Rows[0][1].Str != "full" {
		t.Fatalf("BACKUP statement: %v %v", r, err)
	}
	if shrank.Load() {
		t.Fatal("WAL was truncated while a backup was running")
	}
	e.SetBackupHook(nil)
	before := e.log.Size()
	if _, err := e.pool.FuzzyCheckpoint(); err != nil {
		t.Fatal(err)
	}
	if e.log.Size() >= before {
		t.Fatal("WAL truncation must resume after the backup finishes")
	}
	// 增量 SQL
	mustExec(t, e, "UPDATE t SET v = 'z' WHERE id > 2900")
	dir2 := filepath.Join(t.TempDir(), "b2")
	if r, err := e.Exec("BACKUP TO '" + dir2 + "' INCREMENTAL FROM '" + dir + "'"); err != nil || r.Rows[0][1].Str != "incremental" {
		t.Fatalf("incremental statement: %v %v", r, err)
	}
	// 各种错误
	if _, err := e.Backup(dir, ""); err == nil {
		t.Fatal("backing up into an existing backup must fail")
	}
	if _, err := e.Backup(filepath.Join(t.TempDir(), "n"), filepath.Join(t.TempDir(), "missing")); err == nil {
		t.Fatal("missing parent must fail")
	}
	bad := []struct {
		chain []string
		why   string
	}{
		{[]string{dir2}, "first must be full"},
		{[]string{dir, dir}, "incremental expected"},
		{[]string{dir2, dir}, "wrong order"},
		{nil, "empty"},
	}
	for _, b := range bad {
		if _, err := backup.Restore(filepath.Join(t.TempDir(), "r"), b.chain); !errors.Is(err, backup.ErrBadChain) {
			t.Errorf("%s: expected ErrBadChain, got %v", b.why, err)
		}
	}
	dst := filepath.Join(t.TempDir(), "again")
	if _, err := backup.Restore(dst, []string{dir}); err != nil {
		t.Fatal(err)
	}
	if _, err := backup.Restore(dst, []string{dir}); err == nil {
		t.Fatal("restoring over an existing database must fail")
	}
	// 已被损坏的页: 备份必须报错而不是静默拷贝坏数据
	e2, err := OpenWithOptions(filepath.Join(t.TempDir(), "c.db"), Options{CheckpointInterval: -1})
	if err != nil {
		t.Fatal(err)
	}
	mustExec(t, e2, "CREATE TABLE t (id INT PRIMARY KEY)")
	bulkInsert(t, e2, "t", 500, func(i int) string { return fmt.Sprintf("(%d)", i) })
	e2.pool.Checkpoint()
	f, _ := os.OpenFile(e2.path, os.O_RDWR, 0)
	f.WriteAt([]byte{0xDE, 0xAD, 0xBE, 0xEF}, 2*4096+500)
	f.Close()
	if _, err := e2.Backup(filepath.Join(t.TempDir(), "bad"), ""); err == nil || !strings.Contains(err.Error(), "checksum") {
		t.Fatalf("expected a checksum error, got %v", err)
	}
	e2.crash()
}

func mustExecNoFail(e *Engine, q string) { e.NewSession().Exec(q) } // 另开会话: 外层 Engine.Exec 持有默认会话的锁
