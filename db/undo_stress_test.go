package db

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"mysqlfake/btree"
	"mysqlfake/buffer"
	"mysqlfake/txn"
	"mysqlfake/wal"
)

// 撤销过程中崩溃: 失败者仍然没有 Abort 记录，下一次恢复会从头再撤销一遍。撤销都是"绝对值"写入，
// 所以无论上一次做到了哪一步 (也无论那部分是否已经落盘)，结果都必须相同。
func TestUndoRecovery_CrashDuringUndoIsRestartable(t *testing.T) {
	errSimulatedCrash := errors.New("simulated crash during undo")
	defer func() { undoStepHook = nil }()

	// 同一个键被失败者改了两次: 撤销必须倒序，最终写入来自最早的 undo 记录
	scenario := func(path string) *Engine {
		e := openRecoveryEngine(t, path)
		setupAccounts(t, e)
		loser, winner := e.NewSession(), e.NewSession()
		sexec(t, loser, "BEGIN")
		sexec(t, loser, "UPDATE acc SET bal = 0 WHERE id = 1")
		sexec(t, loser, "UPDATE acc SET bal = -1 WHERE id = 1")
		sexec(t, loser, "DELETE FROM acc WHERE id = 3")
		sexec(t, loser, "INSERT INTO acc VALUES (4,400,'d')")
		sexec(t, winner, "UPDATE acc SET bal = 999 WHERE id = 2")
		return e
	}
	const undoRecords = 4

	for _, durable := range []bool{false, true} {
		for stopAt := 0; stopAt < undoRecords; stopAt++ {
			name := fmt.Sprintf("stopAt=%d durable=%v", stopAt, durable)
			path := filepath.Join(t.TempDir(), "restart.db")
			scenario(path).crash()

			// 第一次恢复在撤销了 stopAt 条之后"崩溃"
			undoStepHook = func(step int, pool *buffer.Pool, lm *wal.LogManager) error {
				if step != stopAt {
					return nil
				}
				if durable { // 已经做完的那部分撤销 (页和日志) 已经落盘
					if err := lm.FlushAll(); err != nil {
						return err
					}
					if err := pool.FlushAll(); err != nil {
						return err
					}
				}
				return errSimulatedCrash
			}
			if _, err := OpenWithOptions(path, Options{PoolSize: 512, CheckpointInterval: -1, StatsInterval: -1}); !errors.Is(err, errSimulatedCrash) {
				t.Fatalf("%s: first recovery should have been interrupted, got %v", name, err)
			}
			undoStepHook = nil

			// 第二次恢复: 从头再撤销，结果正确
			e := openRecoveryEngine(t, path)
			if st := e.RecoveryStats(); st.Losers != 1 || st.Undone != undoRecords {
				t.Fatalf("%s: second recovery stats %+v, want it to undo all %d records again", name, st, undoRecords)
			}
			if got := queryRows(t, e, "SELECT id, bal FROM acc"); got != "1,100;2,999;3,300" {
				t.Fatalf("%s: got %s, want 1,100;2,999;3,300", name, got)
			}
			e.crash() // 之后再崩溃一次也没有问题: 第二次恢复已经写了 Abort 并做了检查点

			e = openRecoveryEngine(t, path)
			if st := e.RecoveryStats(); st.Losers != 0 {
				t.Fatalf("%s: a finished recovery must not leave losers behind: %+v", name, st)
			}
			if got := queryRows(t, e, "SELECT id, bal FROM acc"); got != "1,100;2,999;3,300" {
				t.Fatalf("%s: after the third open: got %s", name, got)
			}
			e.Close()
		}
	}
}

// 并发 + 随机崩溃: 每个工作线程有自己的两个账户，先提交若干次转账，最后留下一个没有提交的事务
// (部分更新、插入一批会让叶子分裂的新行、删除一行)。同时后台不断做 Fuzzy/Sharp Checkpoint，
// 小缓冲池会让脏页被换出。崩溃重启后，每一行必须**精确等于**已提交事务的结果。
//
// 各线程的账户互不相交，所以没有锁等待和死锁，崩溃时刻 (所有线程都停在事务中间) 是确定的；
// 但所有线程的行挤在同一批叶子页里，失败者的插入会让这些页分裂 —— 这正是物理撤销会出错、逻辑撤销不会出错的场景。
func TestUndoRecovery_RandomCrashKeepsCommittedExactly(t *testing.T) {
	rounds := 30
	if testing.Short() {
		rounds = 6
	}
	var losers, undone int
	for round := 0; round < rounds; round++ {
		st := runCrashRound(t, round)
		losers += st.Losers
		undone += st.Undone
		if t.Failed() {
			t.Fatalf("failed in round %d", round)
		}
	}
	if losers == 0 || undone == 0 {
		t.Fatalf("the test never exercised the undo phase (losers=%d undone=%d)", losers, undone)
	}
	t.Logf("%d rounds: %d losers, %d undo records applied", rounds, losers, undone)
}

// checkTree 检查 B+ 树的结构: 扫描必须终止、键严格递增、值是合法的行版本，并且扫描到的每个键都能被点查找到
// (点查走的是内部节点的分隔键，扫描走的是叶子链，两者不一致说明树的结构已经损坏)。
func checkTree(tr *btree.Tree) error {
	var prev []byte
	n := 0
	for it := tr.Scan(nil, nil); it.Next(); {
		if n++; n > 100000 {
			return errors.New("scan does not terminate")
		}
		k := it.Key()
		if prev != nil && bytes.Compare(prev, k) >= 0 {
			return fmt.Errorf("keys out of order at entry %d: %x then %x", n, prev, k)
		}
		prev = append(prev[:0], k...)
		if len(it.Value()) < txn.VersionHeaderSize {
			return fmt.Errorf("entry %d (key %x) has a %d-byte value, not a row version", n, k, len(it.Value()))
		}
		if _, ok, err := tr.Get(k); err != nil || !ok {
			return fmt.Errorf("key %x is on the leaf chain but Get cannot find it (found=%v err=%v): separators and leaves disagree", k, ok, err)
		}
	}
	return nil
}

func runCrashRound(t *testing.T, round int) RecoveryStats {
	t.Helper()
	const workers = 4
	rng := rand.New(rand.NewSource(int64(round)))
	path := filepath.Join(t.TempDir(), "stress.db")
	poolSize := []int{64, 512}[rng.Intn(2)]
	open := func() *Engine {
		e, err := OpenWithOptions(path, Options{PoolSize: poolSize, CheckpointInterval: -1, StatsInterval: -1})
		if err != nil {
			t.Fatalf("round %d: open: %v", round, err)
		}
		return e
	}
	e := open()
	mustExec(t, e, "CREATE TABLE b (id INT PRIMARY KEY, bal INT)")
	var acc []string
	for i := 0; i < 2*workers; i++ {
		acc = append(acc, fmt.Sprintf("(%d,1000)", i))
	}
	mustExec(t, e, "INSERT INTO b VALUES "+strings.Join(acc, ",")+",(999,0)")
	const fillers = 600
	bulkInsert(t, e, "b", fillers, func(i int) string { return fmt.Sprintf("(%d,1)", 1000+i) })

	bal := make([][2]int, workers) // 每个工作线程两个账户已提交的余额
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		bal[w] = [2]int{1000, 1000}
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(int64(round*100 + w)))
			s := e.NewSession()
			ids := [2]int{w, w + workers}
			run := func(q string) bool {
				if _, err := s.Exec(q); err != nil {
					t.Errorf("round %d worker %d: %s: %v", round, w, q, err)
					return false
				}
				return true
			}
			for i, n := 0, r.Intn(9); i < n; i++ { // 已提交的转账
				from, d := r.Intn(2), 1+r.Intn(50)
				to := 1 - from
				if run("BEGIN") &&
					run(fmt.Sprintf("UPDATE b SET bal = bal - %d WHERE id = %d", d, ids[from])) &&
					run(fmt.Sprintf("UPDATE b SET bal = bal + %d WHERE id = %d", d, ids[to])) &&
					run("COMMIT") {
					bal[w][from] -= d
					bal[w][to] += d
				} else {
					return
				}
			}
			// 最后一个事务不提交: 线程就停在这里 (会话保持打开)，等着被崩溃
			if !run("BEGIN") {
				return
			}
			if r.Intn(3) != 0 {
				run(fmt.Sprintf("UPDATE b SET bal = bal - %d WHERE id = %d", 1+r.Intn(50), ids[r.Intn(2)]))
			}
			if r.Intn(2) == 0 {
				run(fmt.Sprintf("UPDATE b SET bal = bal + %d WHERE id = %d", 1+r.Intn(50), ids[r.Intn(2)]))
			}
			for j, k := 0, r.Intn(41); j < k; j++ { // 键落在账户所在的叶子里，足以让它分裂
				run(fmt.Sprintf("INSERT INTO b VALUES (%d,7)", 10+w*200+j))
			}
			if r.Intn(2) == 0 {
				run(fmt.Sprintf("DELETE FROM b WHERE id = %d", 1000+w))
			}
		}(w)
	}
	stopCk := make(chan struct{})
	var ckWG sync.WaitGroup
	ckWG.Add(1)
	go func() {
		defer ckWG.Done()
		cr := rand.New(rand.NewSource(int64(round) + 7))
		for {
			select {
			case <-stopCk:
				return
			default:
			}
			var err error
			if cr.Intn(4) == 0 {
				err = e.pool.Checkpoint()
			} else {
				_, err = e.pool.FuzzyCheckpoint()
			}
			if err != nil {
				t.Errorf("round %d: checkpoint: %v", round, err)
				return
			}
			time.Sleep(time.Duration(cr.Intn(3000)) * time.Microsecond)
		}
	}()
	wg.Wait()
	close(stopCk)
	ckWG.Wait()

	// 最后一次提交把整个日志缓冲 (含失败者的全部记录) 一起落盘，崩溃时日志是完整的。
	// 必须如此: 一次页修改由多条日志记录组成，一次分裂涉及多个页，日志刷盘点可能落在它们中间；
	// 如果崩溃时日志尾部恰好停在中间，重做出来的页/树就是半成品 (Phase 3 已知限制，见 PLAN.md Phase 10)，
	// 与 Undo 无关。这里要检验的是 Undo: 日志完整时，每一行都必须精确等于已提交事务的结果。
	sexec(t, e.NewSession(), "UPDATE b SET bal = bal + 1 WHERE id = 999")
	const sentinel = 1
	e.crash()

	want := map[int]int{999: sentinel}
	for w := 0; w < workers; w++ {
		want[w], want[w+workers] = bal[w][0], bal[w][1]
	}
	for i := 0; i < fillers; i++ {
		want[1000+i] = 1
	}
	verify := func(e *Engine, label string) {
		t.Helper()
		ti, err := e.cat.GetTable("b")
		if err != nil {
			t.Fatal(err)
		}
		if err := checkTree(ti.Tree); err != nil {
			t.Errorf("round %d (%s, pool=%d): B+ tree is structurally broken after recovery: %v (recovery=%+v)", round, label, poolSize, err, e.RecoveryStats())
			return
		}
		got := map[int]int{}
		for _, row := range mustExec(t, e, "SELECT id, bal FROM b").Rows {
			got[int(row[0].Int)] = int(row[1].Int)
		}
		var diffs []string
		for id, v := range want {
			if g, ok := got[id]; !ok {
				diffs = append(diffs, fmt.Sprintf("row %d missing (want %d)", id, v))
			} else if g != v {
				diffs = append(diffs, fmt.Sprintf("row %d = %d, want %d", id, g, v))
			}
		}
		for id, v := range got {
			if _, ok := want[id]; !ok {
				diffs = append(diffs, fmt.Sprintf("row %d = %d should not exist (an uncommitted insert survived)", id, v))
			}
		}
		if len(diffs) > 0 {
			sort.Strings(diffs)
			if len(diffs) > 8 {
				diffs = append(diffs[:8], fmt.Sprintf("... and %d more", len(diffs)-8))
			}
			t.Errorf("round %d (%s, pool=%d): %d rows differ from the committed state:\n  %s", round, label, poolSize, len(diffs), strings.Join(diffs, "\n  "))
		}
	}

	e = open()
	st := e.RecoveryStats()
	verify(e, "after crash")
	e.crash() // 恢复刚做完就再崩溃一次: 结果不变
	e = open()
	verify(e, "after second crash")
	mustExec(t, e, "INSERT INTO b VALUES (10,1)") // 失败者插过的键可以重新插入 (没有残留)
	e.Close()
	return st
}
