package db

import (
	"fmt"
	"math/rand"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"mysqlfake/optimizer"
)

func idsOf(r *Result) []string {
	var out []string
	for _, row := range r.Rows {
		out = append(out, row[0].String())
	}
	sort.Strings(out)
	return out
}

// queryBoth 用二级索引 (默认) 和禁用索引 (全表扫描) 各执行一次，结果必须一致。
func queryBoth(t *testing.T, e *Engine, q string) []string {
	t.Helper()
	e.SetPlannerOptions(optimizer.Options{})
	with := idsOf(mustExec(t, e, q))
	e.SetPlannerOptions(optimizer.Options{NoIndex: true})
	without := idsOf(mustExec(t, e, q))
	e.SetPlannerOptions(optimizer.Options{})
	if fmt.Sprint(with) != fmt.Sprint(without) {
		t.Fatalf("index scan and table scan disagree for %q:\n index: %v\n scan:  %v", q, with, without)
	}
	return with
}

func TestCreateIndexAccessPathAndResults(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t (id INT PRIMARY KEY, k INT, s VARCHAR(20), v INT)")
	bulkInsert(t, e, "t", 4000, func(i int) string {
		k := fmt.Sprint(i % 200)
		if i%50 == 0 {
			k = "NULL"
		}
		return fmt.Sprintf("(%d,%s,'s%d',%d)", i, k, i%37, i)
	})
	mustExec(t, e, "CREATE INDEX idx_k ON t (k)")
	mustExec(t, e, "CREATE INDEX idx_s ON t (s)")
	mustExec(t, e, "ANALYZE TABLE t")
	if plan := explainText(t, e, "SELECT * FROM t WHERE k = 7"); !strings.Contains(plan, "index idx_k") {
		t.Fatalf("selective predicate on an indexed column should use the index:\n%s", plan)
	}
	if plan := explainText(t, e, "SELECT * FROM t WHERE k >= 5"); strings.Contains(plan, "index idx_k") {
		t.Fatalf("an unselective range must not use the index:\n%s", plan)
	}
	if plan := explainText(t, e, "SELECT * FROM t WHERE s = 's5' AND v > 100"); !strings.Contains(plan, "index idx_s") {
		t.Fatalf("varchar index expected:\n%s", plan)
	}
	for _, q := range []string{
		"SELECT id FROM t WHERE k = 7",
		"SELECT id FROM t WHERE k < 3",
		"SELECT id FROM t WHERE k > 197",
		"SELECT id FROM t WHERE k >= 10 AND k <= 12",
		"SELECT id FROM t WHERE k = 7 AND v > 1000",
		"SELECT id FROM t WHERE 7 = k",
		"SELECT id FROM t WHERE k IS NULL", // NULL 不在索引里: 必须走全表
		"SELECT id FROM t WHERE k = 99999",
		"SELECT id FROM t WHERE s = 's5'",
		"SELECT id FROM t WHERE s >= 's3' AND s < 's4'",
		"SELECT id FROM t WHERE s != 's5' AND k = 1",
		"SELECT id FROM t WHERE k = 7 OR k = 8",
	} {
		got := queryBoth(t, e, q)
		if strings.Contains(q, "k = 7") && !strings.Contains(q, "OR") && !strings.Contains(q, "99999") && len(got) == 0 {
			t.Fatalf("%s returned nothing", q)
		}
	}
	// 表本身的数据和索引一起持久化
	mustExec(t, e, "UPDATE t SET v = v + 1 WHERE k = 7")
	if got := rowSet(mustExec(t, e, "SELECT v FROM t WHERE id = 7")); fmt.Sprint(got) != "[8]" {
		t.Fatalf("update through index: %v", got)
	}
	// 错误
	for _, q := range []string{
		"CREATE INDEX idx_k ON t (v)", "CREATE INDEX x ON t (nope)", "CREATE INDEX x ON nope (a)", "CREATE INDEX x ON t (id)",
	} {
		if _, err := e.Exec(q); err == nil {
			t.Errorf("expected error for %q", q)
		}
	}
}

func TestIndexMaintenanceAndRollback(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t (id INT PRIMARY KEY, k INT)")
	mustExec(t, e, "INSERT INTO t VALUES (1,10),(2,20),(3,NULL)")
	mustExec(t, e, "CREATE INDEX ik ON t (k)")
	count := func() int {
		ti, _ := e.cat.GetTable("t")
		n := 0
		it := ti.Indexes[0].Tree.Scan(nil, nil)
		for it.Next() {
			n++
		}
		return n
	}
	if count() != 2 {
		t.Fatalf("index should have 2 entries (NULL not indexed), has %d", count())
	}
	s := e.NewSession()
	sexec(t, s, "BEGIN")
	sexec(t, s, "INSERT INTO t VALUES (4,40)")
	sexec(t, s, "UPDATE t SET k = 11 WHERE id = 1")
	sexec(t, s, "UPDATE t SET k = 12 WHERE id = 1")
	sexec(t, s, "UPDATE t SET k = 10 WHERE id = 1") // 改回原值: 原来的索引项还在，不应重复也不应被撤销
	sexec(t, s, "UPDATE t SET k = 30 WHERE id = 3") // NULL -> 30
	if count() != 6 {                               // 10,20 + 40 + 11 + 12 + 30
		t.Fatalf("after DML: %d entries", count())
	}
	sexec(t, s, "ROLLBACK")
	if count() != 2 {
		t.Fatalf("rollback must remove exactly the entries it added, have %d", count())
	}
	if got := queryBoth(t, e, "SELECT id FROM t WHERE k = 10"); fmt.Sprint(got) != "[1]" {
		t.Fatalf("row 1 lost after rollback: %v", got)
	}
	// 语句级回滚也要撤销索引项
	sexec(t, s, "BEGIN")
	sexec(t, s, "INSERT INTO t VALUES (5,50)")
	if _, err := s.Exec("INSERT INTO t VALUES (6,60),(7,70),(1,1)"); err == nil { // 主键冲突，整条语句回滚
		t.Fatal("expected duplicate key")
	}
	sexec(t, s, "COMMIT")
	if count() != 3 { // 10, 20, 50
		t.Fatalf("statement rollback left index entries: %d", count())
	}
	// 删除后重新插入 (在删除标记上建立新版本) 同样维护索引
	mustExec(t, e, "DELETE FROM t WHERE id = 2")
	mustExec(t, e, "INSERT INTO t VALUES (2,22)")
	if got := queryBoth(t, e, "SELECT id FROM t WHERE k = 22"); fmt.Sprint(got) != "[2]" {
		t.Fatalf("got %v", got)
	}
	if got := queryBoth(t, e, "SELECT id FROM t WHERE k = 20"); len(got) != 0 {
		t.Fatalf("stale index entry leaked a deleted version: %v", got)
	}
}

func TestIndexSnapshotSemanticsAndValidFrom(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t (id INT PRIMARY KEY, k INT)")
	bulkInsert(t, e, "t", 2000, func(i int) string { return fmt.Sprintf("(%d,%d)", i, i%100) })
	old := e.NewSession()
	sexec(t, old, "BEGIN")
	sexec(t, old, "SELECT * FROM t WHERE id = 1") // 在建索引之前建立 Read View
	mustExec(t, e, "CREATE INDEX ik ON t (k)")
	mustExec(t, e, "ANALYZE TABLE t")
	// 旧视图不能用新索引 (它可能需要未被索引的旧值)；新会话可以
	if plan := explainText(t, e, "SELECT * FROM t WHERE k = 5"); !strings.Contains(plan, "index ik") {
		t.Fatalf("new views should use the index:\n%s", plan)
	}
	r := sexec(t, old, "EXPLAIN SELECT * FROM t WHERE k = 5")
	for _, row := range r.Rows {
		if strings.Contains(row[0].Str, "index ik") {
			t.Fatal("a Read View older than the index must not use it")
		}
	}
	if got := len(sexec(t, old, "SELECT id FROM t WHERE k = 5").Rows); got != 20 {
		t.Fatalf("old view result: %d rows", got)
	}
	sexec(t, old, "COMMIT")

	// 快照读经过陈旧索引项仍然正确
	snap, w := e.NewSession(), e.NewSession()
	sexec(t, snap, "BEGIN")
	if got := len(sexec(t, snap, "SELECT id FROM t WHERE k = 5").Rows); got != 20 {
		t.Fatal(got)
	}
	sexec(t, w, "UPDATE t SET k = 999 WHERE k = 5") // 写者把这 20 行都改成 999 并提交
	if got := len(sexec(t, snap, "SELECT id FROM t WHERE k = 5").Rows); got != 20 {
		t.Fatalf("snapshot must still see the old value through the stale index entries, got %d", got)
	}
	if got := len(sexec(t, snap, "SELECT id FROM t WHERE k = 999").Rows); got != 0 {
		t.Fatalf("snapshot must not see the new value, got %d", got)
	}
	sexec(t, snap, "COMMIT")
	if got := len(mustExec(t, e, "SELECT id FROM t WHERE k = 5").Rows); got != 0 {
		t.Fatalf("latest view must not return stale entries, got %d", got)
	}
	if got := len(mustExec(t, e, "SELECT id FROM t WHERE k = 999").Rows); got != 20 {
		t.Fatalf("got %d", got)
	}
	// 当前读 (UPDATE/DELETE) 通过索引定位候选行，并用最新值重新校验谓词
	if r := mustExec(t, e, "UPDATE t SET k = 5 WHERE k = 5"); r.Affected != 0 {
		t.Fatalf("stale index entries matched rows that no longer satisfy the predicate: %d", r.Affected)
	}
	if r := mustExec(t, e, "DELETE FROM t WHERE k = 999"); r.Affected != 20 {
		t.Fatalf("delete via index affected %d", r.Affected)
	}
}

func TestAddColumnBasicsAndPersistence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ac.db")
	e, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	mustExec(t, e, "CREATE TABLE t (id INT PRIMARY KEY, a INT)")
	mustExec(t, e, "INSERT INTO t VALUES (1,10),(2,20)")
	mustExec(t, e, "CREATE INDEX ia ON t (a)")
	mustExec(t, e, "ALTER TABLE t ADD COLUMN b INT DEFAULT 7")
	mustExec(t, e, "ALTER TABLE t ADD c VARCHAR(10)")
	mustExec(t, e, "ALTER TABLE t ADD COLUMN d VARCHAR(5) DEFAULT 'hi'")
	if got := rowsOf(mustExec(t, e, "SELECT * FROM t ORDER BY id")); got != "1,10,7,NULL,hi;2,20,7,NULL,hi" {
		t.Fatalf("backfill: %s", got)
	}
	mustExec(t, e, "INSERT INTO t VALUES (3,30,1,'x','y')")
	mustExec(t, e, "UPDATE t SET c = 'set' WHERE id = 1")
	// 原有索引在换表后仍然有效
	if got := queryBoth(t, e, "SELECT id FROM t WHERE a = 20"); fmt.Sprint(got) != "[2]" {
		t.Fatalf("existing index broke after ADD COLUMN: %v", got)
	}
	for _, q := range []string{
		"ALTER TABLE t ADD COLUMN a INT", "ALTER TABLE t ADD COLUMN z INT DEFAULT 'str'", "ALTER TABLE nope ADD COLUMN z INT",
		"ALTER TABLE t ADD COLUMN z INT PRIMARY KEY", "ALTER TABLE t ADD COLUMN z VARCHAR(2) DEFAULT 'toolong'",
	} {
		if _, err := e.Exec(q); err == nil {
			t.Errorf("expected error for %q", q)
		}
	}
	e.crash() // 不做 Checkpoint 直接宕机: 表结构、索引和数据都靠 WAL 恢复

	e, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if got := rowsOf(mustExec(t, e, "SELECT * FROM t ORDER BY id")); got != "1,10,7,set,hi;2,20,7,NULL,hi;3,30,1,x,y" {
		t.Fatalf("after crash recovery: %s", got)
	}
	if got := queryBoth(t, e, "SELECT id FROM t WHERE a = 30"); fmt.Sprint(got) != "[3]" {
		t.Fatalf("index after recovery: %v", got)
	}
	ti, _ := e.cat.GetTable("t")
	if len(ti.Indexes) != 1 || ti.Indexes[0].Name != "ia" || len(ti.Schema.Columns) != 5 {
		t.Fatalf("catalog not persisted: %+v", ti)
	}
}

type writerStats struct {
	ops atomic.Int64
}

// 在线加列: 拷贝/回放/切换期间持续有并发的 INSERT/UPDATE/DELETE，切换后不能丢任何一次写入。
func TestOnlineAddColumnUnderConcurrentWrites(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE w (id INT PRIMARY KEY, v INT)")
	const initial = 3000
	bulkInsert(t, e, "w", initial, func(i int) string { return fmt.Sprintf("(%d,0)", i) })
	t0 := time.Now()
	e.SetDDLHook(func(stage string) {
		if stage != "copy" {
			t.Logf("[%v] ddl stage %s", time.Since(t0).Round(time.Millisecond), stage)
		}
		if stage == "copy" {
			time.Sleep(3 * time.Millisecond) // 拉长拷贝阶段，保证与写入重叠
		}
	})
	const writers = 4
	models := make([]map[int]int, writers) // id -> v (只由对应 writer 写入)
	stop := make(chan struct{})
	var ws writerStats
	var inDDL atomic.Bool
	var opsDuringDDL atomic.Int64
	var wg sync.WaitGroup
	for g := 0; g < writers; g++ {
		models[g] = map[int]int{}
		for id := g; id < initial; id += writers {
			models[g][id] = 0
		}
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			s := e.NewSession()
			rng := rand.New(rand.NewSource(int64(g)))
			next := initial + g // 新插入的 id 都满足 id%writers == g
			for {
				select {
				case <-stop:
					return
				default:
				}
				time.Sleep(200 * time.Microsecond) // 模拟客户端的间隔，避免写入速度超过回放能力
				var err error
				switch rng.Intn(4) {
				case 0, 1: // 更新自己拥有的某个初始行 (初始行永不删除)
					id := rng.Intn(initial/writers)*writers + g
					models[g][id]++
					_, err = s.Exec(fmt.Sprintf("UPDATE w SET v = v + 1 WHERE id = %d", id))
				case 2:
					models[g][next] = 1
					_, err = s.Exec(fmt.Sprintf("INSERT INTO w (id, v) VALUES (%d, 1)", next))
					next += writers
				case 3: // 删除自己插入过的行 (若有)
					if next-writers >= initial {
						id := next - writers
						if _, ok := models[g][id]; ok {
							delete(models[g], id)
							_, err = s.Exec(fmt.Sprintf("DELETE FROM w WHERE id = %d", id))
							next -= writers
						}
					}
				}
				if err != nil {
					t.Errorf("writer %d: %v", g, err)
					return
				}
				ws.ops.Add(1)
				if inDDL.Load() {
					opsDuringDDL.Add(1)
				}
			}
		}(g)
	}
	for ws.ops.Load() < 200 {
		time.Sleep(time.Millisecond)
	}
	inDDL.Store(true)
	if _, err := e.Exec("ALTER TABLE w ADD COLUMN c INT DEFAULT 7"); err != nil {
		t.Fatal(err)
	}
	inDDL.Store(false)
	for target := ws.ops.Load() + 200; ws.ops.Load() < target; {
		time.Sleep(time.Millisecond)
	}
	close(stop)
	wg.Wait()
	if opsDuringDDL.Load() < 20 {
		t.Fatalf("writers barely overlapped with the DDL (%d ops); the test is not exercising concurrency", opsDuringDDL.Load())
	}
	t.Logf("%d writer statements ran while ALTER TABLE was in progress", opsDuringDDL.Load())

	r := mustExec(t, e, "SELECT id, v, c FROM w")
	got := map[int]int{}
	for _, row := range r.Rows {
		id, v := int(row[0].Int), int(row[1].Int)
		got[id] = v
		if initialRow := id < initial; initialRow && (row[2].Null || row[2].Int != 7) {
			t.Fatalf("initial row %d was not backfilled with the default: %v", id, row[2])
		}
		if !row[2].Null && row[2].Int != 7 {
			t.Fatalf("row %d has an impossible value in the new column: %v", id, row[2])
		}
	}
	want := map[int]int{}
	for _, m := range models {
		for id, v := range m {
			want[id] = v
		}
	}
	if len(got) != len(want) {
		t.Fatalf("row count %d, expected %d", len(got), len(want))
	}
	for id, v := range want {
		if gv, ok := got[id]; !ok || gv != v {
			t.Fatalf("row %d: got %v (present=%v), want %d — a write was lost across the online DDL", id, gv, ok, v)
		}
	}
}

func TestOnlineCreateIndexUnderConcurrentWrites(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE x (id INT PRIMARY KEY, k INT)")
	const initial = 3000
	bulkInsert(t, e, "x", initial, func(i int) string { return fmt.Sprintf("(%d,%d)", i, i%40) })
	t0 := time.Now()
	e.SetDDLHook(func(stage string) {
		if stage != "copy" {
			t.Logf("[%v] ddl stage %s", time.Since(t0).Round(time.Millisecond), stage)
		}
		if stage == "copy" {
			time.Sleep(3 * time.Millisecond)
		}
	})
	const writers = 4
	models := make([]map[int]int, writers)
	stop := make(chan struct{})
	var ops, during atomic.Int64
	var inDDL atomic.Bool
	var wg sync.WaitGroup
	for g := 0; g < writers; g++ {
		models[g] = map[int]int{}
		for id := g; id < initial; id += writers {
			models[g][id] = id % 40
		}
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			s := e.NewSession()
			rng := rand.New(rand.NewSource(int64(100 + g)))
			next := initial + g
			for {
				select {
				case <-stop:
					return
				default:
				}
				time.Sleep(200 * time.Microsecond)
				var err error
				switch rng.Intn(4) {
				case 0, 1: // 修改被索引的列
					id := rng.Intn(initial/writers)*writers + g
					k := rng.Intn(40)
					models[g][id] = k
					_, err = s.Exec(fmt.Sprintf("UPDATE x SET k = %d WHERE id = %d", k, id))
				case 2:
					k := rng.Intn(40)
					models[g][next] = k
					_, err = s.Exec(fmt.Sprintf("INSERT INTO x VALUES (%d, %d)", next, k))
					next += writers
				case 3:
					if next-writers >= initial {
						id := next - writers
						delete(models[g], id)
						_, err = s.Exec(fmt.Sprintf("DELETE FROM x WHERE id = %d", id))
						next -= writers
					}
				}
				if err != nil {
					t.Errorf("writer %d: %v", g, err)
					return
				}
				ops.Add(1)
				if inDDL.Load() {
					during.Add(1)
				}
			}
		}(g)
	}
	for ops.Load() < 200 {
		time.Sleep(time.Millisecond)
	}
	inDDL.Store(true)
	if _, err := e.Exec("CREATE INDEX ik ON x (k)"); err != nil {
		t.Fatal(err)
	}
	inDDL.Store(false)
	for target := ops.Load() + 300; ops.Load() < target; {
		time.Sleep(time.Millisecond)
	}
	close(stop)
	wg.Wait()
	if during.Load() < 20 {
		t.Fatalf("writers barely overlapped with the DDL (%d ops)", during.Load())
	}
	t.Logf("%d writer statements ran while CREATE INDEX was in progress", during.Load())

	mustExec(t, e, "ANALYZE TABLE x")
	byK := map[int][]string{}
	for _, m := range models {
		for id, k := range m {
			byK[k] = append(byK[k], fmt.Sprint(id))
		}
	}
	for k := 0; k < 40; k++ {
		want := byK[k]
		sort.Strings(want)
		if got := queryBoth(t, e, fmt.Sprintf("SELECT id FROM x WHERE k = %d", k)); fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("k=%d: index lookup %v, expected %v — the online index build lost or kept a wrong entry", k, got, want)
		}
	}
}

func TestMetadataLockBlocksCutoverUntilWritersFinish(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t (id INT PRIMARY KEY, a INT)")
	mustExec(t, e, "INSERT INTO t VALUES (1,1)")
	w := e.NewSession()
	sexec(t, w, "BEGIN")
	sexec(t, w, "UPDATE t SET a = 2 WHERE id = 1") // 持有 MDL S 直到事务结束
	done := make(chan error, 1)
	go func() { _, err := e.Exec("ALTER TABLE t ADD COLUMN b INT DEFAULT 5"); done <- err }()
	select {
	case err := <-done:
		t.Fatalf("DDL cutover must wait for the open writer (returned %v)", err)
	case <-time.After(300 * time.Millisecond):
	}
	// 新的写者在 DDL 排队 X 锁期间被阻塞 (FIFO)，直到 DDL 完成
	blocked := make(chan error, 1)
	go func() { _, err := e.NewSession().Exec("INSERT INTO t (id, a) VALUES (2,2)"); blocked <- err }()
	select {
	case <-blocked:
		t.Fatal("new writer must queue behind the pending metadata X lock")
	case <-time.After(150 * time.Millisecond):
	}
	sexec(t, w, "COMMIT")
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if err := <-blocked; err != nil {
		t.Fatal(err)
	}
	if got := rowsOf(mustExec(t, e, "SELECT * FROM t ORDER BY id")); got != "1,2,5;2,2,NULL" {
		t.Fatalf("got %s", got)
	}
}
