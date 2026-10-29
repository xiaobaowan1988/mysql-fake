package db

import (
	"fmt"
	"math/rand"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"mysqlfake/optimizer"
)

func rowSet(r *Result) []string {
	var out []string
	for _, row := range r.Rows {
		var cs []string
		for _, v := range row {
			cs = append(cs, v.String())
		}
		out = append(out, strings.Join(cs, ","))
	}
	sort.Strings(out)
	return out
}

func explainText(t *testing.T, e *Engine, q string) string {
	t.Helper()
	r := mustExec(t, e, "EXPLAIN "+q)
	var lines []string
	for _, row := range r.Rows {
		lines = append(lines, row[0].Str)
	}
	return strings.Join(lines, "\n")
}

func TestJoinBasics(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(10), city INT)")
	mustExec(t, e, "CREATE TABLE orders (id INT PRIMARY KEY, uid INT, amount INT)")
	mustExec(t, e, "INSERT INTO users VALUES (1,'ann',10),(2,'bob',20),(3,'cy',10),(4,'dee',NULL)")
	mustExec(t, e, "INSERT INTO orders VALUES (1,1,100),(2,1,50),(3,2,75),(4,9,5),(5,NULL,1)")

	got := rowSet(mustExec(t, e, "SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.uid"))
	if fmt.Sprint(got) != "[ann,100 ann,50 bob,75]" {
		t.Fatalf("join: %v", got)
	}
	got = rowSet(mustExec(t, e, "SELECT u.name, o.amount FROM users u, orders o WHERE u.id = o.uid AND o.amount > 60"))
	if fmt.Sprint(got) != "[ann,100 bob,75]" {
		t.Fatalf("comma join: %v", got)
	}
	// 自连接 (别名)
	got = rowSet(mustExec(t, e, "SELECT a.name, b.name FROM users a JOIN users b ON a.city = b.city WHERE a.id < b.id"))
	if fmt.Sprint(got) != "[ann,cy]" {
		t.Fatalf("self join: %v", got)
	}
	// ORDER BY / LIMIT / SELECT *
	r := mustExec(t, e, "SELECT * FROM orders ORDER BY amount DESC LIMIT 2")
	if rowsOf(r) != "1,1,100;3,2,75" {
		t.Fatalf("order/limit: %s", rowsOf(r))
	}
	r = mustExec(t, e, "SELECT o.id, u.id FROM orders o JOIN users u ON o.uid = u.id ORDER BY o.id DESC")
	if rowsOf(r) != "3,2;2,1;1,1" {
		t.Fatalf("join order by: %s", rowsOf(r))
	}
	// 笛卡尔积
	if r := mustExec(t, e, "SELECT u.id, o.id FROM users u, orders o"); len(r.Rows) != 20 {
		t.Fatalf("cross: %d", len(r.Rows))
	}
	// 错误
	for _, q := range []string{
		"SELECT id FROM users u JOIN orders o ON u.id = o.uid", // id 有歧义
		"SELECT x.id FROM users u",
		"SELECT u.nope FROM users u",
		"SELECT * FROM users u JOIN users u ON u.id = u.id",
		"SELECT * FROM users u JOIN orders o ON u.name = o.amount", // 类型不匹配
		"SELECT * FROM users JOIN nope ON 1 = 1",
		"SELECT * FROM users u JOIN orders o ON u.id = o.uid FOR UPDATE",
		"SELECT * FROM users ORDER BY nope",
	} {
		if _, err := e.Exec(q); err == nil {
			t.Errorf("expected error for %q", q)
		}
	}
	// 常量谓词
	if r := mustExec(t, e, "SELECT * FROM users WHERE 1 = 0"); len(r.Rows) != 0 {
		t.Fatal("constant false")
	}
	if r := mustExec(t, e, "SELECT * FROM users WHERE 1 = 1 ORDER BY id LIMIT 1"); rowsOf(r) != "1,ann,10" {
		t.Fatalf("got %s", rowsOf(r))
	}
}

// 随机查询: 所有 Join 算法、DP 重排与"按 FROM 顺序的嵌套循环"基线结果必须一致。
func TestOptimizerRandomQueriesAgreeWithBaseline(t *testing.T) {
	e := newEngine(t)
	rng := rand.New(rand.NewSource(11))
	for _, ddl := range []string{
		"CREATE TABLE ta (id INT PRIMARY KEY, x INT, y INT, s VARCHAR(5))",
		"CREATE TABLE tb (id INT PRIMARY KEY, x INT, y INT, s VARCHAR(5))",
		"CREATE TABLE tc (id INT PRIMARY KEY, x INT, y INT, s VARCHAR(5))",
	} {
		mustExec(t, e, ddl)
	}
	lit := func(domain int) string {
		if rng.Intn(8) == 0 {
			return "NULL"
		}
		return fmt.Sprint(rng.Intn(domain))
	}
	for _, tb := range []string{"ta", "tb", "tc"} {
		n := 25 + rng.Intn(40)
		var vals []string
		for i := 0; i < n; i++ {
			s := "NULL"
			if rng.Intn(6) != 0 {
				s = fmt.Sprintf("'v%d'", rng.Intn(5))
			}
			vals = append(vals, fmt.Sprintf("(%d,%s,%s,%s)", i, lit(6), lit(10), s))
		}
		mustExec(t, e, fmt.Sprintf("INSERT INTO %s VALUES %s", tb, strings.Join(vals, ",")))
	}
	mustExec(t, e, "ANALYZE TABLE ta")
	mustExec(t, e, "ANALYZE TABLE tb")
	// tc 故意不分析: 无统计信息的路径 (ensureStats 会补)
	var configs []optimizer.Options
	for _, rowMode := range []bool{false, true} { // 向量化与行式两种执行模式都要与基线一致
		for _, c := range []optimizer.Options{{ForceJoin: "hash"}, {ForceJoin: "merge"}, {ForceJoin: "nl"}, {}, {NoReorder: true}} {
			c.RowMode = rowMode
			configs = append(configs, c)
		}
	}
	tables := []string{"ta", "tb", "tc"}
	for round := 0; round < 120; round++ {
		n := 2 + rng.Intn(2)
		perm := rng.Perm(3)[:n]
		var from []string
		for i, p := range perm {
			from = append(from, fmt.Sprintf("%s a%d", tables[p], i))
		}
		var conds []string
		for i := 1; i < n; i++ { // 保证大多数情况下是连通的
			if rng.Intn(5) > 0 {
				ca, cb := []string{"x", "y", "id"}[rng.Intn(3)], []string{"x", "y", "id"}[rng.Intn(3)]
				conds = append(conds, fmt.Sprintf("a%d.%s = a%d.%s", rng.Intn(i), ca, i, cb))
			}
		}
		for k := rng.Intn(3); k > 0; k-- {
			a := rng.Intn(n)
			switch rng.Intn(5) {
			case 0:
				conds = append(conds, fmt.Sprintf("a%d.x %s %d", a, []string{"<", "<=", ">", ">=", "=", "!="}[rng.Intn(6)], rng.Intn(6)))
			case 1:
				conds = append(conds, fmt.Sprintf("a%d.s = 'v%d'", a, rng.Intn(5)))
			case 2:
				conds = append(conds, fmt.Sprintf("a%d.y IS NULL", a))
			case 3:
				conds = append(conds, fmt.Sprintf("(a%d.x = %d OR a%d.y > %d)", a, rng.Intn(6), a, rng.Intn(10)))
			case 4:
				b := rng.Intn(n)
				if b != a {
					conds = append(conds, fmt.Sprintf("a%d.y < a%d.y", a, b))
				}
			}
		}
		q := "SELECT * FROM " + strings.Join(from, ", ")
		if len(conds) > 0 {
			q += " WHERE " + strings.Join(conds, " AND ")
		}
		e.SetPlannerOptions(optimizer.Options{ForceJoin: "nl", NoReorder: true, RowMode: true}) // 最朴素的行式计划作为基线
		base := rowSet(mustExec(t, e, q))
		for _, cfg := range configs {
			e.SetPlannerOptions(cfg)
			got := rowSet(mustExec(t, e, q))
			if fmt.Sprint(got) != fmt.Sprint(base) {
				t.Fatalf("round %d config %+v differs from baseline\nquery: %s\nbase(%d rows) vs got(%d rows)", round, cfg, q, len(base), len(got))
			}
		}
	}
}

func TestOrderByLimitAcrossJoin(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE a (id INT PRIMARY KEY, k INT)")
	mustExec(t, e, "CREATE TABLE b (id INT PRIMARY KEY, k INT, v INT)")
	var av, bv []string
	for i := 0; i < 50; i++ {
		av = append(av, fmt.Sprintf("(%d,%d)", i, i%7))
		bv = append(bv, fmt.Sprintf("(%d,%d,%d)", i, i%7, 1000-i))
	}
	mustExec(t, e, "INSERT INTO a VALUES "+strings.Join(av, ","))
	mustExec(t, e, "INSERT INTO b VALUES "+strings.Join(bv, ","))
	for _, cfg := range []optimizer.Options{{}, {ForceJoin: "hash"}, {ForceJoin: "merge"}, {ForceJoin: "nl"}} {
		e.SetPlannerOptions(cfg)
		r := mustExec(t, e, "SELECT a.id, b.id FROM a JOIN b ON a.k = b.k ORDER BY b.v, a.id LIMIT 5")
		// b.v 最小的是 id=49 (v=951)，其 k = 0，所以 a.k = 0 的行 0,7,14,21,28 最先出现
		want := "0,49;7,49;14,49;21,49;28,49"
		if rowsOf(r) != want {
			t.Fatalf("%+v: got %s want %s", cfg, rowsOf(r), want)
		}
	}
}

func bulkInsert(t *testing.T, e *Engine, table string, n int, row func(i int) string) {
	t.Helper()
	for start := 0; start < n; start += 200 {
		var vals []string
		for i := start; i < n && i < start+200; i++ {
			vals = append(vals, row(i))
		}
		mustExec(t, e, fmt.Sprintf("INSERT INTO %s VALUES %s", table, strings.Join(vals, ",")))
	}
}

func TestOptimizerAccessPathChoice(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE big (id INT PRIMARY KEY, v INT)")
	bulkInsert(t, e, "big", 5000, func(i int) string { return fmt.Sprintf("(%d,%d)", i, i%50) })
	mustExec(t, e, "ANALYZE TABLE big")
	// 选择性很强的主键谓词 -> IndexScan；几乎全表的范围 -> SeqScan
	if plan := explainText(t, e, "SELECT * FROM big WHERE id = 4242"); !strings.Contains(plan, "IndexScan") {
		t.Fatalf("point lookup should use the index:\n%s", plan)
	}
	if plan := explainText(t, e, "SELECT * FROM big WHERE id >= 10 AND id < 20"); !strings.Contains(plan, "IndexScan") {
		t.Fatalf("narrow range should use the index:\n%s", plan)
	}
	if plan := explainText(t, e, "SELECT * FROM big WHERE id > 100"); !strings.Contains(plan, "SeqScan") {
		t.Fatalf("wide range should seq scan:\n%s", plan)
	}
	if plan := explainText(t, e, "SELECT * FROM big WHERE v = 3"); !strings.Contains(plan, "SeqScan") {
		t.Fatalf("non-key predicate seq scan:\n%s", plan)
	}
	// 估计行数接近真实: v = 3 实际 100 行
	plan, err := e.PlanQuery("SELECT * FROM big WHERE v = 3")
	if err != nil {
		t.Fatal(err)
	}
	if plan.Rows < 60 || plan.Rows > 160 {
		t.Fatalf("estimated rows for v=3: %.1f (actual 100)", plan.Rows)
	}
}

func TestOptimizerHandlesSkew(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE ev (id INT PRIMARY KEY, kind INT)")
	// kind=0 占 90%，其余 100 种各占很少
	bulkInsert(t, e, "ev", 10000, func(i int) string {
		k := 0
		if i%10 == 0 {
			k = 1 + (i/10)%100
		}
		return fmt.Sprintf("(%d,%d)", i, k)
	})
	mustExec(t, e, "ANALYZE TABLE ev")
	hot, _ := e.PlanQuery("SELECT * FROM ev WHERE kind = 0")
	rare, _ := e.PlanQuery("SELECT * FROM ev WHERE kind = 37")
	if hot.Rows < 8000 || hot.Rows > 10000 {
		t.Fatalf("hot value estimate %.0f, actual 9000", hot.Rows)
	}
	if rare.Rows > 30 {
		t.Fatalf("rare value estimate %.0f, actual 10", rare.Rows)
	}
	// 若按"均匀假设" 两者估计会相同 (≈ N/NDV ≈ 99)，这里差两个数量级
	if hot.Rows < 50*rare.Rows {
		t.Fatalf("skew not captured: hot=%.0f rare=%.0f", hot.Rows, rare.Rows)
	}
}

func TestJoinReorderBeatsFromOrder(t *testing.T) {
	e := newEngine(t)
	for _, ddl := range []string{
		"CREATE TABLE fact1 (id INT PRIMARY KEY, d1 INT, d2 INT)",
		"CREATE TABLE fact2 (id INT PRIMARY KEY, f1 INT)",
		"CREATE TABLE dim1 (id INT PRIMARY KEY, tag INT)",
		"CREATE TABLE dim2 (id INT PRIMARY KEY, tag INT)",
	} {
		mustExec(t, e, ddl)
	}
	bulkInsert(t, e, "fact1", 3000, func(i int) string { return fmt.Sprintf("(%d,%d,%d)", i, i%100, i%50) })
	bulkInsert(t, e, "fact2", 3000, func(i int) string { return fmt.Sprintf("(%d,%d)", i, i%3000) })
	bulkInsert(t, e, "dim1", 100, func(i int) string { return fmt.Sprintf("(%d,%d)", i, i%10) })
	bulkInsert(t, e, "dim2", 50, func(i int) string { return fmt.Sprintf("(%d,%d)", i, i%5) })
	for _, tb := range []string{"fact1", "fact2", "dim1", "dim2"} {
		mustExec(t, e, "ANALYZE TABLE "+tb)
	}
	// FROM 顺序故意写得很差: 先把两张大表连接，最后才连带过滤条件的小维表
	q := "SELECT * FROM fact1 f1, fact2 f2, dim1 d1, dim2 d2 WHERE f1.id = f2.f1 AND f1.d1 = d1.id AND f1.d2 = d2.id AND d1.tag = 3 AND d2.tag = 3"
	e.SetPlannerOptions(optimizer.Options{NoReorder: true})
	naive, err := e.PlanQuery(q)
	if err != nil {
		t.Fatal(err)
	}
	e.SetPlannerOptions(optimizer.Options{})
	best, err := e.PlanQuery(q)
	if err != nil {
		t.Fatal(err)
	}
	if best.Cost >= naive.Cost {
		t.Fatalf("DP plan (%.1f) should be cheaper than FROM-order plan (%.1f)\n%s", best.Cost, naive.Cost, strings.Join(best.Explain(), "\n"))
	}
	t.Logf("naive cost %.1f, optimized cost %.1f\n%s", naive.Cost, best.Cost, strings.Join(best.Explain(), "\n"))
	// 结果一致
	e.SetPlannerOptions(optimizer.Options{NoReorder: true, ForceJoin: "nl"})
	_ = naive
	small := "SELECT f1.id, d1.id, d2.id FROM fact1 f1, dim1 d1, dim2 d2 WHERE f1.d1 = d1.id AND f1.d2 = d2.id AND d1.tag = 3 AND d2.tag = 3"
	want := rowSet(mustExec(t, e, small))
	e.SetPlannerOptions(optimizer.Options{})
	if got := rowSet(mustExec(t, e, small)); fmt.Sprint(got) != fmt.Sprint(want) || len(want) == 0 {
		t.Fatalf("reordered plan result differs (%d vs %d rows)", len(got), len(want))
	}
}

func TestMergeJoinChosenWhenBothInputsOrdered(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE p (id INT PRIMARY KEY, v INT)")
	mustExec(t, e, "CREATE TABLE q (id INT PRIMARY KEY, w INT)")
	bulkInsert(t, e, "p", 2000, func(i int) string { return fmt.Sprintf("(%d,%d)", i, i) })
	bulkInsert(t, e, "q", 2000, func(i int) string { return fmt.Sprintf("(%d,%d)", i, i) })
	mustExec(t, e, "ANALYZE TABLE p")
	mustExec(t, e, "ANALYZE TABLE q")
	plan := explainText(t, e, "SELECT * FROM p JOIN q ON p.id = q.id")
	if !strings.Contains(plan, "MergeJoin") || strings.Contains(plan, "sort=[left:true") {
		t.Fatalf("PK-PK join should merge without sorting:\n%s", plan)
	}
	// 连接键不是主键: 需要排序或哈希，不应声称免排序
	plan = explainText(t, e, "SELECT * FROM p JOIN q ON p.v = q.w")
	if strings.Contains(plan, "sort=[left:false right:false]") {
		t.Fatalf("unordered inputs cannot merge for free:\n%s", plan)
	}
	// ORDER BY 主键且已有序 -> 不需要额外 Sort
	if plan := explainText(t, e, "SELECT * FROM p ORDER BY id"); strings.Contains(plan, "Sort") {
		t.Fatalf("redundant sort:\n%s", plan)
	}
	if plan := explainText(t, e, "SELECT * FROM p ORDER BY v"); !strings.Contains(plan, "Sort") {
		t.Fatalf("missing sort:\n%s", plan)
	}
}

func TestStatsDaemonRefreshesAutomatically(t *testing.T) {
	e, err := OpenWithOptions(filepath.Join(t.TempDir(), "sd.db"), Options{StatsInterval: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	mustExec(t, e, "CREATE TABLE t (id INT PRIMARY KEY, v INT)")
	bulkInsert(t, e, "t", 300, func(i int) string { return fmt.Sprintf("(%d,%d)", i, i%7) })
	waitFor := func(rows float64) {
		t.Helper()
		for deadline := time.Now().Add(3 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
			if ts := e.Stats().Get("t"); ts != nil && ts.Rows == rows {
				return
			}
		}
		ts := e.Stats().Get("t")
		t.Fatalf("daemon did not refresh stats to %v rows (have %+v)", rows, ts)
	}
	waitFor(300)
	bulkInsert(t, e, "t", 300, func(i int) string { return fmt.Sprintf("(%d,%d)", 1000+i, i%7) })
	waitFor(600) // 修改量超过 10% 阈值后自动重新采样
	if ts := e.Stats().Get("t"); ts.Col("v") == nil || ts.Col("v").NDV < 6 || ts.Col("v").NDV > 8 {
		t.Fatalf("bad column stats: %+v", ts.Col("v"))
	}
}
