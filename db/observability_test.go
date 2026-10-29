package db

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"mysqlfake/metrics"
	"mysqlfake/optimizer"
)

// metricValue 从默认注册表的文本输出里读取一个样本 (精确匹配 "name{labels}" 或 "name")。
func metricValue(t testing.TB, sample string) float64 {
	t.Helper()
	var buf bytes.Buffer
	metrics.Default.Write(&buf)
	for _, line := range strings.Split(buf.String(), "\n") {
		if strings.HasPrefix(line, sample+" ") {
			v, err := strconv.ParseFloat(strings.TrimPrefix(line, sample+" "), 64)
			if err != nil {
				t.Fatal(err)
			}
			return v
		}
	}
	return 0
}

func TestStatementMetricsAndProfile(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t (id INT PRIMARY KEY, v INT)")
	bulkInsert(t, e, "t", 1000, func(i int) string { return fmt.Sprintf("(%d,%d)", i, i%10) })
	mustExec(t, e, "ANALYZE TABLE t")

	selOK := `mysqlfake_statements_total{type="select",status="ok"}`
	selErr := `mysqlfake_statements_total{type="select",status="error"}`
	insOK := `mysqlfake_statements_total{type="insert",status="ok"}`
	b0, b1, b2 := metricValue(t, selOK), metricValue(t, selErr), metricValue(t, insOK)
	sent0 := metricValue(t, "mysqlfake_rows_sent_total")
	hist0 := metricValue(t, `mysqlfake_statement_duration_seconds_count{type="select"}`)
	commits0 := metricValue(t, "mysqlfake_txn_commits_total")
	fsync0 := metricValue(t, "mysqlfake_wal_fsyncs_total")
	hits0, miss0 := metricValue(t, "mysqlfake_buffer_pool_hits_total"), metricValue(t, "mysqlfake_buffer_pool_misses_total")

	s := e.NewSession()
	sexec(t, s, "SELECT * FROM t WHERE v = 3") // 全表扫描: 1000 行被检查，100 行返回
	p := s.LastProfile()
	if p.Type != "select" || p.RowsSent != 100 || p.RowsExamined != 1000 {
		t.Fatalf("profile: %+v", p)
	}
	if p.Parse <= 0 || p.Plan <= 0 || p.Exec <= 0 || p.Total < p.Parse+p.Plan {
		t.Fatalf("stage timings missing: %+v", p)
	}
	sexec(t, s, "SELECT * FROM t WHERE id = 5") // 主键点查: 只检查极少的行
	if p := s.LastProfile(); p.RowsExamined > 3 || p.RowsSent != 1 {
		t.Fatalf("point lookup examined %d rows", p.RowsExamined)
	}
	if _, err := s.Exec("SELECT nope FROM t"); err == nil {
		t.Fatal("expected error")
	}
	if s.LastProfile().Err == nil {
		t.Fatal("profile must carry the error")
	}
	sexec(t, s, "INSERT INTO t VALUES (5000, 1)")

	if d := metricValue(t, selOK) - b0; d != 2 {
		t.Fatalf("ok selects +%v", d)
	}
	if d := metricValue(t, selErr) - b1; d != 1 {
		t.Fatalf("failed selects +%v", d)
	}
	if d := metricValue(t, insOK) - b2; d != 1 {
		t.Fatalf("inserts +%v", d)
	}
	if d := metricValue(t, "mysqlfake_rows_sent_total") - sent0; d != 101 {
		t.Fatalf("rows sent +%v", d)
	}
	if d := metricValue(t, `mysqlfake_statement_duration_seconds_count{type="select"}`) - hist0; d != 3 {
		t.Fatalf("latency histogram samples +%v", d)
	}
	if metricValue(t, "mysqlfake_txn_commits_total")-commits0 < 3 { // 两条成功的 SELECT + 一条 INSERT
		t.Fatal("commits not counted")
	}
	if metricValue(t, "mysqlfake_wal_fsyncs_total")-fsync0 < 1 {
		t.Fatal("the insert's commit must have fsynced the WAL")
	}
	if metricValue(t, "mysqlfake_buffer_pool_hits_total")-hits0+metricValue(t, "mysqlfake_buffer_pool_misses_total")-miss0 < 10 {
		t.Fatal("buffer pool accesses not counted")
	}
	for _, g := range []string{"mysqlfake_buffer_pool_dirty_pages", "mysqlfake_wal_size_bytes", "mysqlfake_database_pages", "mysqlfake_active_transactions"} {
		var buf bytes.Buffer
		metrics.Default.Write(&buf)
		if !strings.Contains(buf.String(), "\n"+g+" ") {
			t.Errorf("gauge %s not exposed", g)
		}
	}
}

func TestLockWaitMetricsAndProfile(t *testing.T) {
	e := newEngine(t)
	setupAccounts(t, e)
	waits0 := metricValue(t, "mysqlfake_lock_waits_total")
	hist0 := metricValue(t, "mysqlfake_lock_wait_seconds_count")
	dead0 := metricValue(t, "mysqlfake_deadlocks_total")
	a, b := e.NewSession(), e.NewSession()
	sexec(t, a, "BEGIN")
	sexec(t, a, "UPDATE acc SET bal = 1 WHERE id = 1")
	done := make(chan error, 1)
	go func() { _, err := b.Exec("UPDATE acc SET bal = 2 WHERE id = 1"); done <- err }()
	time.Sleep(150 * time.Millisecond)
	sexec(t, a, "COMMIT")
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	p := b.LastProfile()
	if p.LockWait < 100*time.Millisecond || p.LockWait > p.Total {
		t.Fatalf("lock wait not attributed to the blocked statement: %+v", p)
	}
	if metricValue(t, "mysqlfake_lock_waits_total")-waits0 < 1 || metricValue(t, "mysqlfake_lock_wait_seconds_count")-hist0 < 1 {
		t.Fatal("lock wait metrics missing")
	}
	// 死锁计数
	sexec(t, a, "BEGIN")
	sexec(t, b, "BEGIN")
	sexec(t, a, "UPDATE acc SET bal = 1 WHERE id = 1")
	sexec(t, b, "UPDATE acc SET bal = 2 WHERE id = 2")
	ch := make(chan error, 1)
	go func() { _, err := a.Exec("UPDATE acc SET bal = 3 WHERE id = 2"); ch <- err }()
	time.Sleep(100 * time.Millisecond)
	if _, err := b.Exec("UPDATE acc SET bal = 4 WHERE id = 1"); err == nil {
		t.Fatal("expected deadlock")
	}
	<-ch
	sexec(t, a, "ROLLBACK")
	if metricValue(t, "mysqlfake_deadlocks_total")-dead0 != 1 {
		t.Fatal("deadlock not counted")
	}
}

func TestSlowQueryLog(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "slow.log")
	e, err := OpenWithOptions(filepath.Join(dir, "s.db"), Options{SlowQueryThreshold: time.Nanosecond, SlowQueryLog: logPath})
	if err != nil {
		t.Fatal(err)
	}
	slow0 := metricValue(t, "mysqlfake_slow_queries_total")
	mustExec(t, e, "CREATE TABLE t (id INT PRIMARY KEY, v INT)")
	bulkInsert(t, e, "t", 300, func(i int) string { return fmt.Sprintf("(%d,%d)", i, i%3) })
	mustExec(t, e, "SELECT id FROM t WHERE v = 1")
	e.Exec("SELECT nope FROM t")
	e.Close()
	b, _ := os.ReadFile(logPath)
	log := string(b)
	for _, want := range []string{"# Time: ", "# Query_time: ", "Rows_sent: 100", "Rows_examined: 300", "Parse: ", "Plan: ", "Exec: ", "SELECT id FROM t WHERE v = 1;", "# Error: ", "SELECT nope FROM t;"} {
		if !strings.Contains(log, want) {
			t.Errorf("slow log missing %q:\n%s", want, log)
		}
	}
	if metricValue(t, "mysqlfake_slow_queries_total")-slow0 < 4 {
		t.Fatal("slow query counter not incremented")
	}
	// 高阈值: 不记录
	path2 := filepath.Join(dir, "slow2.log")
	e2, _ := OpenWithOptions(filepath.Join(dir, "s2.db"), Options{SlowQueryThreshold: time.Hour, SlowQueryLog: path2})
	mustExec(t, e2, "CREATE TABLE t (id INT PRIMARY KEY)")
	e2.Close()
	if b, _ := os.ReadFile(path2); len(b) != 0 {
		t.Fatalf("fast statements must not be logged: %s", b)
	}
}

func TestExplainAnalyze(t *testing.T) {
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE a (id INT PRIMARY KEY, k INT)")
	mustExec(t, e, "CREATE TABLE b (id INT PRIMARY KEY, k INT)")
	bulkInsert(t, e, "a", 500, func(i int) string { return fmt.Sprintf("(%d,%d)", i, i%50) })
	bulkInsert(t, e, "b", 200, func(i int) string { return fmt.Sprintf("(%d,%d)", i, i%50) })
	mustExec(t, e, "ANALYZE TABLE a")
	mustExec(t, e, "ANALYZE TABLE b")
	q := "SELECT a.id, b.id FROM a JOIN b ON a.k = b.k WHERE a.id < 100 AND b.k < 10 ORDER BY a.id LIMIT 30"
	want := len(mustExec(t, e, q).Rows)
	for _, rowMode := range []bool{false, true} {
		e.SetPlannerOptions(optimizer.Options{RowMode: rowMode})
		r := mustExec(t, e, "EXPLAIN ANALYZE "+q)
		var lines []string
		for _, row := range r.Rows {
			lines = append(lines, row[0].Str)
		}
		text := strings.Join(lines, "\n")
		ops := 0
		for _, l := range lines[:len(lines)-1] {
			if !strings.Contains(l, "(actual rows=") {
				t.Errorf("rowMode=%v: operator without actual stats: %s", rowMode, l)
			}
			ops++
		}
		if ops < 5 {
			t.Fatalf("plan unexpectedly small:\n%s", text)
		}
		// 根算子 (Project) 的实际行数就是查询结果行数
		if !strings.Contains(lines[0], fmt.Sprintf("actual rows=%d ", want)) {
			t.Fatalf("rowMode=%v root actual rows != %d:\n%s", rowMode, want, text)
		}
		last := lines[len(lines)-1]
		if !strings.Contains(last, fmt.Sprintf("rows returned=%d", want)) || !strings.Contains(last, "rows examined=") {
			t.Fatalf("summary line wrong: %s", last)
		}
		// 被过滤下推的扫描: 实际行数应远小于全表
		if !strings.Contains(text, "SeqScan") && !strings.Contains(text, "IndexScan") {
			t.Fatalf("no scans in plan:\n%s", text)
		}
		t.Logf("rowMode=%v\n%s", rowMode, text)
	}
	e.SetPlannerOptions(optimizer.Options{})
	// 普通 EXPLAIN 不带实际数据，也不执行
	r := mustExec(t, e, "EXPLAIN "+q)
	for _, row := range r.Rows {
		if strings.Contains(row[0].Str, "actual") {
			t.Fatal("plain EXPLAIN must not execute")
		}
	}
}

// 用独立实现的 Prometheus 解析器 (prometheus_client) 校验文本格式。
func TestPrometheusFormatAcceptedByOfficialParser(t *testing.T) {
	if err := exec.Command("python3", "-c", "import prometheus_client").Run(); err != nil {
		t.Skip("prometheus_client not installed")
	}
	e := newEngine(t)
	mustExec(t, e, "CREATE TABLE t (id INT PRIMARY KEY)")
	mustExec(t, e, "INSERT INTO t VALUES (1)")
	mustExec(t, e, "SELECT * FROM t")
	var buf bytes.Buffer
	metrics.Default.Write(&buf)
	script := `
import sys
from prometheus_client.parser import text_string_to_metric_families
fams = {f.name: f for f in text_string_to_metric_families(sys.stdin.read())}
need = ["mysqlfake_statements", "mysqlfake_statement_duration_seconds", "mysqlfake_buffer_pool_hits",
        "mysqlfake_lock_wait_seconds", "mysqlfake_wal_fsyncs", "mysqlfake_database_pages"]
missing = [n for n in need if n not in fams]
assert not missing, missing
h = fams["mysqlfake_statement_duration_seconds"]
assert h.type == "histogram"
assert any(s.name.endswith("_bucket") and s.labels.get("le") == "+Inf" for s in h.samples)
print("PROM-OK", len(fams))
`
	cmd := exec.Command("python3", "-I", "-c", script)
	cmd.Stdin = &buf
	out, err := cmd.CombinedOutput()
	if err != nil || !strings.Contains(string(out), "PROM-OK") {
		t.Fatalf("official parser rejected the exposition: %v\n%s", err, out)
	}
}
