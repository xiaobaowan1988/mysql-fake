package db

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"mysqlfake/optimizer"
)

func vecEngine(t testing.TB, n int) *Engine {
	e, err := Open(t.TempDir() + "/vec.db")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { e.Close() })
	mustExecTB(t, e, "CREATE TABLE f (id INT PRIMARY KEY, a INT, b INT, s VARCHAR(20))")
	mustExecTB(t, e, "CREATE TABLE d (id INT PRIMARY KEY, g INT)")
	for start := 0; start < n; start += 500 {
		var vals []string
		for i := start; i < n && i < start+500; i++ {
			vals = append(vals, fmt.Sprintf("(%d,%d,%d,'name%d')", i, i%1000, (i*7)%100, i%50))
		}
		mustExecTB(t, e, "INSERT INTO f VALUES "+strings.Join(vals, ","))
	}
	var dv []string
	for i := 0; i < 1000; i++ {
		dv = append(dv, fmt.Sprintf("(%d,%d)", i, i%10))
	}
	mustExecTB(t, e, "INSERT INTO d VALUES "+strings.Join(dv, ","))
	return e
}

func mustExecTB(t testing.TB, e *Engine, q string) *Result {
	t.Helper()
	r, err := e.Exec(q)
	if err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	return r
}

var vecQueries = []string{
	"SELECT id FROM f WHERE a > 500 AND b < 40",
	"SELECT id, s FROM f WHERE s = 'name7' OR (a < 10 AND b >= 90)",
	"SELECT id FROM f WHERE NOT (a >= 100) AND b IS NOT NULL",
	"SELECT f.id, d.g FROM f JOIN d ON f.a = d.id WHERE d.g = 3 AND f.b < 20",
	"SELECT f.id FROM f, d WHERE f.a = d.id AND d.g < 2 ORDER BY f.id DESC LIMIT 25",
	"SELECT id FROM f WHERE a + b > 150",
}

func TestVectorizedEqualsRowMode(t *testing.T) {
	e := vecEngine(t, 6000)
	mustExec(t, e, "UPDATE f SET b = NULL WHERE id < 100") // 带 NULL
	mustExec(t, e, "DELETE FROM f WHERE id >= 5900")       // 删除标记行不可见
	for _, q := range vecQueries {
		e.SetPlannerOptions(optimizer.Options{RowMode: true})
		want := rowSet(mustExec(t, e, q))
		e.SetPlannerOptions(optimizer.Options{})
		got := rowSet(mustExec(t, e, q))
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("%s\nvectorized %d rows vs row mode %d rows", q, len(got), len(want))
		}
		if len(want) == 0 && !strings.Contains(q, "id < 0") {
			t.Fatalf("query returned nothing: %s", q)
		}
	}
	// 向量化在事务快照下同样遵守 MVCC
	r, w := e.NewSession(), e.NewSession()
	sexec(t, r, "BEGIN")
	before := rowSet(sexec(t, r, "SELECT id FROM f WHERE a = 5"))
	sexec(t, w, "DELETE FROM f WHERE a = 5")
	after := rowSet(sexec(t, r, "SELECT id FROM f WHERE a = 5"))
	if fmt.Sprint(before) != fmt.Sprint(after) || len(before) == 0 {
		t.Fatal("vectorized scan violated snapshot isolation")
	}
}

func timeQuery(e *Engine, q string, rowMode bool, reps int) time.Duration {
	e.SetPlannerOptions(optimizer.Options{RowMode: rowMode})
	best := time.Hour
	for i := 0; i < reps; i++ {
		start := time.Now()
		e.Exec(q)
		if d := time.Since(start); d < best {
			best = d
		}
	}
	return best
}

func TestVectorizedIsFasterOnScanFilterAndJoin(t *testing.T) {
	if testing.Short() || raceEnabled {
		t.Skip("timing test (meaningless with -short or the race detector)")
	}
	e := vecEngine(t, 60000)
	e.Exec("ANALYZE TABLE f")
	e.Exec("ANALYZE TABLE d")
	for _, q := range []string{
		"SELECT id FROM f WHERE a > 500 AND b < 40",
		"SELECT f.id, d.g FROM f JOIN d ON f.a = d.id WHERE d.g = 3",
	} {
		row := timeQuery(e, q, true, 4)
		vec := timeQuery(e, q, false, 4)
		t.Logf("%-70s row=%-10v vectorized=%-10v speedup=%.2fx", q, row, vec, float64(row)/float64(vec))
		if vec > row*3/2 { // 宽松的下限，避免共享机器上计时抖动造成误报
			t.Errorf("vectorized execution is much slower than row mode for %q", q)
		}
	}
}

func BenchmarkScanFilter(b *testing.B) {
	e := vecEngine(b, 60000)
	for _, mode := range []struct {
		name string
		row  bool
	}{{"row", true}, {"vectorized", false}} {
		b.Run(mode.name, func(b *testing.B) {
			e.SetPlannerOptions(optimizer.Options{RowMode: mode.row})
			for i := 0; i < b.N; i++ {
				e.Exec("SELECT id FROM f WHERE a > 500 AND b < 40")
			}
		})
	}
}
