package vector

import (
	"fmt"
	"math/rand"
	"testing"

	"mysqlfake/executor"
	"mysqlfake/sql"
	"mysqlfake/tuple"
)

var testTypes = []tuple.Type{tuple.TypeInt, tuple.TypeInt, tuple.TypeInt, tuple.TypeVarchar, tuple.TypeVarchar}

func testResolver(_, name string) (int, tuple.Type, error) {
	var i int
	if _, err := fmt.Sscanf(name, "c%d", &i); err != nil || i >= len(testTypes) {
		return 0, 0, fmt.Errorf("unknown column %s", name)
	}
	return i, testTypes[i], nil
}

func randBatch(rng *rand.Rand, n int) *Batch {
	cols := newCols(testTypes, n)
	for i := 0; i < n; i++ {
		for c, t := range testTypes {
			if rng.Intn(6) == 0 {
				cols[c].AppendValue(tuple.NullValue(t))
			} else if t == tuple.TypeInt {
				cols[c].AppendValue(tuple.IntValue(int64(rng.Intn(6))))
			} else {
				cols[c].AppendValue(tuple.StrValue(fmt.Sprintf("s%d", rng.Intn(4))))
			}
		}
	}
	return &Batch{Cols: cols, N: n}
}

func randLit(rng *rand.Rand, t tuple.Type) sql.Expr {
	if rng.Intn(12) == 0 {
		return &sql.Literal{Value: tuple.Value{Null: true}}
	}
	if t == tuple.TypeInt {
		return &sql.Literal{Value: tuple.IntValue(int64(rng.Intn(6)))}
	}
	return &sql.Literal{Value: tuple.StrValue(fmt.Sprintf("s%d", rng.Intn(4)))}
}

func col(i int) *sql.ColumnRef { return &sql.ColumnRef{Name: fmt.Sprintf("c%d", i)} }

var cmpOps = []string{"=", "!=", "<", "<=", ">", ">="}

func randPred(rng *rand.Rand, depth int) sql.Expr {
	if depth > 0 {
		switch rng.Intn(5) {
		case 0:
			return &sql.BinaryExpr{Op: "AND", Left: randPred(rng, depth-1), Right: randPred(rng, depth-1)}
		case 1:
			return &sql.BinaryExpr{Op: "OR", Left: randPred(rng, depth-1), Right: randPred(rng, depth-1)}
		case 2:
			return &sql.NotExpr{Expr: randPred(rng, depth-1)}
		}
	}
	op := cmpOps[rng.Intn(len(cmpOps))]
	switch rng.Intn(6) {
	case 0:
		c := rng.Intn(len(testTypes))
		return &sql.IsNullExpr{Expr: col(c), Not: rng.Intn(2) == 0}
	case 1: // 列 op 字面量
		c := rng.Intn(len(testTypes))
		return &sql.BinaryExpr{Op: op, Left: col(c), Right: randLit(rng, testTypes[c])}
	case 2: // 字面量 op 列
		c := rng.Intn(len(testTypes))
		return &sql.BinaryExpr{Op: op, Left: randLit(rng, testTypes[c]), Right: col(c)}
	case 3: // 列 op 列 (同类型)
		if rng.Intn(2) == 0 {
			return &sql.BinaryExpr{Op: op, Left: col(rng.Intn(3)), Right: col(rng.Intn(3))}
		}
		return &sql.BinaryExpr{Op: op, Left: col(3 + rng.Intn(2)), Right: col(3 + rng.Intn(2))}
	case 4: // 算术 -> 兜底路径
		return &sql.BinaryExpr{Op: op,
			Left:  &sql.BinaryExpr{Op: []string{"+", "-", "*"}[rng.Intn(3)], Left: col(rng.Intn(3)), Right: &sql.Literal{Value: tuple.IntValue(int64(rng.Intn(4)))}},
			Right: col(rng.Intn(3))}
	}
	return &sql.BinaryExpr{Op: op, Left: &sql.Literal{Value: tuple.IntValue(1)}, Right: &sql.Literal{Value: tuple.IntValue(int64(rng.Intn(3)))}}
}

// 核函数与行式求值器在三值逻辑下必须完全一致 (TRUE 集合与 FALSE 集合都要一致)。
func TestKernelsMatchRowEvaluator(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	for round := 0; round < 3000; round++ {
		e := randPred(rng, 3)
		bound, err := executor.BindPredicate(e, testResolver)
		if err != nil {
			t.Fatalf("bind %s: %v", sql.ExprString(e), err)
		}
		k, err := Compile(e, testResolver)
		if err != nil {
			t.Fatalf("compile %s: %v", sql.ExprString(e), err)
		}
		b := randBatch(rng, 1+rng.Intn(300))
		// 也测试带选择向量的输入 (随机子集)
		sel := b.Active()
		if rng.Intn(2) == 0 {
			var sub []int32
			for _, i := range sel {
				if rng.Intn(3) != 0 {
					sub = append(sub, i)
				}
			}
			sel = sub
		}
		tt, ff := k.Eval(b, sel)
		var wantT, wantF []int32
		for _, i := range sel {
			v := bound.Eval(b.Row(int(i)))
			switch {
			case v.Null:
			case v.Int != 0:
				wantT = append(wantT, i)
			default:
				wantF = append(wantF, i)
			}
		}
		if fmt.Sprint(tt) != fmt.Sprint(wantT) || fmt.Sprint(ff) != fmt.Sprint(wantF) {
			t.Fatalf("round %d: %s\n T got %v want %v\n F got %v want %v", round, sql.ExprString(e), tt, wantT, ff, wantF)
		}
	}
}

func TestSetOps(t *testing.T) {
	a, b := []int32{1, 3, 5, 7}, []int32{3, 4, 7, 9}
	if fmt.Sprint(intersect(a, b)) != "[3 7]" || fmt.Sprint(union(a, b)) != "[1 3 4 5 7 9]" || fmt.Sprint(diff(a, b)) != "[1 5]" {
		t.Fatal("set ops")
	}
	if len(intersect(nil, b)) != 0 || fmt.Sprint(diff(a, nil)) != "[1 3 5 7]" {
		t.Fatal("empty cases")
	}
}

type sliceBatches struct {
	batches []*Batch
	pos     int
}

func (s *sliceBatches) Init() error { s.pos = 0; return nil }
func (s *sliceBatches) NextBatch() (*Batch, error) {
	if s.pos >= len(s.batches) {
		return nil, nil
	}
	s.pos++
	return s.batches[s.pos-1], nil
}

func drainRows(t *testing.T, e BatchExecutor) []string {
	t.Helper()
	r := NewToRows(e)
	if err := r.Init(); err != nil {
		t.Fatal(err)
	}
	var out []string
	for {
		row, err := r.Next()
		if err != nil {
			t.Fatal(err)
		}
		if row == nil {
			return out
		}
		out = append(out, fmt.Sprint(row.Values))
	}
}

func TestFilterProjectLimitPipeline(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	src := &sliceBatches{batches: []*Batch{randBatch(rng, 100), randBatch(rng, 50), randBatch(rng, 300)}}
	e, _ := sql.Parse("SELECT * FROM x WHERE c0 > 2 AND c3 != 's1'")
	k, err := CompileAll(sql.SplitAnd(e.(*sql.SelectStmt).Where), testResolver)
	if err != nil {
		t.Fatal(err)
	}
	all := drainRows(t, NewProject(NewFilter(src, k), []int{3, 0}))
	// 与行式参考实现对比
	bound, _ := executor.BindPredicate(e.(*sql.SelectStmt).Where, testResolver)
	var want []string
	for _, b := range src.batches {
		for i := 0; i < b.N; i++ {
			if executor.EvalBool(bound, b.Row(i)) {
				r := b.Row(i)
				want = append(want, fmt.Sprint([]tuple.Value{r[3], r[0]}))
			}
		}
	}
	if fmt.Sprint(all) != fmt.Sprint(want) || len(want) == 0 {
		t.Fatalf("pipeline mismatch: %d vs %d rows", len(all), len(want))
	}
	lim := drainRows(t, NewLimit(NewFilter(src, k), 7))
	if len(lim) != 7 || fmt.Sprint(lim) != fmt.Sprint(drainRows(t, NewProject(NewFilter(src, k), []int{0, 1, 2, 3, 4}))[:7]) {
		t.Fatalf("limit: %d rows", len(lim))
	}
}

// 向量化 Hash Join 必须与行式 Hash Join 结果一致 (含 NULL 键、重复键、单键快速路径与多键路径、超过一批的输出)。
func TestHashJoinMatchesRowHashJoin(t *testing.T) {
	rng := rand.New(rand.NewSource(3))
	for round := 0; round < 40; round++ {
		lb := []*Batch{randBatch(rng, 1+rng.Intn(400)), randBatch(rng, 1+rng.Intn(400))}
		rb := []*Batch{randBatch(rng, 1+rng.Intn(100))}
		keySets := [][2][]int{{{0}, {1}}, {{3}, {4}}, {{0, 3}, {1, 4}}, {{2}, {2}}}
		ks := keySets[round%len(keySets)]
		vec := NewHashJoin(&sliceBatches{batches: lb}, &sliceBatches{batches: rb}, ks[0], ks[1], testTypes, testTypes)
		got := drainRows(t, vec)

		var lrows, rrows [][]tuple.Value
		for _, b := range lb {
			for i := 0; i < b.N; i++ {
				lrows = append(lrows, b.Row(i))
			}
		}
		for _, b := range rb {
			for i := 0; i < b.N; i++ {
				rrows = append(rrows, b.Row(i))
			}
		}
		row := executor.NewHashJoin(&executor.RowsExec{Rows: lrows}, &executor.RowsExec{Rows: rrows}, ks[0], ks[1], nil)
		if err := row.Init(); err != nil {
			t.Fatal(err)
		}
		var want []string
		for r, _ := row.Next(); r != nil; r, _ = row.Next() {
			want = append(want, fmt.Sprint(r.Values))
		}
		if len(got) != len(want) {
			t.Fatalf("round %d keys %v: %d rows vs %d", round, ks, len(got), len(want))
		}
		// 两者输出顺序都是 "按左行顺序、右侧按构建顺序"，应逐行相同
		for i := range got {
			if got[i] != want[i] {
				t.Fatalf("round %d row %d differs:\n%s\n%s", round, i, got[i], want[i])
			}
		}
	}
}

func TestFromRowsRoundTrip(t *testing.T) {
	var rows [][]tuple.Value
	for i := 0; i < 2500; i++ { // 跨多个批
		rows = append(rows, []tuple.Value{tuple.IntValue(int64(i)), tuple.StrValue(fmt.Sprint(i)), tuple.NullValue(tuple.TypeInt)})
	}
	types := []tuple.Type{tuple.TypeInt, tuple.TypeVarchar, tuple.TypeInt}
	out := drainRows(t, NewFromRows(&executor.RowsExec{Rows: rows}, types))
	if len(out) != 2500 || out[1234] != fmt.Sprint(rows[1234]) {
		t.Fatalf("round trip failed: %d rows", len(out))
	}
}

// 纯表达式求值的对比 (不含存储层开销): 向量化核函数 vs 逐行 Value 求值。
func BenchmarkPredicate(b *testing.B) {
	rng := rand.New(rand.NewSource(1))
	batch := randBatch(rng, BatchSize)
	e, _ := sql.Parse("SELECT * FROM x WHERE c0 > 2 AND c1 < 4 AND c3 != 's1'")
	where := e.(*sql.SelectStmt).Where
	k, _ := CompileAll(sql.SplitAnd(where), testResolver)
	bound, _ := executor.BindPredicate(where, testResolver)
	b.Run("vectorized", func(b *testing.B) {
		sel := batch.Active()
		for i := 0; i < b.N; i++ {
			k.Eval(batch, sel)
		}
		b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/BatchSize, "ns/row")
	})
	b.Run("row", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			for r := 0; r < batch.N; r++ {
				executor.EvalBool(bound, batch.Row(r))
			}
		}
		b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/BatchSize, "ns/row")
	})
}
