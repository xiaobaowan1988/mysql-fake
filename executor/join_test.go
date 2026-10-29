package executor

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"mysqlfake/tuple"
)

func rowsOfInts(rows [][]int64) [][]tuple.Value {
	out := make([][]tuple.Value, len(rows))
	for i, r := range rows {
		for _, v := range r {
			if v == -1 {
				out[i] = append(out[i], tuple.NullValue(tuple.TypeInt))
			} else {
				out[i] = append(out[i], tuple.IntValue(v))
			}
		}
	}
	return out
}

func drain(t *testing.T, e Executor) []string {
	t.Helper()
	if err := e.Init(); err != nil {
		t.Fatal(err)
	}
	var out []string
	for {
		r, err := e.Next()
		if err != nil {
			t.Fatal(err)
		}
		if r == nil {
			break
		}
		out = append(out, fmt.Sprint(r.Values))
	}
	sort.Strings(out)
	return out
}

func sortedBy(rows [][]tuple.Value, col int) [][]tuple.Value {
	cp := append([][]tuple.Value(nil), rows...)
	sort.SliceStable(cp, func(i, j int) bool { return tuple.Compare(cp[i][col], cp[j][col]) < 0 })
	return cp
}

// 三种 Join 算法在随机数据 (含 NULL 与重复键) 上必须得到相同结果。
func TestJoinAlgorithmsAgree(t *testing.T) {
	rng := rand.New(rand.NewSource(5))
	for round := 0; round < 30; round++ {
		var l, r [][]int64
		for i := 0; i < rng.Intn(40)+1; i++ {
			k := int64(rng.Intn(8))
			if rng.Intn(10) == 0 {
				k = -1 // NULL
			}
			l = append(l, []int64{k, int64(i)})
		}
		for i := 0; i < rng.Intn(40)+1; i++ {
			k := int64(rng.Intn(8))
			if rng.Intn(10) == 0 {
				k = -1
			}
			r = append(r, []int64{int64(i), k})
		}
		L, R := rowsOfInts(l), rowsOfInts(r)
		// l.0 = r.1  (拼接后行: l0 l1 r0 r1 -> 下标 0 与 3)
		pred := &cmpExpr{op: "=", l: &colExpr{0}, r: &colExpr{3}}
		nl := drain(t, NewNestedLoopJoin(&RowsExec{Rows: L}, &RowsExec{Rows: R}, pred))
		hj := drain(t, NewHashJoin(&RowsExec{Rows: L}, &RowsExec{Rows: R}, []int{0}, []int{1}, nil))
		mj := drain(t, NewMergeJoin(&RowsExec{Rows: sortedBy(L, 0)}, &RowsExec{Rows: sortedBy(R, 1)}, 0, 1, nil))
		if fmt.Sprint(nl) != fmt.Sprint(hj) || fmt.Sprint(nl) != fmt.Sprint(mj) {
			t.Fatalf("round %d disagree:\nnl=%v\nhj=%v\nmj=%v", round, nl, hj, mj)
		}
	}
}

func TestJoinResidualAndCross(t *testing.T) {
	L := rowsOfInts([][]int64{{1, 10}, {1, 20}, {2, 30}})
	R := rowsOfInts([][]int64{{1, 5}, {1, 25}, {3, 1}})
	// 额外条件: l.1 < r.1  (拼接下标 1 与 3)
	resid := &cmpExpr{op: "<", l: &colExpr{1}, r: &colExpr{3}}
	hj := drain(t, NewHashJoin(&RowsExec{Rows: L}, &RowsExec{Rows: R}, []int{0}, []int{0}, resid))
	mj := drain(t, NewMergeJoin(&RowsExec{Rows: L}, &RowsExec{Rows: sortedBy(R, 0)}, 0, 0, resid))
	if len(hj) != 2 || fmt.Sprint(hj) != fmt.Sprint(mj) { // (1,10)-(1,25) 和 (1,20)-(1,25)
		t.Fatalf("hj=%v mj=%v", hj, mj)
	}
	cross := drain(t, NewNestedLoopJoin(&RowsExec{Rows: L}, &RowsExec{Rows: R}, nil))
	if len(cross) != 9 {
		t.Fatalf("cross product has %d rows", len(cross))
	}
}

func TestSortAndLimit(t *testing.T) {
	rows := rowsOfInts([][]int64{{3, 1}, {1, 2}, {-1, 3}, {2, 4}, {1, 5}})
	s := NewSort(&RowsExec{Rows: rows}, []SortKey{{Idx: 0}, {Idx: 1, Desc: true}})
	if err := s.Init(); err != nil {
		t.Fatal(err)
	}
	var got []string
	for r, _ := s.Next(); r != nil; r, _ = s.Next() {
		got = append(got, fmt.Sprint(r.Values))
	}
	want := "[[NULL 3] [1 5] [1 2] [2 4] [3 1]]"
	if fmt.Sprint(got) != want {
		t.Fatalf("got %v", got)
	}
	l := NewLimit(NewSort(&RowsExec{Rows: rows}, []SortKey{{Idx: 0, Desc: true}}), 2)
	if out := drainUnsorted(t, l); fmt.Sprint(out) != "[[3 1] [2 4]]" {
		t.Fatalf("limit: %v", out)
	}
	if out := drainUnsorted(t, NewLimit(&RowsExec{Rows: rows}, 0)); len(out) != 0 {
		t.Fatal("limit 0")
	}
}

func drainUnsorted(t *testing.T, e Executor) []string {
	t.Helper()
	e.Init()
	var out []string
	for r, _ := e.Next(); r != nil; r, _ = e.Next() {
		out = append(out, fmt.Sprint(r.Values))
	}
	return out
}
