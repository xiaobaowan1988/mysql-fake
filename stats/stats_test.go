package stats

import (
	"fmt"
	"math"
	"math/rand"
	"testing"

	"mysqlfake/tuple"
)

func ints(vs ...int64) []tuple.Value {
	out := make([]tuple.Value, len(vs))
	for i, v := range vs {
		out[i] = tuple.IntValue(v)
	}
	return out
}

func actual(vals []int64, pred func(int64) bool) float64 {
	n := 0
	for _, v := range vals {
		if pred(v) {
			n++
		}
	}
	return float64(n) / float64(len(vals))
}

func qerr(est, act float64) float64 {
	est, act = math.Max(est, 1e-4), math.Max(act, 1e-4)
	if est > act {
		return est / act
	}
	return act / est
}

func toValues(vs []int64) []tuple.Value {
	out := make([]tuple.Value, len(vs))
	for i, v := range vs {
		out[i] = tuple.IntValue(v)
	}
	return out
}

func TestUniformUsesEquiWidth(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	vs := make([]int64, 20000)
	for i := range vs {
		vs[i] = rng.Int63n(1000)
	}
	cs := BuildColumn(toValues(vs), len(vs), tuple.TypeInt)
	if cs.Hist == nil || cs.Hist.Kind != EquiWidth {
		t.Fatalf("uniform INT data should use an equi-width histogram, got %+v", cs.Hist)
	}
	for _, c := range []int64{100, 500, 900} {
		for _, op := range []string{"<", "<=", ">", ">="} {
			est := cs.SelRange(op, tuple.IntValue(c))
			act := actual(vs, func(v int64) bool {
				switch op {
				case "<":
					return v < c
				case "<=":
					return v <= c
				case ">":
					return v > c
				}
				return v >= c
			})
			if qerr(est, act) > 1.2 {
				t.Errorf("x %s %d: est %.4f actual %.4f", op, c, est, act)
			}
		}
	}
}

// 倾斜数据 (Zipf): 等高直方图 + MCV 必须把热点值估得很准，而不是被"均匀假设"拉平。
func TestSkewedDataMCVAndEquiDepth(t *testing.T) {
	rng := rand.New(rand.NewSource(2))
	z := rand.NewZipf(rng, 1.3, 2, 5000)
	vs := make([]int64, 50000)
	for i := range vs {
		vs[i] = int64(z.Uint64())
	}
	cs := BuildColumn(toValues(vs), len(vs), tuple.TypeInt)
	if cs.Hist == nil || cs.Hist.Kind != EquiDepth {
		t.Fatalf("skewed data should use equi-depth, got %+v", cs.Hist)
	}
	if len(cs.MCV) == 0 {
		t.Fatal("expected MCVs for a skewed column")
	}
	// 热点值
	for _, hot := range []int64{0, 1, 2} {
		act := actual(vs, func(v int64) bool { return v == hot })
		est := cs.SelEq(tuple.IntValue(hot))
		if qerr(est, act) > 1.05 {
			t.Errorf("hot value %d: est %.4f actual %.4f", hot, est, act)
		}
	}
	// 相比"均匀假设" 1/NDV，估计要好得多
	uniform := 1 / cs.NDV
	act0 := actual(vs, func(v int64) bool { return v == 0 })
	if qerr(uniform, act0) < 5 {
		t.Fatalf("test data not skewed enough: uniform=%.5f act=%.5f", uniform, act0)
	}
	// 范围谓词
	for _, c := range []int64{1, 10, 100, 1000} {
		act := actual(vs, func(v int64) bool { return v <= c })
		est := cs.SelRange("<=", tuple.IntValue(c))
		if qerr(est, act) > 1.3 {
			t.Errorf("x <= %d: est %.4f actual %.4f", c, est, act)
		}
	}
}

func TestSampleScaling(t *testing.T) {
	rng := rand.New(rand.NewSource(3))
	const N = 200000
	vs := make([]int64, N)
	for i := range vs {
		vs[i] = rng.Int63n(2000)
	}
	// 抽样 5000 行，NDV 估计应接近真实值 2000
	sm := NewSampler(5000, 42)
	schema := &tuple.Schema{Columns: []tuple.Column{{Name: "A", Type: tuple.TypeInt}}}
	for _, v := range vs {
		sm.Add([]tuple.Value{tuple.IntValue(v)}, 9)
	}
	ts := sm.Build(schema)
	if ts.Rows != N || ts.Sampled != 5000 || ts.RowBytes != 9 {
		t.Fatalf("%+v", ts)
	}
	cs := ts.Col("a")
	if cs == nil || cs.NDV < 1500 || cs.NDV > 2600 {
		t.Fatalf("NDV estimate off: %v", cs.NDV)
	}
	act := actual(vs, func(v int64) bool { return v < 500 })
	if qerr(cs.SelRange("<", tuple.IntValue(500)), act) > 1.15 {
		t.Fatal("range estimate from sample too far off")
	}
	// 相同种子可复现
	sm2 := NewSampler(5000, 42)
	for _, v := range vs {
		sm2.Add([]tuple.Value{tuple.IntValue(v)}, 9)
	}
	if sm2.Build(schema).Col("A").NDV != cs.NDV {
		t.Fatal("sampling is not deterministic for a fixed seed")
	}
}

func TestVarcharAndNulls(t *testing.T) {
	var vals []tuple.Value
	for i := 0; i < 1000; i++ {
		switch {
		case i%10 == 0:
			vals = append(vals, tuple.NullValue(tuple.TypeVarchar))
		case i%3 == 0:
			vals = append(vals, tuple.StrValue("popular"))
		default:
			vals = append(vals, tuple.StrValue(fmt.Sprintf("user%04d", i)))
		}
	}
	cs := BuildColumn(vals, len(vals), tuple.TypeVarchar)
	if math.Abs(cs.NullFrac-0.1) > 1e-9 {
		t.Fatalf("null frac %v", cs.NullFrac)
	}
	if got := cs.SelEq(tuple.StrValue("popular")); math.Abs(got-0.3) > 0.03 {
		t.Fatalf("popular=%v", got)
	}
	if cs.SelEq(tuple.NullValue(tuple.TypeVarchar)) != 0 {
		t.Fatal("= NULL never matches")
	}
	lt := cs.SelRange("<", tuple.StrValue("user0500"))
	// "popular" (30%) 排在 "user..." 之前，加上约一半的 user 值: 约 0.6
	if lt < 0.5 || lt > 0.7 {
		t.Fatalf("string range selectivity implausible: %v", lt)
	}
	if cs.SelEq(tuple.StrValue("zzz-not-there")) != 0 {
		t.Fatal("value beyond max must have ~0 selectivity")
	}
}

func TestStoreNeedsAnalyze(t *testing.T) {
	s := NewStore()
	if !s.NeedsAnalyze("T") {
		t.Fatal("never analyzed")
	}
	s.Set("t", &TableStats{Rows: 10000})
	if s.NeedsAnalyze("T") {
		t.Fatal("fresh stats")
	}
	s.AddMods("t", 500)
	if s.NeedsAnalyze("t") {
		t.Fatal("5% modified should not trigger")
	}
	s.AddMods("t", 600)
	if !s.NeedsAnalyze("t") {
		t.Fatal("11% modified should trigger")
	}
	s.Set("t", &TableStats{Rows: 10000})
	if s.NeedsAnalyze("t") {
		t.Fatal("Set resets the counter")
	}
}

func TestEmptyAndSingleValue(t *testing.T) {
	cs := BuildColumn(nil, 0, tuple.TypeInt)
	if cs.SelEq(tuple.IntValue(1)) < 0 {
		t.Fatal()
	}
	one := BuildColumn(ints(7, 7, 7, 7), 4, tuple.TypeInt)
	if got := one.SelEq(tuple.IntValue(7)); got < 0.99 {
		t.Fatalf("got %v", got)
	}
	if one.SelEq(tuple.IntValue(8)) != 0 {
		t.Fatal("absent value")
	}
}
