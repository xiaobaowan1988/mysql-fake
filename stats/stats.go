// Package stats 保存表/列的统计信息 (行数、NDV、高频值 MCV、直方图)，并提供选择率估算。
// 直方图支持等宽 (INT 且分布接近均匀时) 和等高 (有数据倾斜时)；高频值单独存放，使倾斜值的估算接近精确。
package stats

import (
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"mysqlfake/tuple"
)

type HistKind int

const (
	EquiDepth HistKind = iota // 等高: 每个桶行数大致相同
	EquiWidth                 // 等宽: 每个桶值域宽度相同 (仅 INT)
)

func (k HistKind) String() string {
	if k == EquiWidth {
		return "equi-width"
	}
	return "equi-depth"
}

const (
	maxBuckets = 32
	maxMCV     = 10
)

type MCV struct {
	Val  tuple.Value
	Freq float64 // 占全表行数的比例
}

type Bucket struct {
	Lo, Hi tuple.Value // 闭区间
	Frac   float64     // 占全表行数的比例
	NDV    float64
}

type Histogram struct {
	Kind    HistKind
	Buckets []Bucket
}

type ColumnStats struct {
	Type     tuple.Type
	NullFrac float64
	NDV      float64
	Min, Max tuple.Value
	MCV      []MCV
	Hist     *Histogram
	HistFrac float64 // 直方图覆盖的行比例 (非空且不在 MCV 里)
}

type TableStats struct {
	Rows       float64
	RowBytes   float64
	Sampled    int
	AnalyzedAt time.Time
	Cols       map[string]*ColumnStats // 键为小写列名
}

func (t *TableStats) Col(name string) *ColumnStats {
	if t == nil {
		return nil
	}
	return t.Cols[strings.ToLower(name)]
}

// ---- 采样与构建 ----

// Sampler 用蓄水池抽样保存最多 k 行。
type Sampler struct {
	k     int
	rows  [][]tuple.Value
	seen  int
	bytes float64
	state uint64
}

func NewSampler(k int, seed uint64) *Sampler {
	return &Sampler{k: k, state: seed*2685821657736338717 + 1}
}

func (s *Sampler) rnd(n int) int { // xorshift，保证给定种子可复现
	s.state ^= s.state << 13
	s.state ^= s.state >> 7
	s.state ^= s.state << 17
	return int(s.state % uint64(n))
}

// Add 记录一行 (rowBytes 是该行序列化后的大小)。
func (s *Sampler) Add(row []tuple.Value, rowBytes int) {
	s.seen++
	s.bytes += float64(rowBytes)
	if len(s.rows) < s.k {
		s.rows = append(s.rows, append([]tuple.Value(nil), row...))
		return
	}
	if j := s.rnd(s.seen); j < s.k {
		s.rows[j] = append([]tuple.Value(nil), row...)
	}
}

// Build 根据样本生成表统计信息。
func (s *Sampler) Build(schema *tuple.Schema) *TableStats {
	ts := &TableStats{Rows: float64(s.seen), Sampled: len(s.rows), AnalyzedAt: time.Now(), Cols: map[string]*ColumnStats{}}
	if s.seen > 0 {
		ts.RowBytes = s.bytes / float64(s.seen)
	}
	for i, c := range schema.Columns {
		vals := make([]tuple.Value, len(s.rows))
		for j, r := range s.rows {
			vals[j] = r[i]
		}
		ts.Cols[strings.ToLower(c.Name)] = BuildColumn(vals, s.seen, c.Type)
	}
	return ts
}

// BuildColumn 根据列的样本值 (含 NULL) 构建列统计，total 为全表行数。
func BuildColumn(sample []tuple.Value, total int, typ tuple.Type) *ColumnStats {
	cs := &ColumnStats{Type: typ}
	n := len(sample)
	if n == 0 {
		return cs
	}
	var vals []tuple.Value
	for _, v := range sample {
		if !v.Null {
			vals = append(vals, v)
		}
	}
	cs.NullFrac = float64(n-len(vals)) / float64(n)
	if len(vals) == 0 {
		return cs
	}
	sort.Slice(vals, func(i, j int) bool { return tuple.Compare(vals[i], vals[j]) < 0 })
	cs.Min, cs.Max = vals[0], vals[len(vals)-1]

	type group struct {
		v tuple.Value
		c int
	}
	var groups []group
	for _, v := range vals {
		if k := len(groups); k > 0 && tuple.Compare(groups[k-1].v, v) == 0 {
			groups[k-1].c++
		} else {
			groups = append(groups, group{v, 1})
		}
	}
	d := len(groups)
	f1 := 0
	for _, g := range groups {
		if g.c == 1 {
			f1++
		}
	}
	// NDV: 样本即全表时精确；否则用 Haas-Stokes 的 Duj1 估计量 n·d / (n - f1 + f1·n/N)
	nonNullTotal := float64(total) * (1 - cs.NullFrac)
	if n >= total {
		cs.NDV = float64(d)
	} else {
		nn, dd, ff, NN := float64(len(vals)), float64(d), float64(f1), nonNullTotal
		cs.NDV = nn * dd / (nn - ff + ff*nn/math.Max(NN, nn))
		cs.NDV = math.Min(math.Max(cs.NDV, dd), math.Max(NN, 1))
	}

	// MCV: 频率明显高于平均的值，最多 maxMCV 个
	avg := float64(len(vals)) / float64(d)
	var cands []group
	for _, g := range groups {
		if g.c >= 2 && float64(g.c) > 1.25*avg {
			cands = append(cands, g)
		}
	}
	sort.SliceStable(cands, func(i, j int) bool { return cands[i].c > cands[j].c })
	if len(cands) > maxMCV {
		cands = cands[:maxMCV]
	}
	isMCV := map[string]bool{}
	for _, g := range cands {
		cs.MCV = append(cs.MCV, MCV{Val: g.v, Freq: float64(g.c) / float64(n)})
		isMCV[valKey(g.v)] = true
	}
	sort.Slice(cs.MCV, func(i, j int) bool { return tuple.Compare(cs.MCV[i].Val, cs.MCV[j].Val) < 0 })

	var rest []group
	restN := 0
	for _, g := range groups {
		if !isMCV[valKey(g.v)] {
			rest = append(rest, g)
			restN += g.c
		}
	}
	if restN == 0 {
		return cs
	}
	cs.HistFrac = float64(restN) / float64(n)
	flat := make([]tuple.Value, 0, restN)
	for _, g := range rest {
		for i := 0; i < g.c; i++ {
			flat = append(flat, g.v)
		}
	}
	cs.Hist = buildHistogram(flat, typ, float64(n))
	return cs
}

func valKey(v tuple.Value) string {
	if v.Type == tuple.TypeInt {
		return "i" + strconv.FormatInt(v.Int, 10)
	}
	return "s" + v.Str
}

// buildHistogram: INT 且分布接近均匀 (最大桶不超过平均的 2 倍) 用等宽，否则用等高。
func buildHistogram(sorted []tuple.Value, typ tuple.Type, n float64) *Histogram {
	if typ == tuple.TypeInt {
		if h := tryEquiWidth(sorted, n); h != nil {
			return h
		}
	}
	return buildEquiDepth(sorted, n)
}

func tryEquiWidth(sorted []tuple.Value, n float64) *Histogram {
	lo, hi := sorted[0].Int, sorted[len(sorted)-1].Int
	span := float64(hi) - float64(lo) + 1
	if span < 2 || span > 1e15 {
		return nil
	}
	b := maxBuckets
	if int(span) < b {
		b = int(span)
	}
	w := int64(math.Ceil(span / float64(b)))
	counts := make([]int, b)
	ndv := make([]map[int64]bool, b)
	for _, v := range sorted {
		i := int((v.Int - lo) / w)
		if i >= b {
			i = b - 1
		}
		counts[i]++
		if ndv[i] == nil {
			ndv[i] = map[int64]bool{}
		}
		ndv[i][v.Int] = true
	}
	avg := float64(len(sorted)) / float64(b)
	for _, c := range counts {
		if float64(c) > 2*avg {
			return nil // 有倾斜
		}
	}
	h := &Histogram{Kind: EquiWidth}
	for i := 0; i < b; i++ {
		l := lo + int64(i)*w
		hh := l + w - 1
		if hh > hi {
			hh = hi
		}
		h.Buckets = append(h.Buckets, Bucket{Lo: tuple.IntValue(l), Hi: tuple.IntValue(hh), Frac: float64(counts[i]) / n, NDV: float64(len(ndv[i]))})
	}
	return h
}

func buildEquiDepth(sorted []tuple.Value, n float64) *Histogram {
	b := maxBuckets
	if len(sorted) < b {
		b = len(sorted)
	}
	target := float64(len(sorted)) / float64(b)
	h := &Histogram{Kind: EquiDepth}
	start := 0
	for start < len(sorted) {
		end := start + int(math.Ceil(target))
		if end > len(sorted) {
			end = len(sorted)
		}
		for end < len(sorted) && tuple.Compare(sorted[end], sorted[end-1]) == 0 { // 相同的值不跨桶
			end++
		}
		nd := 1
		for i := start + 1; i < end; i++ {
			if tuple.Compare(sorted[i], sorted[i-1]) != 0 {
				nd++
			}
		}
		h.Buckets = append(h.Buckets, Bucket{Lo: sorted[start], Hi: sorted[end-1], Frac: float64(end-start) / n, NDV: float64(nd)})
		start = end
	}
	return h
}

// ---- 选择率估算 (结果为占全表行数的比例) ----

func (c *ColumnStats) nonNull() float64 { return 1 - c.NullFrac }

// SelEq 估算 col = v。
func (c *ColumnStats) SelEq(v tuple.Value) float64 {
	if c == nil {
		return 0.05
	}
	if v.Null {
		return 0
	}
	for _, m := range c.MCV {
		if tuple.Compare(m.Val, v) == 0 {
			return m.Freq
		}
	}
	if c.Hist != nil {
		for _, b := range c.Hist.Buckets {
			if tuple.Compare(v, b.Lo) >= 0 && tuple.Compare(v, b.Hi) <= 0 {
				return b.Frac / math.Max(b.NDV, 1)
			}
		}
		return 0 // 落在所有桶之外 (且不是 MCV): 样本中没有
	}
	mcvSum := 0.0
	for _, m := range c.MCV {
		mcvSum += m.Freq
	}
	rest := math.Max(c.NDV-float64(len(c.MCV)), 1)
	return math.Max(c.nonNull()-mcvSum, 0) / rest
}

// toFloat 把值映射成可插值的数: INT 原样，VARCHAR 取前 8 字节当作 256 进制小数。
func toFloat(v tuple.Value) float64 {
	if v.Type == tuple.TypeInt {
		return float64(v.Int)
	}
	f, scale := 0.0, 1.0
	for i := 0; i < 8 && i < len(v.Str); i++ {
		scale /= 256
		f += float64(v.Str[i]) * scale
	}
	return f
}

// below 估算桶内小于 (或小于等于) v 的比例。
func (b Bucket) below(v tuple.Value, inclusive bool) float64 {
	cmpLo, cmpHi := tuple.Compare(v, b.Lo), tuple.Compare(v, b.Hi)
	switch {
	case cmpHi > 0 || (cmpHi == 0 && inclusive):
		return 1
	case cmpLo < 0 || (cmpLo == 0 && !inclusive):
		return 0
	}
	if v.Type == tuple.TypeInt {
		w := float64(b.Hi.Int-b.Lo.Int) + 1
		x := float64(v.Int - b.Lo.Int)
		if inclusive {
			x++
		}
		return math.Min(math.Max(x/w, 0), 1)
	}
	lo, hi, x := toFloat(b.Lo), toFloat(b.Hi), toFloat(v)
	if hi <= lo {
		return 0.5
	}
	return math.Min(math.Max((x-lo)/(hi-lo), 0), 1)
}

// SelLess 估算 col < v (inclusive 时为 col <= v)，不含 NULL。
func (c *ColumnStats) SelLess(v tuple.Value, inclusive bool) float64 {
	if c == nil {
		return 1.0 / 3
	}
	if v.Null {
		return 0
	}
	s := 0.0
	for _, m := range c.MCV {
		if r := tuple.Compare(m.Val, v); r < 0 || (r == 0 && inclusive) {
			s += m.Freq
		}
	}
	if c.Hist != nil {
		for _, b := range c.Hist.Buckets {
			s += b.Frac * b.below(v, inclusive)
		}
	}
	return math.Min(s, c.nonNull())
}

// SelRange 估算 col op v，op 为 < <= > >=。
func (c *ColumnStats) SelRange(op string, v tuple.Value) float64 {
	if c == nil {
		return 1.0 / 3
	}
	if v.Null {
		return 0
	}
	var s float64
	switch op {
	case "<":
		s = c.SelLess(v, false)
	case "<=":
		s = c.SelLess(v, true)
	case ">":
		s = c.nonNull() - c.SelLess(v, true)
	case ">=":
		s = c.nonNull() - c.SelLess(v, false)
	default:
		return 1.0 / 3
	}
	return math.Min(math.Max(s, 0), 1)
}

// ---- 存储 ----

// Store 保存各表的统计信息和自上次分析以来的修改计数。
type Store struct {
	mu     sync.RWMutex
	tables map[string]*TableStats
	mods   map[string]int64
}

func NewStore() *Store {
	return &Store{tables: map[string]*TableStats{}, mods: map[string]int64{}}
}

func (s *Store) Get(table string) *TableStats {
	if s == nil {
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.tables[strings.ToLower(table)]
}

func (s *Store) Set(table string, ts *TableStats) {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := strings.ToLower(table)
	s.tables[k] = ts
	s.mods[k] = 0
}

// Mods 返回自上次采样以来累计的修改数。
func (s *Store) Mods(table string) int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.mods[strings.ToLower(table)]
}

// SetSampled 保存一次采样的结果。modsAtStart 是采样开始前读到的修改数: 只扣掉这部分
// (它们已经反映在这次采样里)，采样期间新到达的修改保留下来，仍会触发下一次重新采样。
// (Set 直接清零，会把采样期间的修改一并丢掉。)
func (s *Store) SetSampled(table string, ts *TableStats, modsAtStart int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := strings.ToLower(table)
	s.tables[k] = ts
	if rest := s.mods[k] - modsAtStart; rest > 0 {
		s.mods[k] = rest
	} else {
		s.mods[k] = 0
	}
}

func (s *Store) AddMods(table string, n int64) {
	s.mu.Lock()
	s.mods[strings.ToLower(table)] += n
	s.mu.Unlock()
}

// NeedsAnalyze: 从未分析过，或修改量超过 max(50, 10% 行数)。
func (s *Store) NeedsAnalyze(table string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	k := strings.ToLower(table)
	ts := s.tables[k]
	if ts == nil {
		return true
	}
	threshold := math.Max(50, ts.Rows*0.1)
	return float64(s.mods[k]) > threshold
}

// Invalidate 丢弃表的统计信息 (表结构变化后使用)。
func (s *Store) Invalidate(table string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := strings.ToLower(table)
	delete(s.tables, k)
	delete(s.mods, k)
}
