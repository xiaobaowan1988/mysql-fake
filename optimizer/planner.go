package optimizer

import (
	"fmt"
	"math"
	"math/bits"
	"sort"
	"strings"

	"mysqlfake/catalog"
	"mysqlfake/executor"
	"mysqlfake/sql"
	"mysqlfake/stats"
	"mysqlfake/tuple"
	"mysqlfake/vector"
)

// 代价常数 (单位: 一次顺序页读取 = 1)。
const (
	ioCost        = 1.0
	cpuTuple      = 0.01   // 处理一行
	cpuOp         = 0.0025 // 一次比较/谓词求值/哈希探测
	cpuHashBuild  = 0.02   // 往哈希表插入一行
	pageBytes     = 4000.0
	defaultRows   = 1000.0
	defaultRowLen = 100.0
	maxDPRels     = 10 // 超过则只枚举左深树
)

type TableLookup interface {
	GetTable(name string) (*catalog.TableInfo, error)
}

type StatsProvider interface {
	Get(table string) *stats.TableStats
}

type Options struct {
	ForceJoin string // "" | "nl" | "hash" | "merge": 强制 Join 算法 (测试/提示用)
	NoReorder bool   // 保持 FROM 的顺序 (左深)
	RowMode   bool   // 使用行式执行器 (默认使用向量化执行器)
}

type Planner struct {
	Cat   TableLookup
	Stats StatsProvider
	Opts  Options
}

type edge struct {
	conj sql.Expr
	l, r colID
	mask uint32
}

type pred struct {
	conj sql.Expr
	mask uint32
	sel  float64
}

type query struct {
	pl     *Planner
	st     *sql.SelectStmt
	rels   []*rel
	byName map[string]int
	ts     []*stats.TableStats

	local  [][]sql.Expr // 每个表上的单表谓词
	edges  []edge
	preds  []pred // 跨表的非等值谓词
	consts []sql.Expr

	cardMemo map[uint32]float64
	fRows    []float64 // 每个表应用本地谓词之后的行数
}

// PlanSelect 为 SELECT 生成最优计划并构建执行器。
func (pl *Planner) PlanSelect(ctx *executor.Ctx, st *sql.SelectStmt) (*Plan, error) {
	q := &query{pl: pl, st: st, byName: map[string]int{}, cardMemo: map[uint32]float64{}}
	if len(st.From) == 0 || len(st.From) > 30 {
		return nil, fmt.Errorf("unsupported FROM clause")
	}
	for i, ref := range st.From {
		ti, err := pl.Cat.GetTable(ref.Name)
		if err != nil {
			return nil, err
		}
		b := strings.ToLower(ref.Binding())
		if _, dup := q.byName[b]; dup {
			return nil, fmt.Errorf("not unique table/alias: '%s'", ref.Binding())
		}
		q.byName[b] = i
		var ts *stats.TableStats
		if pl.Stats != nil {
			ts = pl.Stats.Get(ti.Name)
		}
		rows := defaultRows
		if ts != nil {
			rows = math.Max(ts.Rows, 1)
		}
		q.rels = append(q.rels, &rel{idx: i, ref: ref, ti: ti, rows: rows})
		q.ts = append(q.ts, ts)
	}
	if err := q.classify(); err != nil {
		return nil, err
	}
	root, err := q.search()
	if err != nil {
		return nil, err
	}
	top, cols, err := q.finish(root)
	if err != nil {
		return nil, err
	}
	var ex executor.Executor
	if pl.Opts.RowMode {
		ex, err = q.buildRow(ctx, top)
	} else {
		var v vector.BatchExecutor
		if v, err = q.buildVec(ctx, top); err == nil {
			ex = vector.NewToRows(v)
		}
	}
	if err != nil {
		return nil, err
	}
	return &Plan{Root: ex, Columns: cols, Tree: top, Rows: top.Rows, Cost: top.Cost}, nil
}

// ---- 名字解析与谓词分类 ----

func (q *query) resolve(c *sql.ColumnRef) (colID, error) {
	if c.Table != "" {
		ri, ok := q.byName[strings.ToLower(c.Table)]
		if !ok {
			return colID{}, fmt.Errorf("unknown table '%s'", c.Table)
		}
		ci := q.rels[ri].ti.Schema.ColumnIndex(c.Name)
		if ci < 0 {
			return colID{}, fmt.Errorf("unknown column '%s.%s'", c.Table, c.Name)
		}
		return colID{ri, ci}, nil
	}
	found := colID{-1, -1}
	for ri, r := range q.rels {
		if ci := r.ti.Schema.ColumnIndex(c.Name); ci >= 0 {
			if found.rel >= 0 {
				return colID{}, fmt.Errorf("column '%s' is ambiguous", c.Name)
			}
			found = colID{ri, ci}
		}
	}
	if found.rel < 0 {
		return colID{}, fmt.Errorf("unknown column '%s'", c.Name)
	}
	return found, nil
}

func (q *query) maskOf(e sql.Expr) (uint32, error) {
	switch e := e.(type) {
	case *sql.ColumnRef:
		id, err := q.resolve(e)
		if err != nil {
			return 0, err
		}
		return 1 << id.rel, nil
	case *sql.BinaryExpr:
		l, err := q.maskOf(e.Left)
		if err != nil {
			return 0, err
		}
		r, err := q.maskOf(e.Right)
		return l | r, err
	case *sql.NotExpr:
		return q.maskOf(e.Expr)
	case *sql.IsNullExpr:
		return q.maskOf(e.Expr)
	}
	return 0, nil
}

func (q *query) classify() error {
	q.local = make([][]sql.Expr, len(q.rels))
	for _, c := range sql.SplitAnd(q.st.Where) {
		// 先做类型检查: 用一个能解析全部列的临时解析器绑定一次
		if _, err := executor.BindPredicate(c, q.typeResolver()); err != nil {
			return err
		}
		m, err := q.maskOf(c)
		if err != nil {
			return err
		}
		switch bits.OnesCount32(m) {
		case 0:
			q.consts = append(q.consts, c)
		case 1:
			q.local[bits.TrailingZeros32(m)] = append(q.local[bits.TrailingZeros32(m)], c)
		default:
			if e, ok := q.asEquiEdge(c, m); ok {
				q.edges = append(q.edges, e)
			} else {
				q.preds = append(q.preds, pred{conj: c, mask: m, sel: 0.3})
			}
		}
	}
	// SELECT / ORDER BY 中的列也要能解析
	for i := range q.st.Items {
		if _, err := q.resolve(&q.st.Items[i]); err != nil {
			return err
		}
	}
	for i := range q.st.OrderBy {
		if _, err := q.resolve(&q.st.OrderBy[i].Col); err != nil {
			return err
		}
	}
	q.fRows = make([]float64, len(q.rels))
	for i, r := range q.rels {
		s := 1.0
		for _, c := range q.local[i] {
			s *= q.selectivity(c, i)
		}
		q.fRows[i] = math.Max(1, r.rows*s)
	}
	return nil
}

// typeResolver 用于校验 WHERE: 解析任意列并返回类型，位置无关紧要。
func (q *query) typeResolver() executor.Resolver {
	return func(table, name string) (int, tuple.Type, error) {
		id, err := q.resolve(&sql.ColumnRef{Table: table, Name: name})
		if err != nil {
			return 0, 0, err
		}
		return 0, q.rels[id.rel].ti.Schema.Columns[id.col].Type, nil
	}
}

func (q *query) asEquiEdge(c sql.Expr, m uint32) (edge, bool) {
	b, ok := c.(*sql.BinaryExpr)
	if !ok || b.Op != "=" {
		return edge{}, false
	}
	lc, ok1 := b.Left.(*sql.ColumnRef)
	rc, ok2 := b.Right.(*sql.ColumnRef)
	if !ok1 || !ok2 {
		return edge{}, false
	}
	l, err1 := q.resolve(lc)
	r, err2 := q.resolve(rc)
	if err1 != nil || err2 != nil || l.rel == r.rel {
		return edge{}, false
	}
	return edge{conj: c, l: l, r: r, mask: m}, true
}

// ---- 选择率与基数 ----

func (q *query) colStats(id colID) *stats.ColumnStats {
	if q.ts[id.rel] == nil {
		return nil
	}
	return q.ts[id.rel].Col(q.rels[id.rel].ti.Schema.Columns[id.col].Name)
}

func (q *query) isPK(id colID) bool { return id.col == q.rels[id.rel].ti.PK }

// selectivity 估算单表谓词 (只引用表 ri 的列) 的选择率。
func (q *query) selectivity(e sql.Expr, ri int) float64 {
	clamp := func(s float64) float64 { return math.Min(math.Max(s, 0), 1) }
	switch e := e.(type) {
	case *sql.BinaryExpr:
		switch e.Op {
		case "AND":
			return clamp(q.selectivity(e.Left, ri) * q.selectivity(e.Right, ri))
		case "OR":
			a, b := q.selectivity(e.Left, ri), q.selectivity(e.Right, ri)
			return clamp(a + b - a*b)
		}
		col, lit, op := splitColLit(e)
		if col == nil {
			if e.Op == "=" {
				return 0.1
			}
			return 1.0 / 3
		}
		id, err := q.resolve(col)
		if err != nil {
			return 0.3
		}
		cs := q.colStats(id)
		switch op {
		case "=":
			if cs == nil {
				if q.isPK(id) {
					return 1 / q.rels[ri].rows
				}
				return 0.05
			}
			return clamp(cs.SelEq(lit.Value))
		case "!=":
			if cs == nil {
				return 0.95
			}
			return clamp(1 - cs.NullFrac - cs.SelEq(lit.Value))
		default:
			return clamp(cs.SelRange(op, lit.Value))
		}
	case *sql.NotExpr:
		return clamp(1 - q.selectivity(e.Expr, ri))
	case *sql.IsNullExpr:
		s := 0.01
		if c, ok := e.Expr.(*sql.ColumnRef); ok {
			if id, err := q.resolve(c); err == nil {
				if cs := q.colStats(id); cs != nil {
					s = cs.NullFrac
				}
			}
		}
		if e.Not {
			return clamp(1 - s)
		}
		return clamp(s)
	}
	return 0.3
}

// splitColLit 识别 "列 op 字面量" / "字面量 op 列" 并统一成列在左。
func splitColLit(b *sql.BinaryExpr) (*sql.ColumnRef, *sql.Literal, string) {
	if c, ok := b.Left.(*sql.ColumnRef); ok {
		if l, ok := b.Right.(*sql.Literal); ok && !l.Value.Null {
			return c, l, b.Op
		}
	}
	if c, ok := b.Right.(*sql.ColumnRef); ok {
		if l, ok := b.Left.(*sql.Literal); ok && !l.Value.Null {
			return c, l, flip(b.Op)
		}
	}
	return nil, nil, ""
}

func flip(op string) string {
	switch op {
	case "<":
		return ">"
	case "<=":
		return ">="
	case ">":
		return "<"
	case ">=":
		return "<="
	}
	return op
}

func (q *query) ndv(id colID) float64 {
	n := q.rels[id.rel].rows
	if cs := q.colStats(id); cs != nil && cs.NDV > 0 {
		n = cs.NDV
	} else if !q.isPK(id) {
		n = math.Max(n/10, 1)
	}
	return math.Max(math.Min(n, q.fRows[id.rel]), 1) // 过滤后不可能比行数还多
}

// card 估算子集 mask 的连接结果行数 (与枚举顺序无关，因此可以记忆化)。
func (q *query) card(mask uint32) float64 {
	if v, ok := q.cardMemo[mask]; ok {
		return v
	}
	c := 1.0
	for i := range q.rels {
		if mask&(1<<i) != 0 {
			c *= q.fRows[i]
		}
	}
	for _, e := range q.edges {
		if e.mask&mask == e.mask {
			c /= math.Max(q.ndv(e.l), q.ndv(e.r))
		}
	}
	for _, p := range q.preds {
		if p.mask&mask == p.mask {
			c *= p.sel
		}
	}
	c = math.Max(c, 1)
	q.cardMemo[mask] = c
	return c
}

// ---- 访问路径 ----

func (q *query) scanNode(ri int) *Node {
	r := q.rels[ri]
	rowLen := defaultRowLen
	if q.ts[ri] != nil && q.ts[ri].RowBytes > 0 {
		rowLen = q.ts[ri].RowBytes
	}
	rowsPerPage := math.Max(pageBytes/(rowLen+10), 1)
	pages := math.Ceil(r.rows / rowsPerPage)
	nf := float64(len(q.local[ri]))
	cols := make([]colID, len(r.ti.Schema.Columns))
	for i := range cols {
		cols[i] = colID{ri, i}
	}
	pk := colID{ri, r.ti.PK}
	n := &Node{
		Op: "SeqScan", Detail: describeRel(r), rel: r, cols: cols, order: &pk,
		Rows: q.fRows[ri], filters: q.local[ri],
		Cost: pages*ioCost + r.rows*cpuTuple + r.rows*nf*cpuOp,
	}
	if len(q.local[ri]) > 0 {
		n.Detail += " filter: " + joinExprs(q.local[ri])
	}
	// 主键范围
	where := andOf(q.local[ri])
	if where == nil {
		return n
	}
	pkName := r.ti.Schema.Columns[r.ti.PK].Name
	start, end, ok := executor.PKRange(where, r.ti, func(c *sql.ColumnRef) bool {
		id, err := q.resolve(c)
		return err == nil && id.rel == ri && strings.EqualFold(c.Name, pkName)
	})
	if !ok {
		return n
	}
	sel := 1.0
	for _, c := range q.local[ri] {
		if q.onlyPK(c, ri) {
			sel *= q.selectivity(c, ri)
		}
	}
	idxRows := math.Max(1, r.rows*sel)
	height := math.Max(1, math.Ceil(math.Log(pages+1)/math.Log(200))+1)
	cost := height*ioCost + math.Ceil(idxRows/rowsPerPage)*ioCost + idxRows*(cpuTuple+nf*cpuOp)
	if cost < n.Cost {
		return &Node{
			Op: "IndexScan", rel: r, cols: cols, order: &pk, start: start, end: end, filters: q.local[ri],
			Rows: q.fRows[ri], Cost: cost,
			Detail: describeRel(r) + " pk-range " + describeRange(r, start, end) + " filter: " + joinExprs(q.local[ri]),
		}
	}
	return n
}

func (q *query) onlyPK(e sql.Expr, ri int) bool {
	ok := true
	var walk func(e sql.Expr)
	walk = func(e sql.Expr) {
		switch e := e.(type) {
		case *sql.ColumnRef:
			if id, err := q.resolve(e); err != nil || id.rel != ri || !q.isPK(id) {
				ok = false
			}
		case *sql.BinaryExpr:
			walk(e.Left)
			walk(e.Right)
		case *sql.NotExpr:
			walk(e.Expr)
		case *sql.IsNullExpr:
			walk(e.Expr)
		}
	}
	walk(e)
	return ok
}

func describeRel(r *rel) string {
	if r.ref.Alias != "" && !strings.EqualFold(r.ref.Alias, r.ref.Name) {
		return r.ref.Alias + " [" + r.ti.Name + "]"
	}
	return r.ti.Name
}

func describeRange(r *rel, start, end []byte) string {
	f := func(b []byte) string {
		if b == nil {
			return "-inf/+inf"
		}
		if v, err := tuple.DecodeKey(r.ti.Schema.Columns[r.ti.PK].Type, b); err == nil {
			return v.String()
		}
		return fmt.Sprintf("%x", b)
	}
	lo, hi := "(-inf", "+inf)"
	if start != nil {
		lo = "[" + f(start)
	}
	if end != nil {
		hi = f(end) + ")"
	}
	return lo + "," + hi
}

func andOf(es []sql.Expr) sql.Expr {
	var out sql.Expr
	for _, e := range es {
		if out == nil {
			out = e
		} else {
			out = &sql.BinaryExpr{Op: "AND", Left: out, Right: e}
		}
	}
	return out
}

func joinExprs(es []sql.Expr) string {
	var parts []string
	for _, e := range es {
		parts = append(parts, sql.ExprString(e))
	}
	return strings.Join(parts, " AND ")
}

// ---- Join 枚举 (动态规划) ----

func (q *query) search() (*Node, error) {
	n := len(q.rels)
	full := uint32(1)<<n - 1
	for _, allowCross := range []bool{false, true} {
		best := map[uint32][]*Node{}
		for i := range q.rels {
			best[1<<i] = []*Node{q.scanNode(i)}
		}
		leftDeep := n > maxDPRels || q.pl.Opts.NoReorder
		for size := 2; size <= n; size++ {
			for mask := uint32(1); mask <= full; mask++ {
				if bits.OnesCount32(mask) != size {
					continue
				}
				var cands []*Node
				for s := (mask - 1) & mask; s > 0; s = (s - 1) & mask {
					r := mask ^ s
					if leftDeep && bits.OnesCount32(r) != 1 {
						continue
					}
					if q.pl.Opts.NoReorder && r != 1<<(size-1) { // 严格按 FROM 顺序
						continue
					}
					if len(best[s]) == 0 || len(best[r]) == 0 {
						continue
					}
					es, ps := q.joinPreds(s, r)
					if len(es) == 0 && len(ps) == 0 && !allowCross {
						continue
					}
					for _, lp := range best[s] {
						for _, rp := range best[r] {
							cands = append(cands, q.joinCandidates(lp, rp, mask, es, ps)...)
						}
					}
				}
				best[mask] = prune(cands)
			}
		}
		if len(best[full]) > 0 {
			return q.pickFinal(best[full]), nil
		}
	}
	return nil, fmt.Errorf("optimizer: no plan found")
}

// joinPreds 返回把 s 与 r 连接时新变得可用的等值边和其他谓词。
func (q *query) joinPreds(s, r uint32) (es []edge, ps []pred) {
	both := s | r
	for _, e := range q.edges {
		if e.mask&both == e.mask && e.mask&s != 0 && e.mask&r != 0 {
			// 统一方向: l 在 s 一侧
			if s&(1<<e.l.rel) == 0 {
				e.l, e.r = e.r, e.l
			}
			es = append(es, e)
		}
	}
	for _, p := range q.preds {
		if p.mask&both == p.mask && p.mask&s != 0 && p.mask&r != 0 {
			ps = append(ps, p)
		}
	}
	return
}

func sameOrder(a, b *colID) bool { return a != nil && b != nil && *a == *b }

// prune: 保留代价最低的计划，以及每种"有用有序性"下代价最低的计划；被更便宜且有序性不差的计划支配的丢弃。
func prune(cands []*Node) []*Node {
	sort.SliceStable(cands, func(i, j int) bool { return cands[i].Cost < cands[j].Cost })
	var kept []*Node
	for _, c := range cands {
		dominated := false
		for _, k := range kept {
			if k.Cost <= c.Cost && (c.order == nil || sameOrder(k.order, c.order)) {
				dominated = true
				break
			}
		}
		if !dominated && len(kept) < 4 {
			kept = append(kept, c)
		}
	}
	return kept
}

func (q *query) pickFinal(cands []*Node) *Node {
	best, bestCost := cands[0], math.Inf(1)
	for _, c := range cands {
		cost := c.Cost + q.orderByCost(c)
		if cost < bestCost {
			best, bestCost = c, cost
		}
	}
	return best
}

// orderByCost: ORDER BY 需要额外排序的代价 (输入已有序则为 0)。
func (q *query) orderByCost(n *Node) float64 {
	if len(q.st.OrderBy) == 0 || q.orderSatisfied(n) {
		return 0
	}
	return sortCost(n.Rows)
}

func sortCost(rows float64) float64 { return rows*math.Log2(math.Max(rows, 2))*cpuOp*2 + rows*cpuTuple }

func (q *query) orderSatisfied(n *Node) bool {
	if len(q.st.OrderBy) != 1 || q.st.OrderBy[0].Desc || n.order == nil {
		return false
	}
	id, err := q.resolve(&q.st.OrderBy[0].Col)
	return err == nil && id == *n.order
}

func (q *query) joinCandidates(lp, rp *Node, mask uint32, es []edge, ps []pred) []*Node {
	out := q.card(mask)
	cols := append(append([]colID(nil), lp.cols...), rp.cols...)
	var residual []sql.Expr
	for _, p := range ps {
		residual = append(residual, p.conj)
	}
	force := q.pl.Opts.ForceJoin
	var res []*Node
	base := lp.Cost + rp.Cost

	if (force == "" || force == "nl") || len(es) == 0 {
		var all []sql.Expr
		for _, e := range es {
			all = append(all, e.conj)
		}
		all = append(all, residual...)
		n := &Node{
			Op: "NestedLoop", Rows: out, cols: cols, order: lp.order, Children: []*Node{lp, rp}, residual: all,
			Cost: base + lp.Rows*rp.Rows*cpuOp*float64(1+len(all)) + out*cpuTuple,
		}
		if len(all) > 0 {
			n.Detail = "on " + joinExprs(all)
		} else {
			n.Detail = "(cross product)"
		}
		res = append(res, n)
	}
	if len(es) > 0 && (force == "" || force == "hash") {
		n := &Node{
			Op: "HashJoin", Rows: out, cols: cols, order: lp.order, Children: []*Node{lp, rp}, residual: residual,
			Cost: base + rp.Rows*cpuHashBuild + lp.Rows*cpuOp*4 + out*cpuTuple,
		}
		for _, e := range es {
			n.lkeys = append(n.lkeys, e.l)
			n.rkeys = append(n.rkeys, e.r)
		}
		n.Detail = "build=right on " + keyString(es)
		res = append(res, n)
	}
	if len(es) > 0 && (force == "" || force == "merge") {
		e0 := es[0]
		n := &Node{
			Op: "MergeJoin", Rows: out, cols: cols, Children: []*Node{lp, rp},
			lkeys: []colID{e0.l}, rkeys: []colID{e0.r},
		}
		n.sortL = !sameOrder(lp.order, &e0.l)
		n.sortR = !sameOrder(rp.order, &e0.r)
		cost := base + (lp.Rows+rp.Rows)*cpuOp + out*cpuTuple
		if n.sortL {
			cost += sortCost(lp.Rows)
		}
		if n.sortR {
			cost += sortCost(rp.Rows)
		}
		n.Cost = cost
		lo := e0.l
		n.order = &lo
		for _, e := range es[1:] {
			n.residual = append(n.residual, e.conj)
		}
		n.residual = append(n.residual, residual...)
		n.Detail = "on " + sql.ExprString(e0.conj)
		if n.sortL || n.sortR {
			n.Detail += fmt.Sprintf(" sort=[left:%v right:%v]", n.sortL, n.sortR)
		}
		res = append(res, n)
	}
	return res
}

func keyString(es []edge) string {
	var parts []string
	for _, e := range es {
		parts = append(parts, sql.ExprString(e.conj))
	}
	return strings.Join(parts, " AND ")
}

// ---- 顶层算子: 常量过滤 / ORDER BY / LIMIT / 投影 ----

func (q *query) finish(root *Node) (*Node, []string, error) {
	top := root
	if len(q.consts) > 0 { // 与表无关的常量谓词 (例如 1 = 0)
		top = &Node{Op: "Filter", Detail: joinExprs(q.consts), Rows: root.Rows, Cost: root.Cost,
			cols: root.cols, order: root.order, Children: []*Node{root}, residual: q.consts}
	}
	if len(q.st.OrderBy) > 0 && !q.orderSatisfied(top) {
		var keys []executor.SortKey
		var desc []string
		for _, o := range q.st.OrderBy {
			id, _ := q.resolve(&o.Col)
			keys = append(keys, executor.SortKey{Idx: indexOf(top.cols, id), Desc: o.Desc})
			s := sql.ExprString(&o.Col)
			if o.Desc {
				s += " DESC"
			}
			desc = append(desc, s)
		}
		top = &Node{Op: "Sort", Detail: strings.Join(desc, ", "), Rows: top.Rows, Cost: top.Cost + sortCost(top.Rows),
			cols: top.cols, Children: []*Node{top}, sortKeys: keys}
	}
	if q.st.Limit >= 0 {
		rows := math.Min(top.Rows, float64(q.st.Limit))
		top = &Node{Op: "Limit", Detail: fmt.Sprint(q.st.Limit), Rows: rows, Cost: top.Cost, cols: top.cols,
			order: top.order, Children: []*Node{top}, limit: q.st.Limit}
	}
	var proj []colID
	var names []string
	if q.st.Star {
		for ri, r := range q.rels {
			for ci, c := range r.ti.Schema.Columns {
				proj = append(proj, colID{ri, ci})
				names = append(names, c.Name)
			}
		}
	} else {
		for i := range q.st.Items {
			id, _ := q.resolve(&q.st.Items[i])
			proj = append(proj, id)
			names = append(names, q.rels[id.rel].ti.Schema.Columns[id.col].Name)
		}
	}
	top = &Node{Op: "Project", Detail: strings.Join(names, ", "), Rows: top.Rows, Cost: top.Cost, cols: proj,
		Children: []*Node{top}, projCols: proj}
	return top, names, nil
}

func indexOf(cols []colID, id colID) int {
	for i, c := range cols {
		if c == id {
			return i
		}
	}
	return -1
}

// ---- 构建执行器 ----

func (q *query) layoutResolver(cols []colID) executor.Resolver {
	return func(table, name string) (int, tuple.Type, error) {
		id, err := q.resolve(&sql.ColumnRef{Table: table, Name: name})
		if err != nil {
			return 0, 0, err
		}
		i := indexOf(cols, id)
		if i < 0 {
			return 0, 0, fmt.Errorf("optimizer: column %s.%s not in layout", table, name)
		}
		return i, q.rels[id.rel].ti.Schema.Columns[id.col].Type, nil
	}
}

func (q *query) bindAll(es []sql.Expr, cols []colID) (executor.Expr, error) {
	var bound []executor.Expr
	for _, e := range es {
		b, err := executor.BindPredicate(e, q.layoutResolver(cols))
		if err != nil {
			return nil, err
		}
		bound = append(bound, b)
	}
	return executor.And(bound), nil
}

// childRow 构建子节点的行式执行器 (向量化模式下把批执行器适配成行)。
func (q *query) childRow(ctx *executor.Ctx, c *Node) (executor.Executor, error) {
	if q.pl.Opts.RowMode {
		return q.buildRow(ctx, c)
	}
	v, err := q.buildVec(ctx, c)
	if err != nil {
		return nil, err
	}
	return vector.NewToRows(v), nil
}

func (q *query) typesOf(cols []colID) []tuple.Type {
	out := make([]tuple.Type, len(cols))
	for i, c := range cols {
		out[i] = q.rels[c.rel].ti.Schema.Columns[c.col].Type
	}
	return out
}

// buildVec 构建向量化执行器: 扫描/过滤/哈希连接/LIMIT/投影原生向量化，其余算子 (嵌套循环、归并连接、排序)
// 用行式实现并通过适配器接入。
func (q *query) buildVec(ctx *executor.Ctx, n *Node) (vector.BatchExecutor, error) {
	filter := func(child vector.BatchExecutor, es []sql.Expr, cols []colID) (vector.BatchExecutor, error) {
		if len(es) == 0 {
			return child, nil
		}
		k, err := vector.CompileAll(es, q.layoutResolver(cols))
		if err != nil {
			return nil, err
		}
		return vector.NewFilter(child, k), nil
	}
	switch n.Op {
	case "SeqScan", "IndexScan":
		var start, end []byte
		if n.Op == "IndexScan" {
			start, end = n.start, n.end
		}
		return filter(vector.NewScan(ctx, n.rel.ti, start, end), n.filters, n.cols)
	case "Filter":
		child, err := q.buildVec(ctx, n.Children[0])
		if err != nil {
			return nil, err
		}
		return filter(child, n.residual, n.cols)
	case "HashJoin":
		l, err := q.buildVec(ctx, n.Children[0])
		if err != nil {
			return nil, err
		}
		r, err := q.buildVec(ctx, n.Children[1])
		if err != nil {
			return nil, err
		}
		lc, rc := n.Children[0].cols, n.Children[1].cols
		var lk, rk []int
		for i := range n.lkeys {
			lk = append(lk, indexOf(lc, n.lkeys[i]))
			rk = append(rk, indexOf(rc, n.rkeys[i]))
		}
		var join vector.BatchExecutor = vector.NewHashJoin(l, r, lk, rk, q.typesOf(lc), q.typesOf(rc))
		return filter(join, n.residual, n.cols)
	case "Limit":
		child, err := q.buildVec(ctx, n.Children[0])
		if err != nil {
			return nil, err
		}
		return vector.NewLimit(child, n.limit), nil
	case "Project":
		child, err := q.buildVec(ctx, n.Children[0])
		if err != nil {
			return nil, err
		}
		in := n.Children[0].cols
		idx := make([]int, len(n.projCols))
		for i, id := range n.projCols {
			idx[i] = indexOf(in, id)
		}
		return vector.NewProject(child, idx), nil
	}
	row, err := q.buildRow(ctx, n)
	if err != nil {
		return nil, err
	}
	return vector.NewFromRows(row, q.typesOf(n.cols)), nil
}

func (q *query) buildRow(ctx *executor.Ctx, n *Node) (executor.Executor, error) {
	switch n.Op {
	case "SeqScan", "IndexScan":
		var scan executor.Executor
		if n.Op == "SeqScan" {
			scan = executor.NewSeqScan(ctx, n.rel.ti)
		} else {
			scan = executor.NewIndexScan(ctx, n.rel.ti, n.start, n.end)
		}
		pred, err := q.bindAll(n.filters, n.cols)
		if err != nil {
			return nil, err
		}
		if pred != nil {
			scan = executor.NewFilter(scan, pred)
		}
		return scan, nil
	case "Filter":
		child, err := q.childRow(ctx, n.Children[0])
		if err != nil {
			return nil, err
		}
		pred, err := q.bindAll(n.residual, n.cols)
		if err != nil {
			return nil, err
		}
		return executor.NewFilter(child, pred), nil
	case "NestedLoop", "HashJoin", "MergeJoin":
		l, err := q.childRow(ctx, n.Children[0])
		if err != nil {
			return nil, err
		}
		r, err := q.childRow(ctx, n.Children[1])
		if err != nil {
			return nil, err
		}
		resid, err := q.bindAll(n.residual, n.cols)
		if err != nil {
			return nil, err
		}
		lc, rc := n.Children[0].cols, n.Children[1].cols
		switch n.Op {
		case "NestedLoop":
			return executor.NewNestedLoopJoin(l, r, resid), nil
		case "HashJoin":
			var lk, rk []int
			for i := range n.lkeys {
				lk = append(lk, indexOf(lc, n.lkeys[i]))
				rk = append(rk, indexOf(rc, n.rkeys[i]))
			}
			return executor.NewHashJoin(l, r, lk, rk, resid), nil
		default:
			lk, rk := indexOf(lc, n.lkeys[0]), indexOf(rc, n.rkeys[0])
			if n.sortL {
				l = executor.NewSort(l, []executor.SortKey{{Idx: lk}})
			}
			if n.sortR {
				r = executor.NewSort(r, []executor.SortKey{{Idx: rk}})
			}
			return executor.NewMergeJoin(l, r, lk, rk, resid), nil
		}
	case "Sort":
		child, err := q.childRow(ctx, n.Children[0])
		if err != nil {
			return nil, err
		}
		return executor.NewSort(child, n.sortKeys), nil
	case "Limit":
		child, err := q.childRow(ctx, n.Children[0])
		if err != nil {
			return nil, err
		}
		return executor.NewLimit(child, n.limit), nil
	case "Project":
		child, err := q.childRow(ctx, n.Children[0])
		if err != nil {
			return nil, err
		}
		in := n.Children[0].cols
		idx := make([]int, len(n.projCols))
		for i, id := range n.projCols {
			idx[i] = indexOf(in, id)
		}
		return executor.NewProjection(child, idx), nil
	}
	return nil, fmt.Errorf("optimizer: unknown operator %s", n.Op)
}
