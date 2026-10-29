package executor

import (
	"bytes"
	"fmt"
	"strings"

	"mysqlfake/catalog"
	"mysqlfake/sql"
	"mysqlfake/tuple"
	"mysqlfake/txn"
)

type kind int

const (
	kInt kind = iota
	kStr
	kBool
	kNull
)

func kindOf(t tuple.Type) kind {
	if t == tuple.TypeInt {
		return kInt
	}
	return kStr
}

// BindExpr 校验 AST 表达式中的列是否存在、类型是否匹配，并生成可求值的表达式。
func BindExpr(e sql.Expr, s *tuple.Schema) (Expr, error) {
	return BindPredicate(e, schemaResolver(s))
}

// Resolver 把 (表限定名, 列名) 解析为输入行中的位置和类型。
type Resolver func(table, name string) (idx int, t tuple.Type, err error)

func schemaResolver(s *tuple.Schema) Resolver {
	return func(_, name string) (int, tuple.Type, error) {
		i := s.ColumnIndex(name)
		if i < 0 {
			return 0, 0, fmt.Errorf("unknown column '%s'", name)
		}
		return i, s.Columns[i].Type, nil
	}
}

// BindPredicate 在给定的列解析规则下绑定一个布尔表达式。
func BindPredicate(e sql.Expr, res Resolver) (Expr, error) {
	be, k, err := bindExpr(e, res)
	if err == nil && k != kBool {
		return nil, fmt.Errorf("WHERE clause must be a boolean expression")
	}
	return be, err
}

func bind(e sql.Expr, s *tuple.Schema) (Expr, kind, error) { return bindExpr(e, schemaResolver(s)) }

func bindExpr(e sql.Expr, res Resolver) (Expr, kind, error) {
	switch e := e.(type) {
	case *sql.Literal:
		if e.Value.Null {
			return &litExpr{e.Value}, kNull, nil
		}
		return &litExpr{e.Value}, kindOf(e.Value.Type), nil
	case *sql.ColumnRef:
		i, t, err := res(e.Table, e.Name)
		if err != nil {
			return nil, 0, err
		}
		return &colExpr{i}, kindOf(t), nil
	case *sql.NotExpr:
		x, k, err := bindExpr(e.Expr, res)
		if err != nil {
			return nil, 0, err
		}
		if k != kBool {
			return nil, 0, fmt.Errorf("NOT requires a boolean expression")
		}
		return &notExpr{x}, kBool, nil
	case *sql.IsNullExpr:
		x, _, err := bindExpr(e.Expr, res)
		if err != nil {
			return nil, 0, err
		}
		return &isNullExpr{x, e.Not}, kBool, nil
	case *sql.BinaryExpr:
		l, lk, err := bindExpr(e.Left, res)
		if err != nil {
			return nil, 0, err
		}
		r, rk, err := bindExpr(e.Right, res)
		if err != nil {
			return nil, 0, err
		}
		if e.Op == "+" || e.Op == "-" || e.Op == "*" {
			if (lk != kInt && lk != kNull) || (rk != kInt && rk != kNull) {
				return nil, 0, fmt.Errorf("arithmetic '%s' requires INT operands", e.Op)
			}
			return &arithExpr{e.Op[0], l, r}, kInt, nil
		}
		if e.Op == "AND" || e.Op == "OR" {
			if lk != kBool || rk != kBool {
				return nil, 0, fmt.Errorf("%s requires boolean operands", e.Op)
			}
			return &logicExpr{e.Op == "AND", l, r}, kBool, nil
		}
		if lk == kBool || rk == kBool || (lk != rk && lk != kNull && rk != kNull) {
			return nil, 0, fmt.Errorf("type mismatch in comparison '%s'", e.Op)
		}
		return &cmpExpr{e.Op, l, r}, kBool, nil
	}
	return nil, 0, fmt.Errorf("unsupported expression")
}

// BindInsert 把 INSERT 的字面量行按表定义顺序补全并做类型检查。
func BindInsert(st *sql.InsertStmt, ti *catalog.TableInfo) ([][]tuple.Value, error) {
	cols := ti.Schema.Columns
	pos := make([]int, 0, len(cols))
	if len(st.Columns) == 0 {
		for i := range cols {
			pos = append(pos, i)
		}
	} else {
		seen := map[int]bool{}
		for _, name := range st.Columns {
			i := ti.Schema.ColumnIndex(name)
			if i < 0 {
				return nil, fmt.Errorf("unknown column '%s'", name)
			}
			if seen[i] {
				return nil, fmt.Errorf("column '%s' specified twice", name)
			}
			seen[i] = true
			pos = append(pos, i)
		}
		if !seen[ti.PK] {
			return nil, fmt.Errorf("primary key column '%s' must be specified", cols[ti.PK].Name)
		}
	}
	var out [][]tuple.Value
	for _, exprs := range st.Rows {
		if len(exprs) != len(pos) {
			return nil, fmt.Errorf("column count doesn't match value count")
		}
		row := make([]tuple.Value, len(cols))
		for i, c := range cols {
			row[i] = tuple.NullValue(c.Type)
		}
		for j, e := range exprs {
			lit, ok := e.(*sql.Literal)
			if !ok {
				return nil, fmt.Errorf("INSERT only supports literal values")
			}
			col := cols[pos[j]]
			if lit.Value.Null {
				if col.PrimaryKey {
					return nil, fmt.Errorf("primary key '%s' cannot be NULL", col.Name)
				}
				continue
			}
			if lit.Value.Type != col.Type {
				return nil, fmt.Errorf("incorrect %s value for column '%s'", col.Type, col.Name)
			}
			row[pos[j]] = lit.Value
		}
		out = append(out, row)
	}
	return out, nil
}

// scanSpec 是从 WHERE 中分析出的扫描方案: 谓词 + (可选) 主键键范围。
type scanSpec struct {
	pred       Expr
	start, end []byte
	ranged     bool
}

func analyzeWhere(where sql.Expr, ti *catalog.TableInfo) (scanSpec, error) {
	var sp scanSpec
	if where == nil {
		return sp, nil
	}
	var err error
	if sp.pred, err = BindExpr(where, ti.Schema); err != nil {
		return sp, err
	}
	sp.start, sp.end, sp.ranged = PKRange(where, ti, func(c *sql.ColumnRef) bool {
		return strings.EqualFold(c.Name, ti.Schema.Columns[ti.PK].Name)
	})
	return sp, nil
}

// snapshotScan 生成无锁快照读的扫描: IndexScan (WHERE 含主键范围时) 或 SeqScan，再叠加 Filter。
func snapshotScan(ctx *Ctx, ti *catalog.TableInfo, sp scanSpec) Executor {
	var scan Executor = NewSeqScan(ctx, ti)
	if sp.ranged {
		scan = NewIndexScan(ctx, ti, sp.start, sp.end)
	}
	if sp.pred != nil {
		scan = NewFilter(scan, sp.pred)
	}
	return scan
}

// PlanSelect 绑定并生成执行计划:
//
//	普通查询:        Projection <- Filter <- SeqScan | IndexScan   (快照读，不加锁)
//	FOR SHARE/UPDATE: Projection <- LockScan                       (当前读，逐行加 S/X 锁)
//
// WHERE 中针对主键的比较条件会被转换为键范围。
func PlanSelect(ctx *Ctx, st *sql.SelectStmt, ti *catalog.TableInfo) (Executor, []string, error) {
	var proj []int
	var names []string
	if st.Star {
		for i, c := range ti.Schema.Columns {
			proj = append(proj, i)
			names = append(names, c.Name)
		}
	} else {
		for _, ref := range st.Items {
			i := ti.Schema.ColumnIndex(ref.Name)
			if i < 0 {
				return nil, nil, fmt.Errorf("unknown column '%s'", ref.Name)
			}
			proj = append(proj, i)
			names = append(names, ti.Schema.Columns[i].Name)
		}
	}
	sp, err := analyzeWhere(st.Where, ti)
	if err != nil {
		return nil, nil, err
	}
	var root Executor
	switch st.Lock {
	case sql.LockShare:
		root = NewLockScan(ctx, ti, sp.start, sp.end, sp.pred, txn.LockS)
	case sql.LockUpdate:
		root = NewLockScan(ctx, ti, sp.start, sp.end, sp.pred, txn.LockX)
	default:
		root = snapshotScan(ctx, ti, sp)
	}
	return NewProjection(root, proj), names, nil
}

// PlanUpdate 生成 UPDATE 计划: Update <- LockScan(X)。不允许修改主键。
func PlanUpdate(ctx *Ctx, st *sql.UpdateStmt, ti *catalog.TableInfo) (*Update, error) {
	var sets []assignment
	seen := map[int]bool{}
	for _, a := range st.Sets {
		i := ti.Schema.ColumnIndex(a.Column)
		if i < 0 {
			return nil, fmt.Errorf("unknown column '%s'", a.Column)
		}
		if i == ti.PK {
			return nil, fmt.Errorf("updating primary key column '%s' is not supported", a.Column)
		}
		if seen[i] {
			return nil, fmt.Errorf("column '%s' specified twice", a.Column)
		}
		seen[i] = true
		e, k, err := bind(a.Value, ti.Schema)
		if err != nil {
			return nil, err
		}
		if k == kBool || (k != kNull && k != kindOf(ti.Schema.Columns[i].Type)) {
			return nil, fmt.Errorf("incorrect %s value for column '%s'", ti.Schema.Columns[i].Type, a.Column)
		}
		sets = append(sets, assignment{i, e})
	}
	sp, err := analyzeWhere(st.Where, ti)
	if err != nil {
		return nil, err
	}
	child := NewLockScan(ctx, ti, sp.start, sp.end, sp.pred, txn.LockX)
	return &Update{ctx: ctx, table: ti, child: child, sets: sets}, nil
}

// PlanDelete 生成 DELETE 计划: Delete <- LockScan(X)。
func PlanDelete(ctx *Ctx, st *sql.DeleteStmt, ti *catalog.TableInfo) (*Delete, error) {
	sp, err := analyzeWhere(st.Where, ti)
	if err != nil {
		return nil, err
	}
	child := NewLockScan(ctx, ti, sp.start, sp.end, sp.pred, txn.LockX)
	return &Delete{ctx: ctx, table: ti, child: child}, nil
}

// PKRange 从 WHERE 的顶层 AND 合取项中提取主键范围 [start, end)。isPKRef 判断一个列引用是否指向该表的主键列。
func PKRange(where sql.Expr, ti *catalog.TableInfo, isPKRef func(*sql.ColumnRef) bool) (start, end []byte, ok bool) {
	pk := ti.Schema.Columns[ti.PK]
	var visit func(e sql.Expr)
	visit = func(e sql.Expr) {
		b, isBin := e.(*sql.BinaryExpr)
		if !isBin {
			return
		}
		if b.Op == "AND" {
			visit(b.Left)
			visit(b.Right)
			return
		}
		op, lit := b.Op, (*sql.Literal)(nil)
		switch l, r := b.Left, b.Right; {
		case isPK(l, isPKRef) && isLit(r):
			lit = r.(*sql.Literal)
		case isPK(r, isPKRef) && isLit(l):
			lit = l.(*sql.Literal)
			op = flip(op)
		default:
			return
		}
		if lit.Value.Null || lit.Value.Type != pk.Type {
			return
		}
		key, err := tuple.EncodeKey(lit.Value)
		if err != nil {
			return
		}
		var lo, hi []byte
		switch op {
		case "=":
			lo, hi = key, tuple.KeySuccessor(key)
		case ">":
			lo = tuple.KeySuccessor(key)
		case ">=":
			lo = key
		case "<":
			hi = key
		case "<=":
			hi = tuple.KeySuccessor(key)
		default:
			return
		}
		ok = true
		if lo != nil && (start == nil || bytes.Compare(lo, start) > 0) {
			start = lo
		}
		if hi != nil && (end == nil || bytes.Compare(hi, end) < 0) {
			end = hi
		}
	}
	visit(where)
	return
}

func isPK(e sql.Expr, isPKRef func(*sql.ColumnRef) bool) bool {
	c, ok := e.(*sql.ColumnRef)
	return ok && isPKRef(c)
}

func isLit(e sql.Expr) bool { _, ok := e.(*sql.Literal); return ok }

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
