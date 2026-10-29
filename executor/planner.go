package executor

import (
	"bytes"
	"fmt"
	"strings"

	"mysqlfake/catalog"
	"mysqlfake/sql"
	"mysqlfake/tuple"
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
	be, k, err := bind(e, s)
	if err == nil && k != kBool {
		return nil, fmt.Errorf("WHERE clause must be a boolean expression")
	}
	return be, err
}

func bind(e sql.Expr, s *tuple.Schema) (Expr, kind, error) {
	switch e := e.(type) {
	case *sql.Literal:
		if e.Value.Null {
			return &litExpr{e.Value}, kNull, nil
		}
		return &litExpr{e.Value}, kindOf(e.Value.Type), nil
	case *sql.ColumnRef:
		i := s.ColumnIndex(e.Name)
		if i < 0 {
			return nil, 0, fmt.Errorf("unknown column '%s'", e.Name)
		}
		return &colExpr{i}, kindOf(s.Columns[i].Type), nil
	case *sql.NotExpr:
		x, k, err := bind(e.Expr, s)
		if err != nil {
			return nil, 0, err
		}
		if k != kBool {
			return nil, 0, fmt.Errorf("NOT requires a boolean expression")
		}
		return &notExpr{x}, kBool, nil
	case *sql.IsNullExpr:
		x, _, err := bind(e.Expr, s)
		if err != nil {
			return nil, 0, err
		}
		return &isNullExpr{x, e.Not}, kBool, nil
	case *sql.BinaryExpr:
		l, lk, err := bind(e.Left, s)
		if err != nil {
			return nil, 0, err
		}
		r, rk, err := bind(e.Right, s)
		if err != nil {
			return nil, 0, err
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

// PlanSelect 绑定并生成执行计划:
// [Projection] <- [Filter] <- SeqScan | IndexScan。
// WHERE 中针对主键的比较条件会被转换为 IndexScan 的键范围。
func PlanSelect(st *sql.SelectStmt, ti *catalog.TableInfo) (Executor, []string, error) {
	var proj []int
	var names []string
	if st.Star {
		for i, c := range ti.Schema.Columns {
			proj = append(proj, i)
			names = append(names, c.Name)
		}
	} else {
		for _, n := range st.Items {
			i := ti.Schema.ColumnIndex(n)
			if i < 0 {
				return nil, nil, fmt.Errorf("unknown column '%s'", n)
			}
			proj = append(proj, i)
			names = append(names, ti.Schema.Columns[i].Name)
		}
	}
	var scan Executor = NewSeqScan(ti)
	var pred Expr
	if st.Where != nil {
		var err error
		if pred, err = BindExpr(st.Where, ti.Schema); err != nil {
			return nil, nil, err
		}
		if start, end, ok := pkRange(st.Where, ti); ok {
			scan = NewIndexScan(ti, start, end)
		}
	}
	root := scan
	if pred != nil {
		root = NewFilter(root, pred)
	}
	return NewProjection(root, proj), names, nil
}

// pkRange 从 WHERE 的顶层 AND 合取项中提取主键范围 [start, end)。
func pkRange(where sql.Expr, ti *catalog.TableInfo) (start, end []byte, ok bool) {
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
		case isPK(l, pk.Name) && isLit(r):
			lit = r.(*sql.Literal)
		case isPK(r, pk.Name) && isLit(l):
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

func isPK(e sql.Expr, name string) bool {
	c, ok := e.(*sql.ColumnRef)
	return ok && strings.EqualFold(c.Name, name)
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
