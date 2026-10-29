package vector

import (
	"cmp"
	"fmt"

	"mysqlfake/executor"
	"mysqlfake/sql"
	"mysqlfake/tuple"
)

// Kernel 在一批数据的选择向量 sel 上求值谓词，返回三值逻辑的结果:
// t = 结果为 TRUE 的行，f = 结果为 FALSE 的行，其余 (NULL 导致 UNKNOWN) 两者都不含。
// 输入输出的下标都按升序排列。
type Kernel interface {
	Eval(b *Batch, sel []int32) (t, f []int32)
}

// 选择向量的集合运算 (均要求升序输入)。
func intersect(a, b []int32) []int32 {
	out := make([]int32, 0, min(len(a), len(b)))
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		switch {
		case a[i] == b[j]:
			out = append(out, a[i])
			i++
			j++
		case a[i] < b[j]:
			i++
		default:
			j++
		}
	}
	return out
}

func union(a, b []int32) []int32 {
	out := make([]int32, 0, len(a)+len(b))
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		switch {
		case a[i] == b[j]:
			out = append(out, a[i])
			i++
			j++
		case a[i] < b[j]:
			out = append(out, a[i])
			i++
		default:
			out = append(out, b[j])
			j++
		}
	}
	out = append(out, a[i:]...)
	return append(out, b[j:]...)
}

// diff 返回 a 中不在 b 里的元素。
func diff(a, b []int32) []int32 {
	if len(b) == 0 {
		return a
	}
	out := make([]int32, 0, len(a))
	j := 0
	for _, x := range a {
		for j < len(b) && b[j] < x {
			j++
		}
		if j < len(b) && b[j] == x {
			continue
		}
		out = append(out, x)
	}
	return out
}

type andK struct{ l, r Kernel }

func (k andK) Eval(b *Batch, sel []int32) (t, f []int32) {
	t1, f1 := k.l.Eval(b, sel)
	t2, f2 := k.r.Eval(b, diff(sel, f1)) // 左边为假的行不用再算右边
	return intersect(t1, t2), union(f1, f2)
}

type orK struct{ l, r Kernel }

func (k orK) Eval(b *Batch, sel []int32) (t, f []int32) {
	t1, f1 := k.l.Eval(b, sel)
	t2, f2 := k.r.Eval(b, diff(sel, t1)) // 左边为真的行不用再算右边
	return union(t1, t2), intersect(f1, f2)
}

type notK struct{ k Kernel }

func (k notK) Eval(b *Batch, sel []int32) (t, f []int32) {
	t, f = k.k.Eval(b, sel)
	return f, t
}

type isNullK struct {
	col int
	not bool
}

func (k isNullK) Eval(b *Batch, sel []int32) (t, f []int32) {
	nulls := b.Cols[k.col].Nulls
	for _, i := range sel {
		if nulls[i] != k.not {
			t = append(t, i)
		} else {
			f = append(f, i)
		}
	}
	return
}

// unknownK: 与 NULL 比较，结果恒为 UNKNOWN。
type unknownK struct{}

func (unknownK) Eval(*Batch, []int32) (t, f []int32) { return nil, nil }

// cmpLit: 列 op 字面量。
type cmpLit[T cmp.Ordered] struct {
	col int
	op  string
	lit T
}

func values[T cmp.Ordered](c *tuple.ColBuf) []T {
	if c.Type == tuple.TypeInt {
		return any(c.Ints).([]T)
	}
	return any(c.Strs).([]T)
}

func (k cmpLit[T]) Eval(b *Batch, sel []int32) (t, f []int32) {
	c := b.Cols[k.col]
	vals, nulls, lit := values[T](c), c.Nulls, k.lit
	t, f = make([]int32, 0, len(sel)), make([]int32, 0, len(sel))
	switch k.op {
	case "=":
		for _, i := range sel {
			if nulls[i] {
				continue
			}
			if vals[i] == lit {
				t = append(t, i)
			} else {
				f = append(f, i)
			}
		}
	case "!=":
		for _, i := range sel {
			if nulls[i] {
				continue
			}
			if vals[i] != lit {
				t = append(t, i)
			} else {
				f = append(f, i)
			}
		}
	case "<":
		for _, i := range sel {
			if nulls[i] {
				continue
			}
			if vals[i] < lit {
				t = append(t, i)
			} else {
				f = append(f, i)
			}
		}
	case "<=":
		for _, i := range sel {
			if nulls[i] {
				continue
			}
			if vals[i] <= lit {
				t = append(t, i)
			} else {
				f = append(f, i)
			}
		}
	case ">":
		for _, i := range sel {
			if nulls[i] {
				continue
			}
			if vals[i] > lit {
				t = append(t, i)
			} else {
				f = append(f, i)
			}
		}
	case ">=":
		for _, i := range sel {
			if nulls[i] {
				continue
			}
			if vals[i] >= lit {
				t = append(t, i)
			} else {
				f = append(f, i)
			}
		}
	}
	return
}

// cmpCol: 列 op 列 (同类型)。
type cmpCol[T cmp.Ordered] struct {
	a, b int
	op   string
}

func (k cmpCol[T]) Eval(b *Batch, sel []int32) (t, f []int32) {
	ca, cb := b.Cols[k.a], b.Cols[k.b]
	av, bv := values[T](ca), values[T](cb)
	t, f = make([]int32, 0, len(sel)), make([]int32, 0, len(sel))
	for _, i := range sel {
		if ca.Nulls[i] || cb.Nulls[i] {
			continue
		}
		var ok bool
		switch k.op {
		case "=":
			ok = av[i] == bv[i]
		case "!=":
			ok = av[i] != bv[i]
		case "<":
			ok = av[i] < bv[i]
		case "<=":
			ok = av[i] <= bv[i]
		case ">":
			ok = av[i] > bv[i]
		default:
			ok = av[i] >= bv[i]
		}
		if ok {
			t = append(t, i)
		} else {
			f = append(f, i)
		}
	}
	return
}

// rowK 是兜底: 对形状不被向量化核函数支持的表达式 (算术等)，逐行用行式求值器计算。
type rowK struct {
	expr executor.Expr
}

func (k rowK) Eval(b *Batch, sel []int32) (t, f []int32) {
	for _, i := range sel {
		v := k.expr.Eval(b.Row(int(i)))
		switch {
		case v.Null:
		case v.Int != 0:
			t = append(t, i)
		default:
			f = append(f, i)
		}
	}
	return
}

func flipOp(op string) string {
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

func isCmp(op string) bool {
	switch op {
	case "=", "!=", "<", "<=", ">", ">=":
		return true
	}
	return false
}

// Compile 把谓词编译成核函数。列位置/类型由 res 给出；无法向量化的子表达式整体退回行式求值。
func Compile(e sql.Expr, res executor.Resolver) (Kernel, error) {
	// 先做绑定 (类型检查 + 兜底用的行式表达式)，不合法的表达式在这里报错
	bound, err := executor.BindPredicate(e, res)
	if err != nil {
		return nil, err
	}
	return compile(e, res, bound)
}

func compile(e sql.Expr, res executor.Resolver, bound executor.Expr) (Kernel, error) {
	fallback := func() (Kernel, error) {
		b, err := executor.BindPredicate(e, res)
		if err != nil {
			return nil, err
		}
		return rowK{b}, nil
	}
	switch e := e.(type) {
	case *sql.BinaryExpr:
		switch {
		case e.Op == "AND" || e.Op == "OR":
			l, err := Compile(e.Left, res)
			if err != nil {
				return nil, err
			}
			r, err := Compile(e.Right, res)
			if err != nil {
				return nil, err
			}
			if e.Op == "AND" {
				return andK{l, r}, nil
			}
			return orK{l, r}, nil
		case isCmp(e.Op):
			return compileCmp(e, res, fallback)
		}
	case *sql.NotExpr:
		k, err := Compile(e.Expr, res)
		if err != nil {
			return nil, err
		}
		return notK{k}, nil
	case *sql.IsNullExpr:
		if c, ok := e.Expr.(*sql.ColumnRef); ok {
			idx, _, err := res(c.Table, c.Name)
			if err != nil {
				return nil, err
			}
			return isNullK{col: idx, not: e.Not}, nil
		}
	}
	return fallback()
}

func compileCmp(e *sql.BinaryExpr, res executor.Resolver, fallback func() (Kernel, error)) (Kernel, error) {
	lc, lIsCol := e.Left.(*sql.ColumnRef)
	rc, rIsCol := e.Right.(*sql.ColumnRef)
	ll, lIsLit := e.Left.(*sql.Literal)
	rl, rIsLit := e.Right.(*sql.Literal)
	switch {
	case lIsCol && rIsLit:
		return cmpColLit(lc, rl, e.Op, res, fallback)
	case lIsLit && rIsCol:
		return cmpColLit(rc, ll, flipOp(e.Op), res, fallback)
	case lIsCol && rIsCol:
		ai, at, err := res(lc.Table, lc.Name)
		if err != nil {
			return nil, err
		}
		bi, bt, err := res(rc.Table, rc.Name)
		if err != nil {
			return nil, err
		}
		if at != bt {
			return nil, fmt.Errorf("type mismatch in comparison '%s'", e.Op)
		}
		if at == tuple.TypeInt {
			return cmpCol[int64]{ai, bi, e.Op}, nil
		}
		return cmpCol[string]{ai, bi, e.Op}, nil
	}
	return fallback()
}

func cmpColLit(c *sql.ColumnRef, lit *sql.Literal, op string, res executor.Resolver, fallback func() (Kernel, error)) (Kernel, error) {
	idx, typ, err := res(c.Table, c.Name)
	if err != nil {
		return nil, err
	}
	if lit.Value.Null {
		return unknownK{}, nil
	}
	if lit.Value.Type != typ {
		return nil, fmt.Errorf("type mismatch in comparison '%s'", op)
	}
	if typ == tuple.TypeInt {
		return cmpLit[int64]{idx, op, lit.Value.Int}, nil
	}
	return cmpLit[string]{idx, op, lit.Value.Str}, nil
}

// CompileAll 把多个合取项编译成一个核函数 (用 AND 连接)。
func CompileAll(es []sql.Expr, res executor.Resolver) (Kernel, error) {
	var out Kernel
	for _, e := range es {
		k, err := Compile(e, res)
		if err != nil {
			return nil, err
		}
		if out == nil {
			out = k
		} else {
			out = andK{out, k}
		}
	}
	return out, nil
}
