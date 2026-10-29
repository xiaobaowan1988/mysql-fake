package executor

import (
	"fmt"

	"mysqlfake/tuple"
)

// 已绑定的表达式。布尔值用 INT 的 1/0 表示，NULL 表示 UNKNOWN (SQL 三值逻辑)。
type Expr interface {
	Eval(row []tuple.Value) tuple.Value
}

type litExpr struct{ v tuple.Value }
type colExpr struct{ idx int }
type cmpExpr struct {
	op   string
	l, r Expr
}
type logicExpr struct {
	and  bool
	l, r Expr
}
type notExpr struct{ e Expr }
type arithExpr struct {
	op   byte
	l, r Expr
}
type isNullExpr struct {
	e   Expr
	not bool
}

func (e *litExpr) Eval([]tuple.Value) tuple.Value     { return e.v }
func (e *colExpr) Eval(row []tuple.Value) tuple.Value { return row[e.idx] }

func boolVal(b bool) tuple.Value {
	if b {
		return tuple.IntValue(1)
	}
	return tuple.IntValue(0)
}

var unknown = tuple.NullValue(tuple.TypeInt)

func (e *cmpExpr) Eval(row []tuple.Value) tuple.Value {
	l, r := e.l.Eval(row), e.r.Eval(row)
	if l.Null || r.Null {
		return unknown
	}
	c := tuple.Compare(l, r)
	switch e.op {
	case "=":
		return boolVal(c == 0)
	case "!=":
		return boolVal(c != 0)
	case "<":
		return boolVal(c < 0)
	case "<=":
		return boolVal(c <= 0)
	case ">":
		return boolVal(c > 0)
	case ">=":
		return boolVal(c >= 0)
	}
	panic(fmt.Sprintf("bad comparison operator %q", e.op))
}

func (e *logicExpr) Eval(row []tuple.Value) tuple.Value {
	l, r := e.l.Eval(row), e.r.Eval(row)
	lt, rt := !l.Null && l.Int != 0, !r.Null && r.Int != 0
	lf, rf := !l.Null && l.Int == 0, !r.Null && r.Int == 0
	if e.and {
		switch {
		case lf || rf:
			return boolVal(false)
		case lt && rt:
			return boolVal(true)
		}
		return unknown
	}
	switch {
	case lt || rt:
		return boolVal(true)
	case lf && rf:
		return boolVal(false)
	}
	return unknown
}

func (e *notExpr) Eval(row []tuple.Value) tuple.Value {
	v := e.e.Eval(row)
	if v.Null {
		return unknown
	}
	return boolVal(v.Int == 0)
}

func (e *arithExpr) Eval(row []tuple.Value) tuple.Value {
	l, r := e.l.Eval(row), e.r.Eval(row)
	if l.Null || r.Null {
		return tuple.NullValue(tuple.TypeInt)
	}
	switch e.op {
	case '+':
		return tuple.IntValue(l.Int + r.Int)
	case '-':
		return tuple.IntValue(l.Int - r.Int)
	}
	return tuple.IntValue(l.Int * r.Int)
}

func (e *isNullExpr) Eval(row []tuple.Value) tuple.Value {
	return boolVal(e.e.Eval(row).Null != e.not)
}

// EvalBool 判断行是否满足谓词 (UNKNOWN 视为不满足)。
func EvalBool(e Expr, row []tuple.Value) bool {
	v := e.Eval(row)
	return !v.Null && v.Int != 0
}
