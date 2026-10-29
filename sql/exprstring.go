package sql

import (
	"fmt"
	"strings"
)

// ExprString 把表达式还原成可读的 SQL 文本 (EXPLAIN 使用)。
func ExprString(e Expr) string {
	switch e := e.(type) {
	case *Literal:
		if e.Value.Null {
			return "NULL"
		}
		if e.Value.Type == 2 {
			return "'" + strings.ReplaceAll(e.Value.Str, "'", "''") + "'"
		}
		return fmt.Sprint(e.Value.Int)
	case *ColumnRef:
		if e.Table != "" {
			return e.Table + "." + e.Name
		}
		return e.Name
	case *BinaryExpr:
		return "(" + ExprString(e.Left) + " " + e.Op + " " + ExprString(e.Right) + ")"
	case *NotExpr:
		return "NOT " + ExprString(e.Expr)
	case *IsNullExpr:
		if e.Not {
			return ExprString(e.Expr) + " IS NOT NULL"
		}
		return ExprString(e.Expr) + " IS NULL"
	}
	return "?"
}

// SplitAnd 把顶层 AND 链拆成合取项。
func SplitAnd(e Expr) []Expr {
	if e == nil {
		return nil
	}
	if b, ok := e.(*BinaryExpr); ok && b.Op == "AND" {
		return append(SplitAnd(b.Left), SplitAnd(b.Right)...)
	}
	return []Expr{e}
}
