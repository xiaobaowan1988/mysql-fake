// Package sql 提供词法分析、语法解析及 AST 定义。
package sql

import "mysqlfake/tuple"

type Statement interface{ stmt() }

type ColumnDef struct {
	Name       string
	Type       tuple.Type
	Len        int
	PrimaryKey bool
}

type CreateTableStmt struct {
	Table   string
	Columns []ColumnDef
}

type InsertStmt struct {
	Table   string
	Columns []string // 为空表示按表定义顺序
	Rows    [][]Expr
}

type SelectStmt struct {
	Star  bool
	Items []string // 列名
	Table string
	Where Expr // 可为 nil
}

type ShowTablesStmt struct{}

func (*CreateTableStmt) stmt() {}
func (*InsertStmt) stmt()      {}
func (*SelectStmt) stmt()      {}
func (*ShowTablesStmt) stmt()  {}

type Expr interface{ expr() }

// Literal 字面量 (整数、字符串、NULL)。
type Literal struct{ Value tuple.Value }

// NullLiteral 的 Type 未定，由 Binder 按目标列补全；Value.Type == 0 表示未定类型的 NULL。
type ColumnRef struct{ Name string }

type BinaryExpr struct {
	Op          string // = != < <= > >= AND OR
	Left, Right Expr
}

type NotExpr struct{ Expr Expr }

type IsNullExpr struct {
	Expr Expr
	Not  bool
}

func (*Literal) expr()    {}
func (*ColumnRef) expr()  {}
func (*BinaryExpr) expr() {}
func (*NotExpr) expr()    {}
func (*IsNullExpr) expr() {}
