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

// LockClause 是 SELECT 末尾的加锁子句 (当前读)。
type LockClause int

const (
	LockNone   LockClause = iota
	LockShare             // FOR SHARE
	LockUpdate            // FOR UPDATE
)

type SelectStmt struct {
	Star  bool
	Items []string // 列名
	Table string
	Where Expr // 可为 nil
	Lock  LockClause
}

type Assignment struct {
	Column string
	Value  Expr
}

type UpdateStmt struct {
	Table string
	Sets  []Assignment
	Where Expr
}

type DeleteStmt struct {
	Table string
	Where Expr
}

type BeginStmt struct{}
type CommitStmt struct{}
type RollbackStmt struct{}

type ShowTablesStmt struct{}

func (*CreateTableStmt) stmt() {}
func (*InsertStmt) stmt()      {}
func (*SelectStmt) stmt()      {}
func (*ShowTablesStmt) stmt()  {}
func (*UpdateStmt) stmt()      {}
func (*DeleteStmt) stmt()      {}
func (*BeginStmt) stmt()       {}
func (*CommitStmt) stmt()      {}
func (*RollbackStmt) stmt()    {}

type Expr interface{ expr() }

// Literal 字面量 (整数、字符串、NULL)。
type Literal struct{ Value tuple.Value }

// NullLiteral 的 Type 未定，由 Binder 按目标列补全；Value.Type == 0 表示未定类型的 NULL。
type ColumnRef struct{ Name string }

type BinaryExpr struct {
	Op          string // = != < <= > >= AND OR + - *
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
