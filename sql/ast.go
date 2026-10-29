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

// TableRef 是 FROM 子句中的一个表 (可带别名)。
type TableRef struct {
	Name  string
	Alias string // 为空时用表名
}

// Binding 返回该表在查询中的名字 (别名优先)。
func (t TableRef) Binding() string {
	if t.Alias != "" {
		return t.Alias
	}
	return t.Name
}

type OrderItem struct {
	Col  ColumnRef
	Desc bool
}

// SelectStmt: JOIN ... ON 的条件已并入 Where (只支持 INNER JOIN)，多表用 From 列表表示。
type SelectStmt struct {
	Star    bool
	Items   []ColumnRef
	From    []TableRef
	Where   Expr // 可为 nil
	OrderBy []OrderItem
	Limit   int // -1 表示没有
	Lock    LockClause
}

// ExplainStmt 显示查询的执行计划。
type ExplainStmt struct{ Select *SelectStmt }

// AnalyzeStmt 重新收集表的统计信息。
type AnalyzeStmt struct{ Table string }

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
func (*ExplainStmt) stmt()     {}
func (*AnalyzeStmt) stmt()     {}
func (*UpdateStmt) stmt()      {}
func (*DeleteStmt) stmt()      {}
func (*BeginStmt) stmt()       {}
func (*CommitStmt) stmt()      {}
func (*RollbackStmt) stmt()    {}

type Expr interface{ expr() }

// Literal 字面量 (整数、字符串、NULL)。
type Literal struct{ Value tuple.Value }

// NullLiteral 的 Type 未定，由 Binder 按目标列补全；Value.Type == 0 表示未定类型的 NULL。
type ColumnRef struct {
	Table string // 限定名 (表名或别名)，可为空
	Name  string
}

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
