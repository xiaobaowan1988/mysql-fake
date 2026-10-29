// Package db 把存储、Catalog、解析器和执行器组装成可执行 SQL 的数据库引擎。
package db

import (
	"fmt"
	"strings"
	"sync"

	"mysqlfake/catalog"
	"mysqlfake/executor"
	"mysqlfake/sql"
	"mysqlfake/storage"
	"mysqlfake/tuple"
)

// Result 是一条语句的执行结果。
type Result struct {
	Columns  []string
	Rows     [][]tuple.Value
	Affected int
	IsQuery  bool
}

type Engine struct {
	mu   sync.Mutex
	disk *storage.DiskManager
	cat  *catalog.Catalog
}

func Open(path string) (*Engine, error) {
	d, err := storage.OpenDiskManager(path)
	if err != nil {
		return nil, err
	}
	cat, err := catalog.Open(d, d)
	if err != nil {
		d.Close()
		return nil, err
	}
	return &Engine{disk: d, cat: cat}, nil
}

func (e *Engine) Close() error { return e.disk.Close() }

// Exec 解析并执行一条 SQL。
func (e *Engine) Exec(query string) (*Result, error) {
	st, err := sql.Parse(query)
	if err != nil {
		return nil, err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	switch st := st.(type) {
	case *sql.CreateTableStmt:
		cols := make([]tuple.Column, len(st.Columns))
		for i, c := range st.Columns {
			cols[i] = tuple.Column{Name: c.Name, Type: c.Type, Len: c.Len, PrimaryKey: c.PrimaryKey}
		}
		if _, err := e.cat.CreateTable(st.Table, cols); err != nil {
			return nil, err
		}
		return &Result{}, nil
	case *sql.ShowTablesStmt:
		res := &Result{Columns: []string{"Tables"}, IsQuery: true}
		for _, n := range e.cat.ListTables() {
			res.Rows = append(res.Rows, []tuple.Value{tuple.StrValue(n)})
		}
		return res, nil
	case *sql.InsertStmt:
		ti, err := e.cat.GetTable(st.Table)
		if err != nil {
			return nil, err
		}
		rows, err := executor.BindInsert(st, ti)
		if err != nil {
			return nil, err
		}
		ins := executor.NewInsert(ti, rows)
		if err := ins.Init(); err != nil {
			return nil, err
		}
		_, err = ins.Next()
		return &Result{Affected: ins.Affected}, err
	case *sql.SelectStmt:
		ti, err := e.cat.GetTable(st.Table)
		if err != nil {
			return nil, err
		}
		ex, names, err := executor.PlanSelect(st, ti)
		if err != nil {
			return nil, err
		}
		if err := ex.Init(); err != nil {
			return nil, err
		}
		res := &Result{Columns: names, IsQuery: true}
		for {
			r, err := ex.Next()
			if err != nil {
				return nil, err
			}
			if r == nil {
				return res, nil
			}
			res.Rows = append(res.Rows, r.Values)
		}
	}
	return nil, fmt.Errorf("unsupported statement")
}

// Summary 返回 MySQL 风格的结果摘要。
func (r *Result) Summary() string {
	if r.IsQuery {
		if len(r.Rows) == 1 {
			return "1 row in set"
		}
		return fmt.Sprintf("%d rows in set", len(r.Rows))
	}
	if r.Affected > 0 {
		return fmt.Sprintf("Query OK, %d rows affected", r.Affected)
	}
	return "Query OK"
}

// Format 以类似 MySQL 客户端的 ASCII 表格输出结果集。
func (r *Result) Format() string {
	if !r.IsQuery {
		return r.Summary() + "\n"
	}
	widths := make([]int, len(r.Columns))
	for i, c := range r.Columns {
		widths[i] = len(c)
	}
	cells := make([][]string, len(r.Rows))
	for i, row := range r.Rows {
		cells[i] = make([]string, len(row))
		for j, v := range row {
			s := v.String()
			cells[i][j] = s
			if len(s) > widths[j] {
				widths[j] = len(s)
			}
		}
	}
	var sb strings.Builder
	sep := "+"
	for _, w := range widths {
		sep += strings.Repeat("-", w+2) + "+"
	}
	sep += "\n"
	line := func(vals []string, right []bool) {
		sb.WriteString("|")
		for i, s := range vals {
			pad := strings.Repeat(" ", widths[i]-len(s))
			if right != nil && right[i] {
				sb.WriteString(" " + pad + s + " |")
			} else {
				sb.WriteString(" " + s + pad + " |")
			}
		}
		sb.WriteString("\n")
	}
	if len(r.Columns) > 0 {
		sb.WriteString(sep)
		line(r.Columns, nil)
		sb.WriteString(sep)
		for i, row := range cells {
			right := make([]bool, len(row))
			for j, v := range r.Rows[i] {
				right[j] = v.Type == tuple.TypeInt && !v.Null
			}
			line(row, right)
		}
		sb.WriteString(sep)
	}
	sb.WriteString(r.Summary() + "\n")
	return sb.String()
}
