// Package db 把存储、Catalog、解析器和执行器组装成可执行 SQL 的数据库引擎。
package db

import (
	"fmt"
	"strings"
	"sync"

	"mysqlfake/buffer"
	"mysqlfake/catalog"
	"mysqlfake/executor"
	"mysqlfake/sql"
	"mysqlfake/storage"
	"mysqlfake/tuple"
	"mysqlfake/wal"
)

// Result 是一条语句的执行结果。
type Result struct {
	Columns  []string
	Rows     [][]tuple.Value
	Affected int
	IsQuery  bool
}

// Options 是引擎的可选参数。
type Options struct {
	PoolSize int // 缓冲池页帧数，默认 256
}

type Engine struct {
	mu     sync.Mutex
	disk   *storage.DiskManager
	log    *wal.LogManager
	pool   *buffer.Pool
	cat    *catalog.Catalog
	txnSeq uint64
}

func Open(path string) (*Engine, error) { return OpenWithOptions(path, Options{}) }

// OpenWithOptions 打开数据库: 先做崩溃恢复 (回放 WAL)，再建立缓冲池和 Catalog。
// 数据文件为 path，日志文件为 path+".wal"。
func OpenWithOptions(path string, opts Options) (*Engine, error) {
	if opts.PoolSize == 0 {
		opts.PoolSize = 256
	}
	d, err := storage.OpenDiskManager(path)
	if err != nil {
		return nil, err
	}
	lm, err := wal.Open(path + ".wal")
	if err != nil {
		d.Close()
		return nil, err
	}
	fail := func(err error) (*Engine, error) {
		lm.Close()
		d.Close()
		return nil, err
	}
	if _, err := wal.Recover(d, lm); err != nil {
		return fail(err)
	}
	pool := buffer.New(d, lm, opts.PoolSize)
	cat, err := catalog.Open(pool, d)
	if err != nil {
		return fail(err)
	}
	// 首次创建 Catalog 时文件头先于系统树页落盘，这里立即做一次 Checkpoint 保证二者一致。
	if err := pool.Checkpoint(); err != nil {
		return fail(err)
	}
	return &Engine{disk: d, log: lm, pool: pool, cat: cat}, nil
}

// Close 做 Checkpoint 后关闭所有文件。
func (e *Engine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	err := e.pool.Checkpoint()
	if e2 := e.log.Close(); err == nil {
		err = e2
	}
	if e2 := e.disk.Close(); err == nil {
		err = e2
	}
	return err
}

// commit 写提交记录并刷日志: 语句返回前其修改已持久化。
func (e *Engine) commit() error {
	e.txnSeq++
	return e.pool.Commit(e.txnSeq)
}

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
		_, err := e.cat.CreateTable(st.Table, cols)
		if cerr := e.commit(); err == nil {
			err = cerr
		}
		if err != nil {
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
		if cerr := e.commit(); err == nil {
			err = cerr
		}
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
