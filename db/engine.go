// Package db 把存储、Catalog、解析器和执行器组装成可执行 SQL 的数据库引擎。
package db

import (
	"fmt"
	"strings"
	"sync"

	"mysqlfake/binlog"
	"mysqlfake/buffer"
	"mysqlfake/catalog"
	"mysqlfake/storage"
	"mysqlfake/tuple"
	"mysqlfake/txn"
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
	PoolSize int  // 缓冲池页帧数，默认 256
	NoBinlog bool // 不写 Binlog
}

type Engine struct {
	disk  *storage.DiskManager
	log   *wal.LogManager
	pool  *buffer.Pool
	cat   *catalog.Catalog
	mgr   *txn.Manager
	def   *Session
	defMu sync.Mutex
	bl    *binlog.Log

	guardMu sync.RWMutex
	guard   func() error
}

// SetWriteGuard 设置写入守卫: 每条写语句执行前调用，返回错误则拒绝 (集群中用于只读副本)。
func (e *Engine) SetWriteGuard(f func() error) {
	e.guardMu.Lock()
	e.guard = f
	e.guardMu.Unlock()
}

func (e *Engine) checkWrite() error {
	e.guardMu.RLock()
	g := e.guard
	e.guardMu.RUnlock()
	if g == nil {
		return nil
	}
	return g()
}

// SetReplicator 设置提交前的复制钩子。
func (e *Engine) SetReplicator(f txn.ReplicateFunc) { e.mgr.SetReplicator(f) }

// Binlog 返回本引擎的 Binlog (可能为 nil)。
func (e *Engine) Binlog() *binlog.Log { return e.bl }

// Manager 返回事务管理器。
func (e *Engine) Manager() *txn.Manager { return e.mgr }

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
	e := &Engine{disk: d, log: lm, pool: pool, cat: cat, mgr: txn.NewManager(d, pool)}
	if !opts.NoBinlog {
		if e.bl, err = binlog.Open(path + ".binlog"); err != nil {
			return fail(err)
		}
		e.mgr.Binlog = e.bl
	}
	e.def = e.NewSession()
	return e, nil
}

// Close 做 Checkpoint 后关闭所有文件。
func (e *Engine) Close() error {
	e.def.Close()
	err := e.pool.Checkpoint()
	if e.bl != nil {
		if e2 := e.bl.Close(); err == nil {
			err = e2
		}
	}
	if e2 := e.log.Close(); err == nil {
		err = e2
	}
	if e2 := e.disk.Close(); err == nil {
		err = e2
	}
	return err
}

// Exec 使用引擎内置的默认会话执行一条 SQL (串行)。需要并发/多连接时请用 NewSession。
func (e *Engine) Exec(query string) (*Result, error) {
	e.defMu.Lock()
	defer e.defMu.Unlock()
	return e.def.Exec(query)
}

// NewSession 创建一个独立的会话 (连接)，会话内可以使用显式事务。
func (e *Engine) NewSession() *Session { return &Session{eng: e} }

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
