// Package db 把存储、Catalog、解析器和执行器组装成可执行 SQL 的数据库引擎。
package db

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"mysqlfake/binlog"
	"mysqlfake/buffer"
	"mysqlfake/catalog"
	"mysqlfake/metrics"
	"mysqlfake/optimizer"
	"mysqlfake/stats"
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

	CheckpointInterval time.Duration // 后台 Fuzzy Checkpoint 间隔，0 表示默认 10s，负数表示关闭
	CheckpointLogBytes int64         // WAL 超过该大小也触发 Checkpoint，0 表示默认 16MB

	StatsInterval   time.Duration     // 统计信息后台采样间隔，0 表示默认 30s，负数表示关闭
	StatsSampleSize int               // 每表蓄水池采样行数，默认 10000
	Planner         optimizer.Options // 优化器选项 (测试用: 强制 Join 算法等)

	SlowQueryThreshold time.Duration // 超过该耗时的语句写入慢查询日志；0 表示关闭
	SlowQueryLog       string        // 慢查询日志文件路径，空表示 stderr
}

type Engine struct {
	path      string // 数据文件路径
	disk      *storage.DiskManager
	log       *wal.LogManager
	pool      *buffer.Pool
	cat       *catalog.Catalog
	mgr       *txn.Manager
	def       *Session
	defMu     sync.Mutex
	bl        *binlog.Log
	slow      *slowLog
	stopCkpt  func()
	stats     *stats.Store
	statsOpt  Options
	stopStats func()

	guardMu sync.RWMutex
	guard   func() error

	backupMu   sync.Mutex // 同一时刻只做一个备份
	backupHook func(stage string)

	ddlMu   sync.Mutex // 串行化在线 DDL
	ddlMu2  sync.Mutex // 保护 ddlHook
	ddlHook func(stage string)
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
	e := &Engine{path: path, disk: d, log: lm, pool: pool, cat: cat, mgr: txn.NewManager(d, pool)}
	if !opts.NoBinlog {
		if e.bl, err = binlog.Open(path + ".binlog"); err != nil {
			return fail(err)
		}
		e.mgr.Binlog = e.bl
	}
	e.def = e.NewSession()
	if e.slow, err = openSlowLog(opts.SlowQueryThreshold, opts.SlowQueryLog); err != nil {
		return fail(err)
	}
	e.registerGauges()
	e.stats = stats.NewStore()
	e.statsOpt = opts
	e.mgr.Subscribe(func(rec *binlog.Txn) { // 统计修改计数: 触发后台重新采样
		for _, ev := range rec.Events {
			if ev.Table != "" {
				e.stats.AddMods(ev.Table, 1)
			}
		}
	})
	if opts.StatsInterval >= 0 {
		iv := opts.StatsInterval
		if iv == 0 {
			iv = 30 * time.Second
		}
		e.stopStats = e.startStatsDaemon(iv)
	}
	if opts.CheckpointInterval >= 0 {
		iv, lb := opts.CheckpointInterval, opts.CheckpointLogBytes
		if iv == 0 {
			iv = 10 * time.Second
		}
		if lb == 0 {
			lb = 16 << 20
		}
		e.stopCkpt = pool.StartCheckpointer(iv, lb)
	}
	return e, nil
}

// Close 做 Checkpoint 后关闭所有文件。
func (e *Engine) Close() error {
	e.def.Close()
	e.slow.close()
	if e.stopStats != nil {
		e.stopStats()
	}
	if e.stopCkpt != nil {
		e.stopCkpt()
	}
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

// registerGauges 登记抓取时才求值的仪表 (多个引擎实例时以最近打开的为准)。
func (e *Engine) registerGauges() {
	metrics.Default.GaugeFunc("mysqlfake_buffer_pool_dirty_pages", "Dirty pages currently in the buffer pool.",
		func() float64 { return float64(e.pool.DirtyCount()) })
	metrics.Default.GaugeFunc("mysqlfake_active_transactions", "Open transactions.",
		func() float64 { return float64(e.mgr.ActiveCount()) })
	metrics.Default.GaugeFunc("mysqlfake_wal_size_bytes", "Current size of the write-ahead log file.",
		func() float64 { return float64(e.log.Size()) })
	metrics.Default.GaugeFunc("mysqlfake_database_pages", "Pages in the database file.",
		func() float64 { return float64(e.disk.NumPages()) })
}
