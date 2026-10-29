// Package executor 实现火山模型执行器，以及 Binder / Planner。
package executor

import (
	"sync/atomic"
	"time"

	"mysqlfake/tuple"
	"mysqlfake/txn"
)

// Row 是执行器之间传递的一行: Key 为主键编码，Values 为各列的值。
// Raw 仅由当前读 (LockScan) 填充，是该行在 B+ 树中的完整存储值 (含版本头)，供 UPDATE/DELETE 记录 undo。
type Row struct {
	Key    []byte
	Values []tuple.Value
	Raw    []byte
}

// Executor 是火山模型接口。Next 返回 (nil, nil) 表示结束。
type Executor interface {
	Init() error
	Next() (*Row, error)
}

// ExecStats 收集一条语句执行期间的统计 (可并发更新)。
type ExecStats struct {
	RowsExamined atomic.Int64 // 扫描从存储里读出的行数 (过滤之前)
}

// Ctx 是执行上下文: 当前事务、事务管理器和统计收集器 (可为 nil)。
type Ctx struct {
	Txn   *txn.Txn
	Mgr   *txn.Manager
	Stats *ExecStats
}

// Examined 记录扫描读到 n 行。
func (c *Ctx) Examined(n int64) {
	if c != nil && c.Stats != nil {
		c.Stats.RowsExamined.Add(n)
	}
}

// lockKey 生成行锁资源名: 表名 + 主键编码。
func lockKey(table string, key []byte) string { return table + "\x00" + string(key) }

// OpStats 是 EXPLAIN ANALYZE 为一个算子收集的实际执行数据 (耗时包含其子算子)。
type OpStats struct {
	Rows  atomic.Int64
	Nanos atomic.Int64
	Loops atomic.Int64
}

// Instrumented 包装一个执行器并统计它实际产生的行数和耗时。
type Instrumented struct {
	child Executor
	st    *OpStats
}

func NewInstrumented(child Executor, st *OpStats) *Instrumented { return &Instrumented{child, st} }

func (i *Instrumented) Init() error {
	start := time.Now()
	err := i.child.Init()
	i.st.Nanos.Add(int64(time.Since(start)))
	i.st.Loops.Add(1)
	return err
}

func (i *Instrumented) Next() (*Row, error) {
	start := time.Now()
	r, err := i.child.Next()
	i.st.Nanos.Add(int64(time.Since(start)))
	if r != nil {
		i.st.Rows.Add(1)
	}
	return r, err
}
