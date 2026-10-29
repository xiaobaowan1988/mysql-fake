// Package executor 实现火山模型执行器，以及 Binder / Planner。
package executor

import (
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

// Ctx 是执行上下文: 当前事务及事务管理器。
type Ctx struct {
	Txn *txn.Txn
	Mgr *txn.Manager
}

// lockKey 生成行锁资源名: 表名 + 主键编码。
func lockKey(table string, key []byte) string { return table + "\x00" + string(key) }
