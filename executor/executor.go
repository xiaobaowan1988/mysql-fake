// Package executor 实现火山模型执行器，以及 Binder / Planner。
package executor

import (
	"mysqlfake/tuple"
)

// Row 是执行器之间传递的一行: Key 为主键编码，Values 为各列的值。
type Row struct {
	Key    []byte
	Values []tuple.Value
}

// Executor 是火山模型接口。Next 返回 (nil, nil) 表示结束。
type Executor interface {
	Init() error
	Next() (*Row, error)
}
