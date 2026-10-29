// Package vector 实现向量化执行: 以列式批 (Batch，每批最多 1024 行) 为单位处理数据，
// 过滤使用选择向量 + 按列的核函数，避免逐行的虚函数调用和 Value 对象分配。
package vector

import (
	"mysqlfake/executor"
	"mysqlfake/tuple"
)

const BatchSize = 1024

// Batch 是一批行。Sel 为 nil 表示 0..N-1 全部有效，否则只有 Sel 中的行有效 (升序)。
type Batch struct {
	Cols []*tuple.ColBuf
	N    int
	Sel  []int32
}

// Count 返回有效行数。
func (b *Batch) Count() int {
	if b.Sel == nil {
		return b.N
	}
	return len(b.Sel)
}

// Active 返回有效行下标 (Sel 为 nil 时生成 0..N-1)。
func (b *Batch) Active() []int32 { return active(b) }

// Row 取第 i 行 (物理下标) 的全部列值。
func (b *Batch) Row(i int) []tuple.Value {
	out := make([]tuple.Value, len(b.Cols))
	for c, col := range b.Cols {
		out[c] = col.Value(i)
	}
	return out
}

func newCols(types []tuple.Type, capacity int) []*tuple.ColBuf {
	cols := make([]*tuple.ColBuf, len(types))
	for i, t := range types {
		c := &tuple.ColBuf{Type: t, Nulls: make([]bool, 0, capacity)}
		if t == tuple.TypeInt {
			c.Ints = make([]int64, 0, capacity)
		} else {
			c.Strs = make([]string, 0, capacity)
		}
		cols[i] = c
	}
	return cols
}

// BatchExecutor 是向量化版本的 Executor: NextBatch 返回 (nil, nil) 表示结束。
type BatchExecutor interface {
	Init() error
	NextBatch() (*Batch, error)
}

// ---- 行/批适配器: 让向量化算子与行式算子混合使用 ----

// ToRows 把批执行器适配成行执行器。
type ToRows struct {
	child BatchExecutor
	cur   *Batch
	sel   []int32
	pos   int
}

func NewToRows(child BatchExecutor) *ToRows { return &ToRows{child: child} }

func (t *ToRows) Init() error { t.cur, t.sel, t.pos = nil, nil, 0; return t.child.Init() }

func (t *ToRows) Next() (*executor.Row, error) {
	for t.cur == nil || t.pos >= len(t.sel) {
		b, err := t.child.NextBatch()
		if b == nil || err != nil {
			return nil, err
		}
		t.cur, t.sel, t.pos = b, b.Active(), 0
	}
	i := int(t.sel[t.pos])
	t.pos++
	return &executor.Row{Values: t.cur.Row(i)}, nil
}

// FromRows 把行执行器适配成批执行器 (types 为输出列类型)。
type FromRows struct {
	child executor.Executor
	types []tuple.Type
	done  bool
}

func NewFromRows(child executor.Executor, types []tuple.Type) *FromRows {
	return &FromRows{child: child, types: types}
}

func (f *FromRows) Init() error { f.done = false; return f.child.Init() }

func (f *FromRows) NextBatch() (*Batch, error) {
	if f.done {
		return nil, nil
	}
	cols := newCols(f.types, BatchSize)
	n := 0
	for n < BatchSize {
		r, err := f.child.Next()
		if err != nil {
			return nil, err
		}
		if r == nil {
			f.done = true
			break
		}
		for i, v := range r.Values {
			cols[i].AppendValue(v)
		}
		n++
	}
	if n == 0 {
		return nil, nil
	}
	return &Batch{Cols: cols, N: n}, nil
}
