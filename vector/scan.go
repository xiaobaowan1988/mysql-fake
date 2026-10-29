package vector

import (
	"mysqlfake/btree"
	"mysqlfake/catalog"
	"mysqlfake/executor"
	"mysqlfake/tuple"
)

// Scan 是向量化的表扫描 (全表或主键范围 [Start, End))，快照读: 只取对当前 Read View 可见的版本。
// 行数据被直接解码进列缓冲，每批最多 BatchSize 行。
type Scan struct {
	ctx        *executor.Ctx
	table      *catalog.TableInfo
	start, end []byte
	it         *btree.Iterator
	types      []tuple.Type
	done       bool
}

func NewScan(ctx *executor.Ctx, t *catalog.TableInfo, start, end []byte) *Scan {
	types := make([]tuple.Type, len(t.Schema.Columns))
	for i, c := range t.Schema.Columns {
		types[i] = c.Type
	}
	return &Scan{ctx: ctx, table: t, start: start, end: end, types: types}
}

func (s *Scan) Init() error {
	s.it = s.table.Tree.Scan(s.start, s.end)
	s.done = false
	return nil
}

func (s *Scan) NextBatch() (*Batch, error) {
	if s.done {
		return nil, nil
	}
	view := s.ctx.Txn.View()
	cols := newCols(s.types, BatchSize)
	cbs := make([]tuple.ColBuf, len(cols))
	for i, c := range cols {
		cbs[i] = *c
	}
	n := 0
	for n < BatchSize {
		if !s.it.Next() {
			s.done = true
			if err := s.it.Err(); err != nil {
				return nil, err
			}
			break
		}
		s.ctx.Examined(1)
		data, ok, err := s.ctx.Mgr.Resolve(view, s.it.Value())
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		if err := tuple.UnmarshalColumns(s.table.Schema, data, cbs); err != nil {
			return nil, err
		}
		n++
	}
	if n == 0 {
		return nil, nil
	}
	for i := range cols {
		*cols[i] = cbs[i]
	}
	return &Batch{Cols: cols, N: n}, nil
}
