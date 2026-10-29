package executor

import (
	"mysqlfake/btree"
	"mysqlfake/catalog"
	"mysqlfake/tuple"
)

// SeqScan 全表扫描 (遍历聚簇索引的全部叶子)，快照读: 只返回对当前 Read View 可见的版本，不加锁。
type SeqScan struct {
	ctx   *Ctx
	table *catalog.TableInfo
	it    *btree.Iterator
}

func NewSeqScan(ctx *Ctx, t *catalog.TableInfo) *SeqScan { return &SeqScan{ctx: ctx, table: t} }

func (s *SeqScan) Init() error {
	s.it = s.table.Tree.Scan(nil, nil)
	return nil
}

func (s *SeqScan) Next() (*Row, error) { return nextVisible(s.ctx, s.table, s.it) }

// IndexScan 通过主键 B+ 树扫描 [Start, End) 范围 (nil 表示无界)，同样是快照读。
type IndexScan struct {
	ctx        *Ctx
	table      *catalog.TableInfo
	Start, End []byte
	it         *btree.Iterator
}

func NewIndexScan(ctx *Ctx, t *catalog.TableInfo, start, end []byte) *IndexScan {
	return &IndexScan{ctx: ctx, table: t, Start: start, End: end}
}

func (s *IndexScan) Init() error {
	s.it = s.table.Tree.Scan(s.Start, s.End)
	return nil
}

func (s *IndexScan) Next() (*Row, error) { return nextVisible(s.ctx, s.table, s.it) }

// nextVisible 跳过对当前 Read View 不可见 (尚未提交/已删除) 的行。
func nextVisible(ctx *Ctx, t *catalog.TableInfo, it *btree.Iterator) (*Row, error) {
	view := ctx.Txn.View()
	for it.Next() {
		ctx.Examined(1)
		data, ok, err := ctx.Mgr.Resolve(view, it.Value())
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		vals, err := tuple.Unmarshal(t.Schema, data)
		if err != nil {
			return nil, err
		}
		return &Row{Key: it.Key(), Values: vals}, nil
	}
	return nil, it.Err()
}

// Filter 过滤满足谓词的行。
type Filter struct {
	child Executor
	pred  Expr
}

func NewFilter(child Executor, pred Expr) *Filter { return &Filter{child, pred} }

func (f *Filter) Init() error { return f.child.Init() }

func (f *Filter) Next() (*Row, error) {
	for {
		r, err := f.child.Next()
		if r == nil || err != nil {
			return nil, err
		}
		if EvalBool(f.pred, r.Values) {
			return r, nil
		}
	}
}

// Projection 按列下标重排/裁剪输出列。
type Projection struct {
	child Executor
	cols  []int
}

func NewProjection(child Executor, cols []int) *Projection { return &Projection{child, cols} }

func (p *Projection) Init() error { return p.child.Init() }

func (p *Projection) Next() (*Row, error) {
	r, err := p.child.Next()
	if r == nil || err != nil {
		return nil, err
	}
	out := make([]tuple.Value, len(p.cols))
	for i, c := range p.cols {
		out[i] = r.Values[c]
	}
	return &Row{Key: r.Key, Values: out}, nil
}
