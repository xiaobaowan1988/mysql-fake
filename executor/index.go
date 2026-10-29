package executor

import (
	"bytes"
	"fmt"

	"mysqlfake/btree"
	"mysqlfake/catalog"
	"mysqlfake/sql"
	"mysqlfake/tuple"
	"mysqlfake/txn"
)

// addIndexEntries 为新行值补全各二级索引项 (旧值相同的列不重复)，返回本次真正新增的项 (供回滚撤销)。
// oldVals 为 nil 表示没有旧版本。索引项只增不删: 旧值的索引项保留，读取时回表校验。
func addIndexEntries(ti *catalog.TableInfo, pk []byte, oldVals, newVals []tuple.Value) ([]txn.IdxRef, error) {
	var added []txn.IdxRef
	for _, ix := range ti.Indexes {
		v := newVals[ix.Column]
		if v.Null {
			continue
		}
		if oldVals != nil && tuple.Compare(oldVals[ix.Column], v) == 0 && !oldVals[ix.Column].Null {
			continue
		}
		key, err := tuple.IndexKey(v, pk)
		if err != nil {
			return added, err
		}
		if len(key) > btree.MaxKeySize {
			return added, fmt.Errorf("index key too large for index '%s'", ix.Name)
		}
		if _, exists, err := ix.Tree.Get(key); err != nil {
			return added, err
		} else if exists {
			continue
		}
		if err := ix.Tree.Insert(key, nil); err != nil {
			return added, err
		}
		added = append(added, txn.IdxRef{Tree: ix.Tree, Key: key})
	}
	return added, nil
}

// IndexRange 从 WHERE 的顶层 AND 合取项里提取针对索引列的键范围 [start, end)。
func IndexRange(where sql.Expr, ti *catalog.TableInfo, ix *catalog.IndexInfo, isCol func(*sql.ColumnRef) bool) (start, end []byte, ok bool) {
	col := ti.Schema.Columns[ix.Column]
	var visit func(e sql.Expr)
	visit = func(e sql.Expr) {
		b, isBin := e.(*sql.BinaryExpr)
		if !isBin {
			return
		}
		if b.Op == "AND" {
			visit(b.Left)
			visit(b.Right)
			return
		}
		op, lit := b.Op, (*sql.Literal)(nil)
		switch l, r := b.Left, b.Right; {
		case isPK(l, isCol) && isLit(r):
			lit = r.(*sql.Literal)
		case isPK(r, isCol) && isLit(l):
			lit = l.(*sql.Literal)
			op = flip(op)
		default:
			return
		}
		if lit.Value.Null || lit.Value.Type != col.Type {
			return
		}
		p, err := tuple.EncodeIndexPrefix(lit.Value)
		if err != nil {
			return
		}
		var lo, hi []byte
		switch op {
		case "=":
			lo, hi = p, tuple.PrefixEnd(p)
		case ">":
			lo = tuple.PrefixEnd(p)
		case ">=":
			lo = p
		case "<":
			hi = p
		case "<=":
			hi = tuple.PrefixEnd(p)
		default:
			return
		}
		ok = true
		if lo != nil && (start == nil || bytes.Compare(lo, start) > 0) {
			start = lo
		}
		if hi != nil && (end == nil || bytes.Compare(hi, end) < 0) {
			end = hi
		}
	}
	visit(where)
	return
}

// SecondaryIndexScan 通过二级索引做快照读: 按索引范围取主键，回表取对当前 Read View 可见的版本，
// 并校验该版本的列值仍等于索引项的值 (过滤掉已被修改/删除的陈旧索引项)。
type SecondaryIndexScan struct {
	ctx        *Ctx
	table      *catalog.TableInfo
	idx        *catalog.IndexInfo
	start, end []byte
	it         *btree.Iterator
}

func NewSecondaryIndexScan(ctx *Ctx, t *catalog.TableInfo, ix *catalog.IndexInfo, start, end []byte) *SecondaryIndexScan {
	return &SecondaryIndexScan{ctx: ctx, table: t, idx: ix, start: start, end: end}
}

func (s *SecondaryIndexScan) Init() error {
	s.it = s.idx.Tree.Scan(s.start, s.end)
	return nil
}

func (s *SecondaryIndexScan) Next() (*Row, error) {
	view := s.ctx.Txn.View()
	colType := s.table.Schema.Columns[s.idx.Column].Type
	for s.it.Next() {
		prefix, pk, err := tuple.SplitIndexKey(colType, s.it.Key())
		if err != nil {
			return nil, err
		}
		want, err := tuple.DecodeIndexPrefix(colType, prefix)
		if err != nil {
			return nil, err
		}
		s.ctx.Examined(1)
		raw, found, err := s.table.Tree.Get(pk)
		if err != nil {
			return nil, err
		}
		if !found {
			continue
		}
		data, ok, err := s.ctx.Mgr.Resolve(view, raw)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		vals, err := tuple.Unmarshal(s.table.Schema, data)
		if err != nil {
			return nil, err
		}
		if got := vals[s.idx.Column]; got.Null || tuple.Compare(got, want) != 0 {
			continue // 陈旧索引项
		}
		return &Row{Key: append([]byte(nil), pk...), Values: vals}, nil
	}
	return nil, s.it.Err()
}
