package executor

import (
	"errors"
	"fmt"

	"mysqlfake/btree"
	"mysqlfake/catalog"
	"mysqlfake/tuple"
	"mysqlfake/txn"
)

// LockScan 是"当前读"扫描: 对每个候选行先加锁 (S 或 X，2PL，持有到事务结束)，
// 再读取最新版本，跳过已删除的行并用最新值重新校验谓词。
// 候选键来自对 B+ 树的原始遍历 (包含他人未提交的插入/删除标记)，因此会在对方事务结束前阻塞。
type LockScan struct {
	ctx        *Ctx
	table      *catalog.TableInfo
	start, end []byte
	pred       Expr
	mode       txn.LockMode
	it         *btree.Iterator
}

func NewLockScan(ctx *Ctx, t *catalog.TableInfo, start, end []byte, pred Expr, mode txn.LockMode) *LockScan {
	return &LockScan{ctx: ctx, table: t, start: start, end: end, pred: pred, mode: mode}
}

func (s *LockScan) Init() error {
	s.it = s.table.Tree.Scan(s.start, s.end)
	return nil
}

func (s *LockScan) Next() (*Row, error) {
	for s.it.Next() {
		key := append([]byte(nil), s.it.Key()...)
		if err := s.ctx.Txn.Lock(lockKey(s.table.Name, key), s.mode); err != nil {
			return nil, err
		}
		raw, ok, err := s.table.Tree.Get(key)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue // 他人的插入已回滚
		}
		ver, err := txn.DecodeVersion(raw)
		if err != nil {
			return nil, err
		}
		if ver.Deleted {
			continue
		}
		vals, err := tuple.Unmarshal(s.table.Schema, ver.Data)
		if err != nil {
			return nil, err
		}
		if s.pred != nil && !EvalBool(s.pred, vals) {
			continue
		}
		return &Row{Key: key, Values: vals, Raw: raw}, nil
	}
	return nil, s.it.Err()
}

// Insert 把预先绑定好的行写入表。第一次 Next 执行全部插入并返回 (nil, nil)。
// 对键加 X 锁后: 键不存在 -> 插入新行 (undo=Insert)；键上是已删除标记 -> 在其上建立新版本 (undo=Update)。
type Insert struct {
	ctx      *Ctx
	table    *catalog.TableInfo
	rows     [][]tuple.Value
	Affected int
	done     bool
}

func NewInsert(ctx *Ctx, t *catalog.TableInfo, rows [][]tuple.Value) *Insert {
	return &Insert{ctx: ctx, table: t, rows: rows}
}

func (in *Insert) Init() error { in.done, in.Affected = false, 0; return nil }

func (in *Insert) Next() (*Row, error) {
	if in.done {
		return nil, nil
	}
	in.done = true
	tx, tree := in.ctx.Txn, in.table.Tree
	for _, vals := range in.rows {
		key, err := tuple.EncodeKey(vals[in.table.PK])
		if err != nil {
			return nil, err
		}
		data, err := tuple.Marshal(in.table.Schema, vals)
		if err != nil {
			return nil, err
		}
		if err := tx.Lock(lockKey(in.table.Name, key), txn.LockX); err != nil {
			return nil, err
		}
		cur, found, err := tree.Get(key)
		if err != nil {
			return nil, err
		}
		if !found {
			tx.AddUndo(txn.UndoInsert, tree, key, nil)
			err = tree.Insert(key, txn.RowVersion{TxnID: tx.ID, Data: data}.Encode())
		} else {
			old, derr := txn.DecodeVersion(cur)
			if derr != nil {
				return nil, derr
			}
			if !old.Deleted {
				return nil, fmt.Errorf("duplicate entry '%s' for key 'PRIMARY'", vals[in.table.PK])
			}
			undo := tx.AddUndo(txn.UndoUpdate, tree, key, cur)
			err = tree.Put(key, txn.RowVersion{TxnID: tx.ID, RollPtr: undo, Data: data}.Encode())
		}
		if err != nil {
			if errors.Is(err, btree.ErrValueTooLarge) {
				return nil, errors.New("row too large")
			}
			return nil, err
		}
		in.Affected++
	}
	return nil, nil
}

type assignment struct {
	col  int
	expr Expr
}

// Update 对子执行器 (LockScan, X 锁) 产生的每一行生成新版本。
type Update struct {
	ctx      *Ctx
	table    *catalog.TableInfo
	child    Executor
	sets     []assignment
	Affected int
	done     bool
}

func (u *Update) Init() error { u.done, u.Affected = false, 0; return u.child.Init() }

func (u *Update) Next() (*Row, error) {
	if u.done {
		return nil, nil
	}
	u.done = true
	tx, tree := u.ctx.Txn, u.table.Tree
	for {
		row, err := u.child.Next()
		if err != nil {
			return nil, err
		}
		if row == nil {
			return nil, nil
		}
		next := append([]tuple.Value(nil), row.Values...)
		for _, a := range u.sets {
			v := a.expr.Eval(row.Values) // SET 右侧都基于更新前的行
			if v.Null {
				v = tuple.NullValue(u.table.Schema.Columns[a.col].Type)
			}
			next[a.col] = v
		}
		data, err := tuple.Marshal(u.table.Schema, next)
		if err != nil {
			return nil, err
		}
		undo := tx.AddUndo(txn.UndoUpdate, tree, row.Key, row.Raw)
		if err := tree.Put(row.Key, txn.RowVersion{TxnID: tx.ID, RollPtr: undo, Data: data}.Encode()); err != nil {
			return nil, err
		}
		u.Affected++
	}
}

// Delete 给子执行器产生的每一行打上删除标记 (保留旧数据，旧版本仍可通过 undo 链被快照读看到)。
type Delete struct {
	ctx      *Ctx
	table    *catalog.TableInfo
	child    Executor
	Affected int
	done     bool
}

func (d *Delete) Init() error { d.done, d.Affected = false, 0; return d.child.Init() }

func (d *Delete) Next() (*Row, error) {
	if d.done {
		return nil, nil
	}
	d.done = true
	tx, tree := d.ctx.Txn, d.table.Tree
	for {
		row, err := d.child.Next()
		if err != nil {
			return nil, err
		}
		if row == nil {
			return nil, nil
		}
		cur, err := txn.DecodeVersion(row.Raw)
		if err != nil {
			return nil, err
		}
		undo := tx.AddUndo(txn.UndoDelete, tree, row.Key, row.Raw)
		if err := tree.Put(row.Key, txn.RowVersion{TxnID: tx.ID, RollPtr: undo, Deleted: true, Data: cur.Data}.Encode()); err != nil {
			return nil, err
		}
		d.Affected++
	}
}
