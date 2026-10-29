package executor

import (
	"mysqlfake/catalog"
	"mysqlfake/txn"
)

// ApplyPut 把一条来自复制/回放的 Put 事件写入表: 加 X 锁，键不存在则插入，否则在其上建立新版本。
// 与 Binlog 事件语义一致，因此重复应用是幂等的。
func ApplyPut(ctx *Ctx, ti *catalog.TableInfo, key, row []byte) error {
	tx, tree := ctx.Txn, ti.Tree
	if err := tx.Lock(lockKey(ti.Name, key), txn.LockX); err != nil {
		return err
	}
	cur, found, err := tree.Get(key)
	if err != nil {
		return err
	}
	if !found {
		tx.AddUndo(txn.UndoInsert, ti.Name, tree, key, nil, row)
		return tree.Insert(key, txn.RowVersion{TxnID: tx.ID, Data: row}.Encode())
	}
	undo := tx.AddUndo(txn.UndoUpdate, ti.Name, tree, key, cur, row)
	return tree.Put(key, txn.RowVersion{TxnID: tx.ID, RollPtr: undo, Data: row}.Encode())
}

// ApplyDelete 应用 Delete 事件: 行不存在或已被删除则什么也不做。
func ApplyDelete(ctx *Ctx, ti *catalog.TableInfo, key []byte) error {
	tx, tree := ctx.Txn, ti.Tree
	if err := tx.Lock(lockKey(ti.Name, key), txn.LockX); err != nil {
		return err
	}
	cur, found, err := tree.Get(key)
	if err != nil || !found {
		return err
	}
	v, err := txn.DecodeVersion(cur)
	if err != nil || v.Deleted {
		return err
	}
	undo := tx.AddUndo(txn.UndoDelete, ti.Name, tree, key, cur, nil)
	return tree.Put(key, txn.RowVersion{TxnID: tx.ID, RollPtr: undo, Deleted: true, Data: v.Data}.Encode())
}
