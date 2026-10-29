package db

import (
	"fmt"

	"mysqlfake/binlog"
	"mysqlfake/catalog"
	"mysqlfake/executor"
	"mysqlfake/sql"
	"mysqlfake/tuple"
)

// ApplyBinlog 把一个来自复制或 Binlog 重放的事务原子地应用到本引擎 (幂等)。
// 应用产生的事务不会再次触发复制钩子，但会写本地 Binlog。
func (e *Engine) ApplyBinlog(rec *binlog.Txn) error {
	if len(rec.Events) == 1 && rec.Events[0].Op == binlog.OpDDL {
		// 在线 DDL (CREATE INDEX / ADD COLUMN) 自己管理事务与元数据锁
		if st, err := sql.Parse(rec.Events[0].SQL); err == nil {
			switch st.(type) {
			case *sql.CreateIndexStmt, *sql.AlterTableStmt:
				return e.runDDL(st, true)
			}
		}
	}
	tx, err := e.mgr.Begin()
	if err != nil {
		return err
	}
	tx.SetFromReplication()
	ctx := &executor.Ctx{Txn: tx, Mgr: e.mgr}
	for _, ev := range rec.Events {
		if err = e.applyEvent(ctx, ev); err != nil {
			tx.Rollback()
			return fmt.Errorf("apply binlog txn %d: %w", rec.TxnID, err)
		}
	}
	return tx.Commit()
}

func (e *Engine) applyEvent(ctx *executor.Ctx, ev binlog.Event) error {
	switch ev.Op {
	case binlog.OpDDL:
		st, err := sql.Parse(ev.SQL)
		if err != nil {
			return err
		}
		ctx.Txn.AddDDLEvent(ev.SQL)
		switch st := st.(type) {
		case *sql.CreateTableStmt:
			cols := make([]tuple.Column, len(st.Columns))
			for i, c := range st.Columns {
				cols[i] = tuple.Column{Name: c.Name, Type: c.Type, Len: c.Len, PrimaryKey: c.PrimaryKey}
			}
			if _, err := e.cat.CreateTable(st.Table, cols); err != nil && !catalog.IsTableExists(err) {
				return err
			}
			return nil
		}
		return fmt.Errorf("unsupported DDL in binlog: %q", ev.SQL)
	case binlog.OpPut, binlog.OpDelete:
		if err := ctx.Txn.LockMDL(ev.Table); err != nil {
			return err
		}
		ti, err := e.cat.GetTable(ev.Table)
		if err != nil {
			return err
		}
		if ev.Op == binlog.OpPut {
			return executor.ApplyPut(ctx, ti, ev.Key, ev.Row)
		}
		return executor.ApplyDelete(ctx, ti, ev.Key)
	}
	return fmt.Errorf("unknown binlog op %d", ev.Op)
}
