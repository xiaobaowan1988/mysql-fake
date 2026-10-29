package db

import (
	"errors"
	"fmt"
	"strings"

	"mysqlfake/executor"
	"mysqlfake/sql"
	"mysqlfake/tuple"
	"mysqlfake/txn"
)

// Session 代表一个连接。默认自动提交: 每条语句是一个独立事务；
// BEGIN 之后进入显式事务，直到 COMMIT / ROLLBACK。一个 Session 不能被多个 goroutine 同时使用。
type Session struct {
	eng *Engine
	tx  *txn.Txn // 显式事务，nil 表示自动提交
}

// InTxn 返回会话当前是否处于显式事务中。
func (s *Session) InTxn() bool { return s.tx != nil }

// Close 结束会话；未提交的显式事务被回滚。
func (s *Session) Close() {
	if s.tx != nil {
		s.tx.Rollback()
		s.tx = nil
	}
}

func (s *Session) Exec(query string) (*Result, error) {
	st, err := sql.Parse(query)
	if err != nil {
		return nil, err
	}
	e := s.eng
	switch st := st.(type) {
	case *sql.BeginStmt:
		if s.tx != nil { // 与 MySQL 一致: 隐式提交上一个事务
			if err := s.endTxn(true); err != nil {
				return nil, err
			}
		}
		tx, err := e.mgr.Begin()
		if err != nil {
			return nil, err
		}
		s.tx = tx
		return &Result{}, nil
	case *sql.CommitStmt:
		return &Result{}, s.endTxn(true)
	case *sql.RollbackStmt:
		return &Result{}, s.endTxn(false)
	case *sql.CreateTableStmt:
		if s.tx != nil {
			return nil, errors.New("DDL is not allowed inside a transaction")
		}
		cols := make([]tuple.Column, len(st.Columns))
		for i, c := range st.Columns {
			cols[i] = tuple.Column{Name: c.Name, Type: c.Type, Len: c.Len, PrimaryKey: c.PrimaryKey}
		}
		tx, err := e.mgr.Begin()
		if err != nil {
			return nil, err
		}
		if err := e.checkWrite(); err != nil {
			tx.Rollback()
			return nil, err
		}
		tx.MarkWrote()
		_, err = e.cat.CreateTable(st.Table, cols)
		if err != nil {
			tx.Rollback()
			return nil, err
		}
		tx.AddDDLEvent(strings.TrimSpace(query))
		if err := tx.Commit(); err != nil {
			e.cat.DropTable(st.Table) // 复制/日志失败: 撤销本地建表
			return nil, err
		}
		return &Result{}, nil
	case *sql.ShowTablesStmt:
		res := &Result{Columns: []string{"Tables"}, IsQuery: true}
		for _, n := range e.cat.ListTables() {
			res.Rows = append(res.Rows, []tuple.Value{tuple.StrValue(n)})
		}
		return res, nil
	case *sql.InsertStmt:
		ti, err := e.cat.GetTable(st.Table)
		if err != nil {
			return nil, err
		}
		rows, err := executor.BindInsert(st, ti)
		if err != nil {
			return nil, err
		}
		return s.run(true, func(ctx *executor.Ctx) (*Result, error) {
			ins := executor.NewInsert(ctx, ti, rows)
			if err := ins.Init(); err != nil {
				return nil, err
			}
			_, err := ins.Next()
			return &Result{Affected: ins.Affected}, err
		})
	case *sql.UpdateStmt:
		ti, err := e.cat.GetTable(st.Table)
		if err != nil {
			return nil, err
		}
		return s.run(true, func(ctx *executor.Ctx) (*Result, error) {
			up, err := executor.PlanUpdate(ctx, st, ti)
			if err != nil {
				return nil, err
			}
			if err := up.Init(); err != nil {
				return nil, err
			}
			_, err = up.Next()
			return &Result{Affected: up.Affected}, err
		})
	case *sql.DeleteStmt:
		ti, err := e.cat.GetTable(st.Table)
		if err != nil {
			return nil, err
		}
		return s.run(true, func(ctx *executor.Ctx) (*Result, error) {
			del, err := executor.PlanDelete(ctx, st, ti)
			if err != nil {
				return nil, err
			}
			if err := del.Init(); err != nil {
				return nil, err
			}
			_, err = del.Next()
			return &Result{Affected: del.Affected}, err
		})
	case *sql.SelectStmt:
		ti, err := e.cat.GetTable(st.Table)
		if err != nil {
			return nil, err
		}
		return s.run(st.Lock != sql.LockNone, func(ctx *executor.Ctx) (*Result, error) {
			ex, names, err := executor.PlanSelect(ctx, st, ti)
			if err != nil {
				return nil, err
			}
			if err := ex.Init(); err != nil {
				return nil, err
			}
			res := &Result{Columns: names, IsQuery: true}
			for {
				r, err := ex.Next()
				if err != nil {
					return nil, err
				}
				if r == nil {
					return res, nil
				}
				res.Rows = append(res.Rows, r.Values)
			}
		})
	}
	return nil, fmt.Errorf("unsupported statement")
}

func (s *Session) endTxn(commit bool) error {
	if s.tx == nil {
		return nil
	}
	tx := s.tx
	s.tx = nil
	if commit {
		return tx.Commit()
	}
	return tx.Rollback()
}

// run 在事务上下文中执行一条语句。
//   - 自动提交: 语句自成事务，出错整体回滚，成功则提交 (刷 WAL)。
//   - 显式事务: 语句出错只回滚该语句 (Undo 保存点)；死锁/锁超时则回滚整个事务。
func (s *Session) run(write bool, fn func(ctx *executor.Ctx) (*Result, error)) (*Result, error) {
	if write {
		if err := s.eng.checkWrite(); err != nil {
			return nil, err
		}
	}
	auto := s.tx == nil
	tx := s.tx
	if auto {
		var err error
		if tx, err = s.eng.mgr.Begin(); err != nil {
			return nil, err
		}
	}
	sp := tx.Savepoint()
	res, err := fn(&executor.Ctx{Txn: tx, Mgr: s.eng.mgr})
	switch {
	case err == nil:
		if auto {
			if cerr := tx.Commit(); cerr != nil {
				return nil, cerr
			}
		}
		return res, nil
	case auto:
		tx.Rollback()
	case errors.Is(err, txn.ErrDeadlock) || errors.Is(err, txn.ErrLockTimeout):
		s.tx = nil
		tx.Rollback()
	default:
		if rerr := tx.RollbackTo(sp); rerr != nil {
			err = fmt.Errorf("%w (statement rollback failed: %v)", err, rerr)
		}
	}
	return nil, err
}
