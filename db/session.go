package db

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"mysqlfake/executor"
	"mysqlfake/metrics"
	"mysqlfake/optimizer"
	"mysqlfake/sql"
	"mysqlfake/tuple"
	"mysqlfake/txn"
)

// Session 代表一个连接。默认自动提交: 每条语句是一个独立事务；
// BEGIN 之后进入显式事务，直到 COMMIT / ROLLBACK。一个 Session 不能被多个 goroutine 同时使用。
type Session struct {
	eng *Engine
	tx  *txn.Txn // 显式事务，nil 表示自动提交

	lastProf *Profile
	prof     *Profile // 当前语句的画像 (执行期间)
	stats    executor.ExecStats

	noAutocommit bool // SET autocommit = 0: 下一条语句隐式开启事务，直到 COMMIT / ROLLBACK
}

// Autocommit 返回会话是否处于自动提交模式。
func (s *Session) Autocommit() bool { return !s.noAutocommit }

// SetAutocommit 对应 SET autocommit = 0/1。重新打开自动提交时隐式提交当前事务。
func (s *Session) SetAutocommit(on bool) error {
	if on && s.noAutocommit && s.tx != nil {
		if err := s.endTxn(true); err != nil {
			return err
		}
	}
	s.noAutocommit = !on
	return nil
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
	start := time.Now()
	prof := &Profile{SQL: query, Type: "other"}
	s.prof = prof
	s.stats.RowsExamined.Store(0)
	st, perr := sql.Parse(query)
	prof.Parse = time.Since(start)
	var res *Result
	var err error
	if perr != nil {
		err = perr
	} else {
		prof.Type = stmtType(st)
		res, err = s.execStmt(st, query)
	}
	prof.Total = time.Since(start)
	prof.Exec = prof.Total - prof.Parse - prof.Plan
	prof.RowsExamined = s.stats.RowsExamined.Load()
	if res != nil && res.IsQuery {
		prof.RowsSent = int64(len(res.Rows))
	}
	prof.Err = err
	s.lastProf, s.prof = prof, nil
	status := "ok"
	if err != nil {
		status = "error"
	}
	metrics.Statements.With(prof.Type, status).Inc()
	metrics.StatementSeconds.With(prof.Type).Observe(prof.Total.Seconds())
	metrics.RowsSent.Add(uint64(prof.RowsSent))
	metrics.RowsExamined.Add(uint64(prof.RowsExamined))
	s.eng.slow.record(prof)
	return res, err
}

func (s *Session) execStmt(st sql.Statement, query string) (*Result, error) {
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
		if s.tx != nil { // DDL 隐式提交当前事务 (与 MySQL 一致)
			if err := s.endTxn(true); err != nil {
				return nil, err
			}
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
		return s.run(true, func(ctx *executor.Ctx) (*Result, error) {
			// 先加元数据锁再读取 TableInfo，保证与 Online DDL 的切换互斥
			if err := ctx.Txn.LockMDL(st.Table); err != nil {
				return nil, err
			}
			ti, err := e.cat.GetTable(st.Table)
			if err != nil {
				return nil, err
			}
			rows, err := executor.BindInsert(st, ti)
			if err != nil {
				return nil, err
			}
			ins := executor.NewInsert(ctx, ti, rows)
			if err := ins.Init(); err != nil {
				return nil, err
			}
			_, err = ins.Next()
			return &Result{Affected: ins.Affected}, err
		})
	case *sql.UpdateStmt:
		return s.run(true, func(ctx *executor.Ctx) (*Result, error) {
			if err := ctx.Txn.LockMDL(st.Table); err != nil {
				return nil, err
			}
			ti, err := e.cat.GetTable(st.Table)
			if err != nil {
				return nil, err
			}
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
		return s.run(true, func(ctx *executor.Ctx) (*Result, error) {
			if err := ctx.Txn.LockMDL(st.Table); err != nil {
				return nil, err
			}
			ti, err := e.cat.GetTable(st.Table)
			if err != nil {
				return nil, err
			}
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
	case *sql.BackupStmt:
		if s.tx != nil {
			if err := s.endTxn(true); err != nil {
				return nil, err
			}
		}
		m, err := e.Backup(st.Dir, st.From)
		if err != nil {
			return nil, err
		}
		return &Result{IsQuery: true, Columns: []string{"backup_id", "kind", "pages", "end_lsn", "binlog_seq"},
			Rows: [][]tuple.Value{{tuple.StrValue(m.ID), tuple.StrValue(m.Kind), tuple.IntValue(int64(m.PagesCopied)),
				tuple.IntValue(int64(m.EndLSN)), tuple.IntValue(int64(m.BinlogSeqEnd))}}}, nil
	case *sql.CreateIndexStmt, *sql.AlterTableStmt:
		if s.tx != nil { // DDL 隐式提交当前事务
			if err := s.endTxn(true); err != nil {
				return nil, err
			}
		}
		if err := e.checkWrite(); err != nil {
			return nil, err
		}
		return &Result{}, e.runDDL(st, false)
	case *sql.SelectStmt:
		if st.Lock != sql.LockNone {
			return s.lockingSelect(st)
		}
		return s.run(false, func(ctx *executor.Ctx) (*Result, error) { return s.query(ctx, st) })
	case *sql.ExplainStmt:
		return s.run(false, func(ctx *executor.Ctx) (*Result, error) {
			plan, err := s.plan(ctx, st.Select, st.Analyze)
			if err != nil {
				return nil, err
			}
			lines := plan.Explain()
			if st.Analyze { // 真正执行一遍 (丢弃结果行)，再给出每个算子的实际行数与耗时
				start := time.Now()
				if err := plan.Root.Init(); err != nil {
					return nil, err
				}
				sent := 0
				for {
					r, err := plan.Root.Next()
					if err != nil {
						return nil, err
					}
					if r == nil {
						break
					}
					sent++
				}
				lines = append(plan.ExplainAnalyze(), fmt.Sprintf("Execution: rows returned=%d, rows examined=%d, time=%.3fms",
					sent, s.stats.RowsExamined.Load(), float64(time.Since(start))/1e6))
			}
			res := &Result{Columns: []string{"QUERY PLAN"}, IsQuery: true}
			for _, l := range lines {
				res.Rows = append(res.Rows, []tuple.Value{tuple.StrValue(l)})
			}
			return res, nil
		})
	case *sql.AnalyzeStmt:
		if err := e.Analyze(st.Table); err != nil {
			return nil, err
		}
		return &Result{}, nil
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
	if s.noAutocommit && s.tx == nil { // autocommit=0: 隐式开启事务
		tx, err := s.eng.mgr.Begin()
		if err != nil {
			return nil, err
		}
		s.tx = tx
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
	lw0 := tx.LockWaitTime()
	res, err := fn(&executor.Ctx{Txn: tx, Mgr: s.eng.mgr, Stats: &s.stats})
	if s.prof != nil {
		s.prof.LockWait += tx.LockWaitTime() - lw0
	}
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

func (s *Session) plan(ctx *executor.Ctx, st *sql.SelectStmt, instrument bool) (*optimizer.Plan, error) {
	start := time.Now()
	defer func() {
		if s.prof != nil {
			s.prof.Plan += time.Since(start)
		}
	}()
	e := s.eng
	if len(st.From) > 1 {
		names := make([]string, len(st.From))
		for i, f := range st.From {
			names[i] = f.Name
		}
		e.ensureStats(names)
	}
	opts := e.statsOpt.Planner
	opts.Instrument = instrument
	pl := &optimizer.Planner{Cat: e.cat, Stats: e.stats, Opts: opts}
	return pl.PlanSelect(ctx, st)
}

// query 用优化器规划并执行一条普通 (快照读) SELECT。
func (s *Session) query(ctx *executor.Ctx, st *sql.SelectStmt) (*Result, error) {
	plan, err := s.plan(ctx, st, false)
	if err != nil {
		return nil, err
	}
	if err := plan.Root.Init(); err != nil {
		return nil, err
	}
	res := &Result{Columns: plan.Columns, IsQuery: true}
	for {
		r, err := plan.Root.Next()
		if err != nil {
			return nil, err
		}
		if r == nil {
			return res, nil
		}
		res.Rows = append(res.Rows, r.Values)
	}
}

// lockingSelect: SELECT ... FOR SHARE/UPDATE 是单表的当前读，使用基础执行器。
func (s *Session) lockingSelect(st *sql.SelectStmt) (*Result, error) {
	if len(st.From) != 1 || len(st.OrderBy) > 0 || st.Limit >= 0 {
		return nil, fmt.Errorf("FOR SHARE/UPDATE supports a single table without ORDER BY/LIMIT")
	}
	return s.run(true, func(ctx *executor.Ctx) (*Result, error) {
		if err := ctx.Txn.LockMDL(st.From[0].Name); err != nil {
			return nil, err
		}
		ti, err := s.eng.cat.GetTable(st.From[0].Name)
		if err != nil {
			return nil, err
		}
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
