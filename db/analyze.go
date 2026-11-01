package db

import (
	"fmt"
	"sync"
	"time"

	"mysqlfake/executor"
	"mysqlfake/optimizer"
	"mysqlfake/sql"
	"mysqlfake/stats"
	"mysqlfake/tuple"
)

// Stats 返回统计信息存储 (供优化器与测试使用)。
func (e *Engine) Stats() *stats.Store { return e.stats }

// Analyze 对表做一次快照扫描 + 蓄水池抽样，并更新统计信息。
func (e *Engine) Analyze(table string) error {
	ti, err := e.cat.GetTable(table)
	if err != nil {
		return err
	}
	size := e.statsOpt.StatsSampleSize
	if size <= 0 {
		size = 10000
	}
	modsAtStart := e.stats.Mods(ti.Name) // 采样期间到达的修改要保留 (见 Store.SetSampled)
	tx, err := e.mgr.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback() // 只读事务
	ctx := &executor.Ctx{Txn: tx, Mgr: e.mgr}
	scan := executor.NewSeqScan(ctx, ti)
	if err := scan.Init(); err != nil {
		return err
	}
	sm := stats.NewSampler(size, uint64(time.Now().UnixNano()))
	for {
		r, err := scan.Next()
		if err != nil {
			return err
		}
		if r == nil {
			break
		}
		n := 0
		if data, err := tuple.Marshal(ti.Schema, r.Values); err == nil {
			n = len(data)
		}
		sm.Add(r.Values, n)
	}
	e.stats.SetSampled(ti.Name, sm.Build(ti.Schema), modsAtStart)
	return nil
}

// startStatsDaemon 启动后台守护协程: 定期检查各表，从未分析或修改量过大的表重新采样。
func (e *Engine) startStatsDaemon(interval time.Duration) (stop func()) {
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-t.C:
				for _, name := range e.cat.ListTables() {
					select {
					case <-done:
						return
					default:
					}
					if e.stats.NeedsAnalyze(name) {
						e.Analyze(name)
					}
				}
			}
		}
	}()
	var once sync.Once
	return func() { once.Do(func() { close(done); wg.Wait() }) }
}

// ensureStats 保证多表查询用到的表都有统计信息 (没有则立即采样一次)。
func (e *Engine) ensureStats(tables []string) {
	for _, t := range tables {
		if e.stats.Get(t) == nil {
			e.Analyze(t)
		}
	}
}

// SetPlannerOptions 修改优化器选项 (测试/诊断用，不是并发安全的，请在没有查询运行时调用)。
func (e *Engine) SetPlannerOptions(o optimizer.Options) { e.statsOpt.Planner = o }

// PlanQuery 只规划不执行，返回计划 (测试/诊断用)。
func (e *Engine) PlanQuery(query string) (*optimizer.Plan, error) {
	st, err := sql.Parse(query)
	if err != nil {
		return nil, err
	}
	sel, ok := st.(*sql.SelectStmt)
	if !ok {
		return nil, fmt.Errorf("PlanQuery expects a SELECT")
	}
	tx, err := e.mgr.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	return e.def.plan(&executor.Ctx{Txn: tx, Mgr: e.mgr}, sel, false)
}

// Describe 返回表的列名和索引名 (诊断/测试用)。
func (e *Engine) Describe(table string) (columns, indexes []string, err error) {
	ti, err := e.cat.GetTable(table)
	if err != nil {
		return nil, nil, err
	}
	for _, c := range ti.Schema.Columns {
		columns = append(columns, c.Name)
	}
	for _, ix := range ti.Indexes {
		indexes = append(indexes, ix.Name)
	}
	return columns, indexes, nil
}
