package db

import (
	"fmt"
	"strings"
	"sync"

	"mysqlfake/binlog"
	"mysqlfake/btree"
	"mysqlfake/catalog"
	"mysqlfake/executor"
	"mysqlfake/sql"
	"mysqlfake/tuple"
	"mysqlfake/txn"
)

// capture 订阅已提交事务，记录某张表的行事件 (Online DDL 的"增量日志")。
type capture struct {
	table  string
	mu     sync.Mutex
	events []binlog.Event
	cancel func()
}

func (e *Engine) startCapture(table string) *capture {
	c := &capture{table: strings.ToLower(table)}
	c.cancel = e.mgr.Subscribe(func(rec *binlog.Txn) {
		c.mu.Lock()
		for _, ev := range rec.Events {
			if ev.Op != binlog.OpDDL && strings.EqualFold(ev.Table, c.table) {
				c.events = append(c.events, ev)
			}
		}
		c.mu.Unlock()
	})
	return c
}

func (c *capture) drain() []binlog.Event {
	c.mu.Lock()
	defer c.mu.Unlock()
	evs := c.events
	c.events = nil
	return evs
}

func (c *capture) stop() { c.cancel() }

// SetDDLHook 设置 Online DDL 各阶段的测试钩子 (stage: "copy" 每拷贝一批、"replay" 每轮回放、"before-cutover")。
func (e *Engine) SetDDLHook(f func(stage string)) {
	e.ddlMu2.Lock()
	e.ddlHook = f
	e.ddlMu2.Unlock()
}

func (e *Engine) hook(stage string) {
	e.ddlMu2.Lock()
	h := e.ddlHook
	e.ddlMu2.Unlock()
	if h != nil {
		h(stage)
	}
}

// runDDL 执行 CREATE INDEX / ALTER TABLE ADD COLUMN。fromReplication 为真时表示由复制日志/Binlog 回放触发:
// 不再向外复制，且对"已经存在"的对象视为成功 (幂等)。
func (e *Engine) runDDL(st sql.Statement, fromReplication bool) error {
	e.ddlMu.Lock() // 同一时刻只允许一个在线 DDL
	defer e.ddlMu.Unlock()
	switch st := st.(type) {
	case *sql.CreateIndexStmt:
		return e.createIndex(st, fromReplication)
	case *sql.AlterTableStmt:
		return e.addColumn(st, fromReplication)
	}
	return fmt.Errorf("unsupported DDL")
}

// copyTable 用一个新的读事务 (快照) 扫描表，对每个可见行调用 fn。
func (e *Engine) copyTable(ti *catalog.TableInfo, fn func(r *executor.Row) error) error {
	tx, err := e.mgr.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	scan := executor.NewSeqScan(&executor.Ctx{Txn: tx, Mgr: e.mgr}, ti)
	if err := scan.Init(); err != nil {
		return err
	}
	for n := 1; ; n++ {
		r, err := scan.Next()
		if err != nil {
			return err
		}
		if r == nil {
			return nil
		}
		if err := fn(r); err != nil {
			return err
		}
		if n%256 == 0 {
			e.hook("copy")
		}
	}
}

// compact 对一批行事件按主键去重，只保留每个键最后一次事件 (回放的是"最终状态"，中间状态无关紧要)。
func compact(evs []binlog.Event) []binlog.Event {
	last := make(map[string]int, len(evs))
	for i, ev := range evs {
		last[ev.Table+"\x00"+string(ev.Key)] = i
	}
	out := make([]binlog.Event, 0, len(last))
	for i, ev := range evs {
		if last[ev.Table+"\x00"+string(ev.Key)] == i {
			out = append(out, ev)
		}
	}
	return out
}

// finishDDL 完成切换: 对表加元数据锁 X (等待已触及该表的事务结束)，回放最后的增量，
// 在同一个 DDL 事务里提交 DDL 事件 (复制 + Binlog)，并在释放 X 之前执行 swap。
func (e *Engine) finishDDL(table, ddlSQL string, fromReplication bool, cap *capture, replay func(evs []binlog.Event) error, swap func(ddlTxnID uint64) error) error {
	// 多轮回放直到积压很小，缩短持有 X 锁的时间。如果写入速度超过回放速度 (积压不再缩小)，
	// 继续回放只会越追越远，直接进入切换: 持有 X 锁期间写入被阻塞，最后一次回放必然能结束。
	prev := -1
	for round := 0; round < 8; round++ {
		evs := compact(cap.drain())
		if err := replay(evs); err != nil {
			return err
		}
		e.hook("replay")
		if len(evs) < 32 || (prev >= 0 && len(evs) >= prev) {
			break
		}
		prev = len(evs)
	}
	e.hook("before-cutover")
	tx, err := e.mgr.Begin()
	if err != nil {
		return err
	}
	if err := tx.Lock(txn.MDLResource(table), txn.LockX); err != nil {
		tx.Rollback()
		return fmt.Errorf("online ddl: cannot acquire metadata lock on %s: %w", table, err)
	}
	if err := replay(compact(cap.drain())); err != nil {
		tx.Rollback()
		return err
	}
	tx.MarkWrote()
	tx.AddDDLEvent(ddlSQL)
	if fromReplication {
		tx.SetFromReplication()
	}
	tx.SetOnCommit(func() error { return swap(tx.ID) })
	return tx.Commit() // 复制失败则不切换，Commit 会回滚并释放 X
}

func (e *Engine) createIndex(st *sql.CreateIndexStmt, fromReplication bool) error {
	ti, err := e.cat.GetTable(st.Table)
	if err != nil {
		return err
	}
	col := ti.Schema.ColumnIndex(st.Column)
	if col < 0 {
		return fmt.Errorf("unknown column '%s'", st.Column)
	}
	if col == ti.PK {
		return fmt.Errorf("column '%s' is the primary key and is already indexed", st.Column)
	}
	for _, ix := range ti.Indexes {
		if strings.EqualFold(ix.Name, st.Name) {
			if fromReplication {
				return nil
			}
			return fmt.Errorf("%w: %s", catalog.ErrIndexExists, st.Name)
		}
	}
	shadow, err := btree.Create(e.pool)
	if err != nil {
		return err
	}
	putEntry := func(v tuple.Value, pk []byte) error {
		if v.Null {
			return nil
		}
		key, err := tuple.IndexKey(v, pk)
		if err != nil {
			return err
		}
		if len(key) > btree.MaxKeySize {
			return fmt.Errorf("index key too large for column '%s'", st.Column)
		}
		return shadow.Put(key, nil)
	}
	cap := e.startCapture(ti.Name)
	defer cap.stop()
	if err := e.copyTable(ti, func(r *executor.Row) error { return putEntry(r.Values[col], r.Key) }); err != nil {
		return err
	}
	replay := func(evs []binlog.Event) error {
		for _, ev := range evs {
			if ev.Op != binlog.OpPut {
				continue // 删除不需要处理: 索引项只增不删
			}
			vals, err := tuple.Unmarshal(ti.Schema, ev.Row)
			if err != nil {
				return err
			}
			if err := putEntry(vals[col], ev.Key); err != nil {
				return err
			}
		}
		return nil
	}
	return e.finishDDL(ti.Name, st.String(), fromReplication, cap, replay, func(ddlTxn uint64) error {
		return e.cat.AddIndex(ti.Name, &catalog.IndexInfo{Name: st.Name, Column: col, Tree: shadow, ValidFrom: ddlTxn})
	})
}

func (e *Engine) addColumn(st *sql.AlterTableStmt, fromReplication bool) error {
	ti, err := e.cat.GetTable(st.Table)
	if err != nil {
		return err
	}
	if ti.Schema.ColumnIndex(st.Add.Name) >= 0 {
		if fromReplication {
			return nil
		}
		return fmt.Errorf("duplicate column name '%s'", st.Add.Name)
	}
	def := tuple.NullValue(st.Add.Type)
	if st.Default != nil && !st.Default.Value.Null {
		if st.Default.Value.Type != st.Add.Type {
			return fmt.Errorf("invalid default value for column '%s'", st.Add.Name)
		}
		def = st.Default.Value
	}
	oldSchema := ti.Schema
	newSchema := &tuple.Schema{Columns: append(append([]tuple.Column(nil), oldSchema.Columns...),
		tuple.Column{Name: st.Add.Name, Type: st.Add.Type, Len: st.Add.Len})}
	if def.Type == tuple.TypeVarchar && !def.Null && st.Add.Len > 0 && len(def.Str) > st.Add.Len {
		return fmt.Errorf("default value too long for column '%s'", st.Add.Name)
	}
	shadow, err := btree.Create(e.pool)
	if err != nil {
		return err
	}
	convert := func(oldVals []tuple.Value) ([]byte, error) {
		return tuple.Marshal(newSchema, append(append([]tuple.Value(nil), oldVals...), def))
	}
	put := func(key []byte, data []byte) error {
		return shadow.Put(key, txn.RowVersion{TxnID: 0, Data: data}.Encode()) // 事务号 0: 对所有视图可见
	}
	cap := e.startCapture(ti.Name)
	defer cap.stop()
	if err := e.copyTable(ti, func(r *executor.Row) error {
		data, err := convert(r.Values)
		if err != nil {
			return err
		}
		return put(r.Key, data)
	}); err != nil {
		return err
	}
	replay := func(evs []binlog.Event) error {
		for _, ev := range evs {
			switch ev.Op {
			case binlog.OpPut:
				vals, err := tuple.Unmarshal(oldSchema, ev.Row)
				if err != nil {
					return err
				}
				data, err := convert(vals)
				if err != nil {
					return err
				}
				if err := put(ev.Key, data); err != nil {
					return err
				}
			case binlog.OpDelete:
				if _, err := shadow.Delete(ev.Key); err != nil {
					return err
				}
			}
		}
		return nil
	}
	return e.finishDDL(ti.Name, st.String(), fromReplication, cap, replay, func(uint64) error {
		_, err := e.cat.ReplaceTable(ti.Name, newSchema, shadow)
		e.stats.Invalidate(ti.Name)
		return err
	})
}
