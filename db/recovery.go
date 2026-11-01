package db

import (
	"fmt"

	"mysqlfake/btree"
	"mysqlfake/buffer"
	"mysqlfake/txn"
	"mysqlfake/wal"
)

// RecoveryStats 描述打开数据库时崩溃恢复做了什么。
type RecoveryStats struct {
	Redone int // Redo 阶段实际重放的页更新记录数
	Losers int // Analysis 找出的失败者事务数 (崩溃时未结束的事务)
	Undone int // Undo 阶段执行的撤销记录数
}

// RecoveryStats 返回本次打开时的恢复统计。
func (e *Engine) RecoveryStats() RecoveryStats { return e.recovery }

// undoStepHook 是测试钩子: 在每一条撤销记录执行之前调用，返回错误则中止恢复 (用来模拟撤销过程中崩溃)。
// 钩子拿到缓冲池和日志，可以先把已完成的那部分撤销刷盘，模拟"崩溃时撤销只做了一半、且已经落盘"。
var undoStepHook func(step int, pool *buffer.Pool, lm *wal.LogManager) error

// undoLosers 是恢复的第三阶段 (Undo): 按 LSN 从大到小对失败者的每条 Undo 记录做逻辑撤销，
// 然后给每个失败者写 Abort 记录并把日志刷盘。
//
// 撤销都是"绝对值"写入 (写回旧值 / 删除键 / 删除本次新增的索引项)，因此重复执行是幂等的:
// 如果撤销过程中再次崩溃，失败者仍没有 Abort 记录，下次恢复会从头撤销一遍，结果相同。
// 撤销期间对页的修改走缓冲池，同样记了日志 (Redo 能重做它们)；Abort 排在它们之后，所以
// 只要 Abort 已落盘，撤销的修改就一定能被重做出来。
func undoLosers(pool *buffer.Pool, lm *wal.LogManager, losers []wal.Loser) (int, error) {
	n := 0
	for _, r := range wal.PendingUndos(losers) {
		info, ok := wal.ParseUndo(&r)
		if !ok {
			return n, fmt.Errorf("recovery: corrupt undo record at LSN %d (txn %d)", r.LSN, r.TxnID)
		}
		if undoStepHook != nil {
			if err := undoStepHook(n, pool, lm); err != nil {
				return n, err
			}
		}
		if err := applyUndo(pool, info); err != nil {
			return n, fmt.Errorf("recovery: undo of txn %d (LSN %d) failed: %w", r.TxnID, r.LSN, err)
		}
		n++
	}
	if len(losers) == 0 {
		return 0, nil
	}
	for _, l := range losers {
		pool.Abort(l.TxnID)
	}
	return n, lm.FlushAll()
}

// applyUndo 与运行期的 Txn.RollbackTo 做同一件事，只是树是按根页号打开的。
func applyUndo(pool *buffer.Pool, u wal.UndoInfo) error {
	tree := btree.Open(pool, u.Root)
	var err error
	if txn.UndoKind(u.Kind) == txn.UndoInsert {
		_, err = tree.Delete(u.Key)
	} else {
		err = tree.Put(u.Key, u.OldValue)
	}
	if err != nil {
		return err
	}
	for _, ix := range u.Idx {
		if _, err := btree.Open(pool, ix.Root).Delete(ix.Key); err != nil {
			return err
		}
	}
	return nil
}
