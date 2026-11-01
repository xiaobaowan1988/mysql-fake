package wal

import (
	"errors"
	"fmt"
	"sort"

	"mysqlfake/storage"
)

// Loser 是崩溃时尚未结束 (日志里没有它的 Commit/Abort) 的事务，及其全部 Undo 记录 (按 LSN 升序)。
type Loser struct {
	TxnID uint64
	Undos []Record
}

// Analyze 是恢复的第一阶段: 顺序扫描日志，出现 Undo 记录的事务进入活跃表，遇到它的 Commit/Abort 则移出，
// 扫描结束仍在表中的就是失败者 (按第一条 Undo 记录的 LSN 排序)。
// 有 Abort 记录的事务不算失败者: 回滚过程对 B+ 树的修改也记了日志，重做历史就得到回滚后的状态。
func Analyze(recs []Record) []Loser {
	idx := map[uint64]int{} // txn -> 在 active 中的下标
	var active []*Loser
	for i := range recs {
		r := &recs[i]
		switch r.Type {
		case RecUndo:
			j, ok := idx[r.TxnID]
			if !ok {
				j = len(active)
				idx[r.TxnID] = j
				active = append(active, &Loser{TxnID: r.TxnID})
			}
			active[j].Undos = append(active[j].Undos, *r)
		case RecCommit, RecAbort:
			if j, ok := idx[r.TxnID]; ok {
				active[j] = nil
				delete(idx, r.TxnID)
			}
		}
	}
	var out []Loser
	for _, l := range active {
		if l != nil {
			out = append(out, *l)
		}
	}
	return out
}

// PendingUndos 把所有失败者的 Undo 记录合并，按 LSN 从大到小排列 (撤销的执行顺序)。
func PendingUndos(losers []Loser) []Record {
	var all []Record
	for _, l := range losers {
		all = append(all, l.Undos...)
	}
	sort.Slice(all, func(i, j int) bool { return all[i].LSN > all[j].LSN })
	return all
}

// Recover 正向回放日志中的 Update 记录。页上的 LSN >= 记录 LSN 说明该修改已落盘，跳过，
// 因此重放是幂等的。完成后刷盘并截断日志。返回实际重放的记录数。
func Recover(disk *storage.DiskManager, lm *LogManager) (int, error) {
	recs := lm.Records()
	if len(recs) == 0 {
		return 0, nil
	}
	pages := map[storage.PageID]*storage.Page{}
	// 每页的第一条日志是否是整页镜像: 是的话页的旧内容 (即使损坏) 无关紧要。
	firstFull := map[storage.PageID]bool{}
	for i := range recs {
		if r := &recs[i]; r.Type == RecUpdate {
			if _, seen := firstFull[r.PageID]; !seen {
				firstFull[r.PageID] = r.Offset == 0 && len(r.After) == storage.PageSize
			}
		}
	}
	redone := 0
	for i := range recs {
		r := &recs[i]
		if r.Type != RecUpdate {
			continue
		}
		p, ok := pages[r.PageID]
		if !ok {
			p = &storage.Page{}
			if err := disk.ReadPageRaw(r.PageID, p); err != nil {
				if !errors.Is(err, storage.ErrChecksum) || !firstFull[r.PageID] {
					return 0, fmt.Errorf("wal: unrecoverable corrupt page %d: %w", r.PageID, err)
				}
				p.Data = [storage.PageSize]byte{} // 将被整页镜像完全覆盖
				p.ID = r.PageID
			}
			pages[r.PageID] = p
		}
		if p.LSN() >= r.LSN {
			continue
		}
		copy(p.Data[r.Offset:], r.After)
		p.SetLSN(r.LSN)
		redone++
	}
	for _, p := range pages {
		if err := disk.WritePage(p); err != nil {
			return 0, err
		}
	}
	if err := disk.Sync(); err != nil {
		return 0, err
	}
	return redone, lm.Truncate()
}
