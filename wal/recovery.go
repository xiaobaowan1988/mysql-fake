package wal

import (
	"mysqlfake/storage"
)

// Recover 正向回放日志中的 Update 记录。页上的 LSN >= 记录 LSN 说明该修改已落盘，跳过，
// 因此重放是幂等的。完成后刷盘并截断日志。返回实际重放的记录数。
func Recover(disk *storage.DiskManager, lm *LogManager) (int, error) {
	recs := lm.Records()
	if len(recs) == 0 {
		return 0, nil
	}
	pages := map[storage.PageID]*storage.Page{}
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
				return 0, err
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
