package wal

import (
	"errors"
	"fmt"

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
