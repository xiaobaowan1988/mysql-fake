package db

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"mysqlfake/backup"
	"mysqlfake/binlog"
	"mysqlfake/storage"
)

// SetBackupHook 设置备份各阶段的测试钩子 (stage: "page" 每拷贝 8 页、"wal")。
func (e *Engine) SetBackupHook(f func(stage string)) {
	e.backupMu.Lock()
	e.backupHook = f
	e.backupMu.Unlock()
}

// Backup 做一次热备份 (在线，不阻塞读写): parentDir 为空是全量备份，否则是相对 parentDir 的增量备份。
//
//	① 暂停 WAL 截断，做 Fuzzy Checkpoint 把脏页刷盘 → ② 拷贝数据页 (全量: 全部；增量: 页 LSN 大于上次备份 EndLSN 的页，
//	校验和不符说明恰好读到撕裂页，重读) → ③ 刷日志，记下 Binlog 位置，拷贝 WAL 与 Binlog → ④ 最后拷贝文件头页。
//
// 备份是"模糊"的 (各页的拷贝时刻不同)，但与 WAL 一起是自洽的: 还原后的崩溃恢复会把每页重放到同一个时间点。
func (e *Engine) Backup(dir, parentDir string) (*backup.Manifest, error) {
	e.backupMu.Lock()
	defer e.backupMu.Unlock()
	hook := func(stage string) {
		if e.backupHook != nil {
			e.backupHook(stage)
		}
	}
	var parent *backup.Manifest
	if parentDir != "" {
		var err error
		if parent, err = backup.ReadManifest(parentDir); err != nil {
			return nil, err
		}
	}
	if _, err := os.Stat(filepath.Join(dir, backup.ManifestName)); err == nil {
		return nil, fmt.Errorf("backup: %s already contains a backup", dir)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	release := e.pool.HoldLogTruncation()
	defer release()

	start := time.Now()
	m := &backup.Manifest{Version: 1, Kind: "full", StartTime: start.UnixNano(), PageSize: storage.PageSize}
	m.ID = fmt.Sprintf("%d", start.UnixNano())
	if parent != nil {
		m.Kind, m.ParentID = "incremental", parent.ID
	}
	var err error
	if m.StartLSN, err = e.pool.FuzzyCheckpoint(); err != nil {
		return nil, err
	}
	nPages := e.disk.NumPages()
	m.NumPages = nPages

	var data *os.File
	var inc *backup.IncWriter
	if parent == nil {
		if data, err = os.Create(filepath.Join(dir, backup.DataFile)); err != nil {
			return nil, err
		}
		defer data.Close()
		if err := data.Truncate(int64(nPages) * storage.PageSize); err != nil {
			return nil, err
		}
	} else {
		if inc, err = backup.CreateInc(filepath.Join(dir, backup.IncFile)); err != nil {
			return nil, err
		}
	}
	var p storage.Page
	for id := storage.PageID(1); uint32(id) < nPages; id++ {
		if err := e.disk.ReadPageConsistent(id, &p); err != nil {
			return nil, fmt.Errorf("backup: page %d: %w", id, err)
		}
		switch {
		case data != nil:
			if _, err := data.WriteAt(p.Data[:], int64(id)*storage.PageSize); err != nil {
				return nil, err
			}
			m.PagesCopied++
		case p.LSN() > parent.EndLSN:
			if err := inc.Write(&p); err != nil {
				return nil, err
			}
			m.PagesCopied++
		}
		if id%8 == 0 {
			hook("page")
		}
	}
	// 结束阶段: 先静默写事务 (恢复只做 redo，WAL 拷贝里不能有未结束事务的修改)；
	// 再记下 Binlog 位置并刷日志: 位置 <= BinlogSeqEnd 的事务，其页修改一定都在已落盘的 WAL 里
	releaseWrites, err := e.mgr.Quiesce(30 * time.Second)
	if err != nil {
		return nil, fmt.Errorf("backup: %w", err)
	}
	defer releaseWrites()
	if e.bl != nil {
		m.BinlogSeqEnd = e.bl.LastSeq()
	}
	m.EndTime = time.Now().UnixNano()
	if err := e.log.FlushAll(); err != nil {
		return nil, err
	}
	m.EndLSN = e.log.NextLSN() - 1
	hook("wal")
	if err := backup.CopyFile(e.path+".wal", filepath.Join(dir, backup.WALFile)); err != nil {
		return nil, err
	}
	binPath := filepath.Join(dir, backup.BinlogFile)
	if e.bl != nil {
		if err := backup.CopyFile(e.path+".binlog", binPath); err != nil {
			return nil, err
		}
	} else if err := os.WriteFile(binPath, nil, 0o644); err != nil {
		return nil, err
	}
	// 文件头页最后拷贝: 它记录页数、空闲链表头、Catalog 根，必须不早于上面任何页的拷贝时刻
	var hdr storage.Page
	if err := e.disk.ReadHeader(&hdr.Data); err != nil {
		return nil, err
	}
	if data != nil {
		if _, err := data.WriteAt(hdr.Data[:], 0); err != nil {
			return nil, err
		}
		if err := data.Sync(); err != nil {
			return nil, err
		}
	} else {
		hdr.ID = 0
		if err := inc.Write(&hdr); err != nil {
			return nil, err
		}
		if err := inc.Close(); err != nil {
			return nil, err
		}
	}
	if err := backup.WriteManifest(dir, m); err != nil {
		return nil, err
	}
	return m, nil
}

// RecoveryTarget 是 PITR 的目标点: Time 或 Seq (Binlog 序号)。两者都为零表示只还原备份本身。
type RecoveryTarget struct {
	Time time.Time
	Seq  uint64
}

func (t RecoveryTarget) isZero() bool { return t.Time.IsZero() && t.Seq == 0 }

// RestoreBackups 把备份链 (全量 + 若干增量) 还原到 dst，并可选地用 binlogPath 指向的 Binlog 重放到目标点 (PITR)。
// 返回重放的事务数。还原出来的数据库在这里被打开一次 (崩溃恢复 + Checkpoint) 后关闭，之后即可正常使用。
func RestoreBackups(dst string, chain []string, binlogPath string, target RecoveryTarget) (int, error) {
	m, err := backup.Restore(dst, chain)
	if err != nil {
		return 0, err
	}
	if !target.isZero() {
		if !target.Time.IsZero() && target.Time.UnixNano() < m.EndTime {
			return 0, fmt.Errorf("%w (target %s, backup ended %s)", backup.ErrTargetTime,
				target.Time.Format(time.RFC3339Nano), time.Unix(0, m.EndTime).Format(time.RFC3339Nano))
		}
		if target.Seq != 0 && target.Seq < m.BinlogSeqEnd {
			return 0, fmt.Errorf("%w (target seq %d < backup binlog position %d)", backup.ErrTargetTime, target.Seq, m.BinlogSeqEnd)
		}
		if binlogPath == "" {
			return 0, errors.New("PITR needs the binlog path")
		}
	}
	eng, err := OpenWithOptions(filepath.Join(dst, "data.db"), Options{CheckpointInterval: -1, StatsInterval: -1})
	if err != nil {
		return 0, err
	}
	applied := 0
	if !target.isZero() {
		recs, rerr := binlog.ReadFile(binlogPath, m.BinlogSeqEnd+1)
		if rerr != nil {
			eng.Close()
			return 0, rerr
		}
		for _, rec := range recs {
			if (!target.Time.IsZero() && rec.TimeNano > target.Time.UnixNano()) || (target.Seq != 0 && rec.Seq > target.Seq) {
				break
			}
			if err := eng.ApplyBinlog(rec); err != nil {
				eng.Close()
				return applied, fmt.Errorf("PITR: replaying binlog seq %d: %w", rec.Seq, err)
			}
			applied++
		}
	}
	return applied, eng.Close()
}
