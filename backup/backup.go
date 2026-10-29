// Package backup 定义热备份的磁盘格式及文件级操作 (清单、增量页文件、WAL 合并、还原)。
// 与引擎交互的部分 (创建备份、PITR) 在 db 包。
//
// 备份目录内容:
//
//	MANIFEST.json  清单
//	data.db        全量备份: 数据文件拷贝 (页 0 最后拷贝)
//	pages.inc      增量备份: 自上次备份以来变化的页 (含页 0)
//	wal            备份结束时的 WAL 拷贝
//	binlog         备份结束时的 Binlog 拷贝
package backup

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"

	"mysqlfake/storage"
	"mysqlfake/wal"
)

const (
	ManifestName = "MANIFEST.json"
	DataFile     = "data.db"
	IncFile      = "pages.inc"
	WALFile      = "wal"
	BinlogFile   = "binlog"
)

type Manifest struct {
	Version  int    `json:"version"`
	ID       string `json:"id"`
	Kind     string `json:"kind"` // "full" | "incremental"
	ParentID string `json:"parent_id,omitempty"`

	StartTime int64  `json:"start_time_unix_nano"`
	EndTime   int64  `json:"end_time_unix_nano"`
	StartLSN  uint64 `json:"start_lsn"` // Fuzzy Checkpoint 的 LSN: 备份开始时的恢复起点
	EndLSN    uint64 `json:"end_lsn"`   // 备份结束时已落盘日志的最大 LSN；下一次增量以它为页 LSN 的分界

	PageSize    int    `json:"page_size"`
	NumPages    uint32 `json:"num_pages"`
	PagesCopied int    `json:"pages_copied"`

	BinlogSeqEnd uint64 `json:"binlog_seq_end"` // 备份结束时的 Binlog 位置: PITR 从其后开始重放
}

var (
	ErrBadChain   = errors.New("backup: invalid backup chain")
	ErrTargetTime = errors.New("backup: recovery target is earlier than the end of the backup")
)

func WriteManifest(dir string, m *Manifest) error {
	b, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, ManifestName), b, 0o644)
}

func ReadManifest(dir string) (*Manifest, error) {
	b, err := os.ReadFile(filepath.Join(dir, ManifestName))
	if err != nil {
		return nil, err
	}
	m := &Manifest{}
	if err := json.Unmarshal(b, m); err != nil {
		return nil, fmt.Errorf("backup: bad manifest in %s: %w", dir, err)
	}
	if m.PageSize != storage.PageSize {
		return nil, fmt.Errorf("backup: page size %d does not match this build (%d)", m.PageSize, storage.PageSize)
	}
	return m, nil
}

// IncWriter 写增量页文件: 重复的 { pageID(4) page(4096) }。
type IncWriter struct{ f *os.File }

func CreateInc(path string) (*IncWriter, error) {
	f, err := os.Create(path)
	return &IncWriter{f}, err
}

func (w *IncWriter) Write(p *storage.Page) error {
	var hdr [4]byte
	binary.LittleEndian.PutUint32(hdr[:], uint32(p.ID))
	if _, err := w.f.Write(hdr[:]); err != nil {
		return err
	}
	_, err := w.f.Write(p.Data[:])
	return err
}

func (w *IncWriter) Close() error {
	if err := w.f.Sync(); err != nil {
		w.f.Close()
		return err
	}
	return w.f.Close()
}

// ForEachIncPage 依次读出增量页文件里的页。
func ForEachIncPage(path string, fn func(p *storage.Page) error) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	for {
		var hdr [4]byte
		if _, err := io.ReadFull(f, hdr[:]); err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		p := &storage.Page{ID: storage.PageID(binary.LittleEndian.Uint32(hdr[:]))}
		if _, err := io.ReadFull(f, p.Data[:]); err != nil {
			return fmt.Errorf("backup: truncated incremental file: %w", err)
		}
		if err := fn(p); err != nil {
			return err
		}
	}
}

// MergeWALs 把多个备份的 WAL 按 LSN 去重合并成一个 WAL 文件。恢复时，页上的 LSN 保证重复/过期的记录被跳过。
func MergeWALs(paths []string, out string) error {
	seen := map[uint64]wal.Record{}
	var max uint64
	for _, p := range paths {
		recs, err := wal.ReadFile(p)
		if err != nil {
			return fmt.Errorf("backup: reading %s: %w", p, err)
		}
		for _, r := range recs {
			seen[r.LSN] = r
			if r.LSN > max {
				max = r.LSN
			}
		}
	}
	merged := make([]wal.Record, 0, len(seen))
	for _, r := range seen {
		merged = append(merged, r)
	}
	sort.Slice(merged, func(i, j int) bool { return merged[i].LSN < merged[j].LSN })
	return wal.WriteFile(out, merged, max+1)
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		return err
	}
	if err := out.Sync(); err != nil {
		out.Close()
		return err
	}
	return out.Close()
}

// Restore 把备份链还原成一个数据库目录 (dst/data.db 及其 .wal/.binlog)。chain[0] 必须是全量备份，
// 其后依次是增量备份。数据库文件在首次打开时由崩溃恢复重放合并后的 WAL，得到一致的状态。
// 返回链上最后一份备份的清单。
func Restore(dst string, chain []string) (*Manifest, error) {
	if len(chain) == 0 {
		return nil, fmt.Errorf("%w: empty", ErrBadChain)
	}
	var manifests []*Manifest
	for i, dir := range chain {
		m, err := ReadManifest(dir)
		if err != nil {
			return nil, err
		}
		switch {
		case i == 0 && m.Kind != "full":
			return nil, fmt.Errorf("%w: the first backup must be a full backup", ErrBadChain)
		case i > 0 && (m.Kind != "incremental" || m.ParentID != manifests[i-1].ID):
			return nil, fmt.Errorf("%w: %s is not the child of %s", ErrBadChain, dir, chain[i-1])
		}
		manifests = append(manifests, m)
	}
	if err := os.MkdirAll(dst, 0o755); err != nil {
		return nil, err
	}
	dataPath := filepath.Join(dst, "data.db")
	if _, err := os.Stat(dataPath); err == nil {
		return nil, fmt.Errorf("backup: %s already exists", dataPath)
	}
	if err := copyFile(filepath.Join(chain[0], DataFile), dataPath); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(dataPath, os.O_RDWR, 0)
	if err != nil {
		return nil, err
	}
	for _, dir := range chain[1:] {
		if err := ForEachIncPage(filepath.Join(dir, IncFile), func(p *storage.Page) error {
			_, err := f.WriteAt(p.Data[:], int64(p.ID)*storage.PageSize)
			return err
		}); err != nil {
			f.Close()
			return nil, err
		}
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return nil, err
	}
	f.Close()
	var wals []string
	for _, dir := range chain {
		wals = append(wals, filepath.Join(dir, WALFile))
	}
	if err := MergeWALs(wals, dataPath+".wal"); err != nil {
		return nil, err
	}
	last := manifests[len(manifests)-1]
	if err := copyFile(filepath.Join(chain[len(chain)-1], BinlogFile), dataPath+".binlog"); err != nil {
		return nil, err
	}
	return last, WriteManifest(dst, last) // 记下还原所基于的最后一份备份，PITR 要用
}

// CopyFile 暴露文件拷贝 (引擎在备份时拷贝 WAL / Binlog 使用)。
func CopyFile(src, dst string) error { return copyFile(src, dst) }
