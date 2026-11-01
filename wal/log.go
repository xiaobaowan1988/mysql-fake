// Package wal 实现预写日志: 日志格式、LogManager (追加/fsync) 与崩溃恢复。
package wal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"sync"

	"mysqlfake/metrics"
	"mysqlfake/storage"
)

type RecType uint8

const (
	RecUpdate RecType = 1 // 页内 [Offset, Offset+len) 字节区间的变化
	RecCommit RecType = 2
	RecAbort  RecType = 3
	RecUndo   RecType = 4 // 逻辑 undo 信息 (Phase 10): 事务在修改数据之前写入，载荷见 UndoInfo
)

// Record 是一条 WAL 日志。Update 记录携带 Before/After 镜像 (等长)。
// Undo 记录只用 After 存放载荷 (UndoInfo 的编码)，没有 Before。
type Record struct {
	LSN    uint64
	TxnID  uint64
	Type   RecType
	PageID storage.PageID
	Offset uint16
	Before []byte
	After  []byte
}

// 文件格式: 文件头 magic(8) baseLSN(8)，随后是连续的记录:
//
//	bodyLen(4) crc32(4) | lsn(8) txn(8) type(1) page(4) offset(2) n(2) before(n) after(n)
//
// Undo 记录没有 before 部分: ... n(2) payload(n)。
const (
	logMagic      = "MYFAKWAL"
	logHeaderSize = 16
	recFixed      = 8 + 8 + 1 + 4 + 2 + 2
)

func (r *Record) encode() []byte {
	n := len(r.After)
	hasBefore := r.Type != RecUndo
	size := recFixed + n
	if hasBefore {
		size += n
	}
	body := make([]byte, recFixed, size)
	binary.LittleEndian.PutUint64(body[0:], r.LSN)
	binary.LittleEndian.PutUint64(body[8:], r.TxnID)
	body[16] = byte(r.Type)
	binary.LittleEndian.PutUint32(body[17:], uint32(r.PageID))
	binary.LittleEndian.PutUint16(body[21:], r.Offset)
	binary.LittleEndian.PutUint16(body[23:], uint16(n))
	if hasBefore {
		body = append(body, r.Before...)
	}
	body = append(body, r.After...)
	out := make([]byte, 8, 8+len(body))
	binary.LittleEndian.PutUint32(out[0:], uint32(len(body)))
	binary.LittleEndian.PutUint32(out[4:], crc32.ChecksumIEEE(body))
	return append(out, body...)
}

func decodeBody(body []byte) (Record, bool) {
	if len(body) < recFixed {
		return Record{}, false
	}
	n := int(binary.LittleEndian.Uint16(body[23:]))
	typ := RecType(body[16])
	want := recFixed + 2*n
	if typ == RecUndo {
		want = recFixed + n
	}
	if len(body) != want {
		return Record{}, false
	}
	r := Record{
		LSN:    binary.LittleEndian.Uint64(body[0:]),
		TxnID:  binary.LittleEndian.Uint64(body[8:]),
		Type:   typ,
		PageID: storage.PageID(binary.LittleEndian.Uint32(body[17:])),
		Offset: binary.LittleEndian.Uint16(body[21:]),
	}
	switch {
	case n == 0:
	case typ == RecUndo:
		r.After = bytes.Clone(body[recFixed:])
	default:
		r.Before = bytes.Clone(body[recFixed : recFixed+n])
		r.After = bytes.Clone(body[recFixed+n:])
	}
	return r, true
}

// UndoInfo 是一条逻辑 undo 记录的持久化形式: 回滚 = 对 Root 指向的 B+ 树做
// Put(Key, OldValue) (Kind 为更新/删除) 或 Delete(Key) (Kind 为插入)，再删除 Idx 中本次新增的索引项。
// 树用根页号标识 (根页号在树的整个生命周期内不变)，因此恢复时不依赖 Catalog。
type UndoInfo struct {
	Kind     uint8 // 与 txn.UndoKind 取值一致: 1 插入 2 更新 3 删除
	Root     storage.PageID
	Key      []byte
	OldValue []byte
	Idx      []UndoIdx
}

// UndoIdx 指向二级索引树中的一个索引项。
type UndoIdx struct {
	Root storage.PageID
	Key  []byte
}

// Marshal 编码: kind(1) root(4) keyLen(2) key valLen(2) val idxCount(2) { root(4) keyLen(2) key }*
func (u *UndoInfo) Marshal() []byte {
	b := make([]byte, 0, 1+4+2+len(u.Key)+2+len(u.OldValue)+2)
	b = append(b, u.Kind)
	b = binary.LittleEndian.AppendUint32(b, uint32(u.Root))
	b = binary.LittleEndian.AppendUint16(b, uint16(len(u.Key)))
	b = append(b, u.Key...)
	b = binary.LittleEndian.AppendUint16(b, uint16(len(u.OldValue)))
	b = append(b, u.OldValue...)
	b = binary.LittleEndian.AppendUint16(b, uint16(len(u.Idx)))
	for _, ix := range u.Idx {
		b = binary.LittleEndian.AppendUint32(b, uint32(ix.Root))
		b = binary.LittleEndian.AppendUint16(b, uint16(len(ix.Key)))
		b = append(b, ix.Key...)
	}
	return b
}

// ParseUndo 解出 Undo 记录的载荷；格式不对返回 false。
func ParseUndo(r *Record) (UndoInfo, bool) {
	if r.Type != RecUndo {
		return UndoInfo{}, false
	}
	b := r.After
	var u UndoInfo
	take := func(n int) ([]byte, bool) {
		if n < 0 || len(b) < n {
			return nil, false
		}
		out := b[:n]
		b = b[n:]
		return out, true
	}
	u16 := func() (int, bool) {
		x, ok := take(2)
		if !ok {
			return 0, false
		}
		return int(binary.LittleEndian.Uint16(x)), true
	}
	hdr, ok := take(5)
	if !ok {
		return UndoInfo{}, false
	}
	u.Kind = hdr[0]
	u.Root = storage.PageID(binary.LittleEndian.Uint32(hdr[1:]))
	kl, ok := u16()
	if !ok {
		return UndoInfo{}, false
	}
	key, ok := take(kl)
	if !ok {
		return UndoInfo{}, false
	}
	u.Key = bytes.Clone(key)
	vl, ok := u16()
	if !ok {
		return UndoInfo{}, false
	}
	val, ok := take(vl)
	if !ok {
		return UndoInfo{}, false
	}
	u.OldValue = bytes.Clone(val)
	cnt, ok := u16()
	if !ok {
		return UndoInfo{}, false
	}
	for i := 0; i < cnt; i++ {
		rt, ok := take(4)
		if !ok {
			return UndoInfo{}, false
		}
		il, ok := u16()
		if !ok {
			return UndoInfo{}, false
		}
		ik, ok := take(il)
		if !ok {
			return UndoInfo{}, false
		}
		u.Idx = append(u.Idx, UndoIdx{Root: storage.PageID(binary.LittleEndian.Uint32(rt)), Key: bytes.Clone(ik)})
	}
	if len(b) != 0 {
		return UndoInfo{}, false
	}
	return u, true
}

var ErrBadLog = errors.New("wal: not a log file")

type LogManager struct {
	mu         sync.Mutex
	path       string
	f          *os.File
	buf        []byte
	nextLSN    uint64
	flushedLSN uint64
	recovered  []Record // 打开时读到的有效记录

	// 每个仍在运行的写事务的第一条 Undo 记录的 LSN。截断日志时不能越过其中最小的那个:
	// 这些事务一旦在崩溃时没有结束，恢复需要用这些 Undo 记录撤销它们。
	firstUndo map[uint64]uint64
}

// Open 打开 (或创建) 日志文件，读取全部有效记录并截掉损坏的尾部。
func Open(path string) (*LogManager, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	lm := &LogManager{f: f, path: path, firstUndo: map[uint64]uint64{}}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	if st.Size() == 0 {
		lm.nextLSN = 1
		if err := lm.resetFile(); err != nil {
			f.Close()
			return nil, err
		}
		return lm, nil
	}
	data := make([]byte, st.Size())
	if _, err := f.ReadAt(data, 0); err != nil && err != io.EOF {
		f.Close()
		return nil, err
	}
	if len(data) < logHeaderSize || string(data[:8]) != logMagic {
		f.Close()
		return nil, ErrBadLog
	}
	lm.nextLSN = binary.LittleEndian.Uint64(data[8:16])
	pos := logHeaderSize
	for pos+8 <= len(data) {
		bl := int(binary.LittleEndian.Uint32(data[pos:]))
		crc := binary.LittleEndian.Uint32(data[pos+4:])
		if pos+8+bl > len(data) {
			break
		}
		body := data[pos+8 : pos+8+bl]
		if crc32.ChecksumIEEE(body) != crc {
			break
		}
		r, ok := decodeBody(body)
		if !ok {
			break
		}
		lm.recovered = append(lm.recovered, r)
		lm.nextLSN = r.LSN + 1
		pos += 8 + bl
	}
	if pos < len(data) { // 丢弃撕裂的尾部
		if err := f.Truncate(int64(pos)); err != nil {
			f.Close()
			return nil, err
		}
	}
	lm.flushedLSN = lm.nextLSN - 1
	return lm, nil
}

func (lm *LogManager) resetFile() error {
	if err := lm.f.Truncate(0); err != nil {
		return err
	}
	var h [logHeaderSize]byte
	copy(h[:8], logMagic)
	binary.LittleEndian.PutUint64(h[8:], lm.nextLSN)
	if _, err := lm.f.WriteAt(h[:], 0); err != nil {
		return err
	}
	lm.flushedLSN = lm.nextLSN - 1
	return lm.f.Sync()
}

// Records 返回打开时读到的记录 (供恢复使用)。
func (lm *LogManager) Records() []Record { return lm.recovered }

// Append 分配 LSN 并把记录追加到内存缓冲区 (尚未落盘)。
func (lm *LogManager) Append(r *Record) uint64 {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	return lm.appendLocked(r)
}

func (lm *LogManager) appendLocked(r *Record) uint64 {
	r.LSN = lm.nextLSN
	lm.nextLSN++
	lm.buf = append(lm.buf, r.encode()...)
	return r.LSN
}

// AppendUndo 追加一条 Undo 记录 (r.Type 被设为 RecUndo)。调用方必须在修改数据页之前调用 (WAL 规则)。
// 同一把锁内登记该事务的第一条 Undo 记录的 LSN: 之后的任何日志截断都不会越过它，直到 EndTxn。
func (lm *LogManager) AppendUndo(r *Record) uint64 {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	r.Type = RecUndo
	lsn := lm.appendLocked(r)
	if _, ok := lm.firstUndo[r.TxnID]; !ok {
		lm.firstUndo[r.TxnID] = lsn
	}
	return lsn
}

// EndTxn 在事务的 Commit/Abort 记录追加之后调用，解除它对日志截断的约束。
func (lm *LogManager) EndTxn(txnID uint64) {
	lm.mu.Lock()
	delete(lm.firstUndo, txnID)
	lm.mu.Unlock()
}

// undoFloorLocked 返回所有活跃写事务中最小的 firstUndo LSN；没有活跃写事务时 ok 为 false。
func (lm *LogManager) undoFloorLocked() (floor uint64, ok bool) {
	for _, l := range lm.firstUndo {
		if !ok || l < floor {
			floor, ok = l, true
		}
	}
	return
}

// Flush 保证 LSN <= upTo 的记录已 fsync 落盘。
func (lm *LogManager) Flush(upTo uint64) error {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	if upTo <= lm.flushedLSN {
		return nil
	}
	return lm.flushLocked()
}

func (lm *LogManager) FlushAll() error {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	return lm.flushLocked()
}

func (lm *LogManager) flushLocked() error {
	if len(lm.buf) > 0 {
		st, err := lm.f.Stat()
		if err != nil {
			return err
		}
		if _, err := lm.f.WriteAt(lm.buf, st.Size()); err != nil {
			return err
		}
		metrics.WALBytes.Add(uint64(len(lm.buf)))
		lm.buf = lm.buf[:0]
	}
	if err := lm.f.Sync(); err != nil {
		return err
	}
	metrics.WALFsyncs.Inc()
	lm.flushedLSN = lm.nextLSN - 1
	return nil
}

func (lm *LogManager) FlushedLSN() uint64 {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	return lm.flushedLSN
}

func (lm *LogManager) NextLSN() uint64 {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	return lm.nextLSN
}

// Truncate 清空日志 (调用方必须保证所有脏页已刷盘)。LSN 继续递增。
// 如果还有活跃的写事务，则保留从它们最早的 Undo 记录开始的全部日志 (其余照常丢弃)。
func (lm *LogManager) Truncate() error {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	lm.recovered = nil
	if floor, ok := lm.undoFloorLocked(); ok {
		_, err := lm.truncateBeforeLocked(floor)
		return err
	}
	lm.buf = lm.buf[:0]
	return lm.resetFile()
}

// Size 返回日志文件当前大小 (含缓冲区中尚未落盘的部分)。
func (lm *LogManager) Size() int64 {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	st, err := lm.f.Stat()
	if err != nil {
		return 0
	}
	return st.Size() + int64(len(lm.buf))
}

// TruncateBefore 丢弃 LSN 小于 lsn 的日志前缀 (Fuzzy Checkpoint 使用)，保留其余记录。
// 截断点不会越过活跃写事务的第一条 Undo 记录。通过写临时文件再原子替换实现。返回丢弃的记录数。
func (lm *LogManager) TruncateBefore(lsn uint64) (int, error) {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	if floor, ok := lm.undoFloorLocked(); ok && floor < lsn {
		lsn = floor
	}
	return lm.truncateBeforeLocked(lsn)
}

func (lm *LogManager) truncateBeforeLocked(lsn uint64) (int, error) {
	if err := lm.flushLocked(); err != nil {
		return 0, err
	}
	data, err := io.ReadAll(io.NewSectionReader(lm.f, 0, 1<<40))
	if err != nil {
		return 0, err
	}
	var keep []byte
	dropped := 0
	pos := logHeaderSize
	for pos+8 <= len(data) {
		bl := int(binary.LittleEndian.Uint32(data[pos:]))
		if pos+8+bl > len(data) {
			break
		}
		body := data[pos+8 : pos+8+bl]
		if crc32.ChecksumIEEE(body) != binary.LittleEndian.Uint32(data[pos+4:]) {
			break
		}
		r, ok := decodeBody(body)
		if !ok {
			break
		}
		if r.LSN >= lsn {
			keep = append(keep, data[pos:pos+8+bl]...)
		} else {
			dropped++
		}
		pos += 8 + bl
	}
	if dropped == 0 {
		return 0, nil
	}
	path := lm.path
	tmp := path + ".tmp"
	var h [logHeaderSize]byte
	copy(h[:8], logMagic)
	binary.LittleEndian.PutUint64(h[8:], lm.nextLSN)
	if err := os.WriteFile(tmp, append(h[:], keep...), 0o644); err != nil {
		return 0, err
	}
	tf, err := os.OpenFile(tmp, os.O_RDWR, 0)
	if err != nil {
		return 0, err
	}
	if err := tf.Sync(); err != nil {
		tf.Close()
		return 0, err
	}
	if err := os.Rename(tmp, path); err != nil {
		tf.Close()
		return 0, err
	}
	lm.f.Close()
	lm.f = tf
	return dropped, nil
}

// Close 刷盘并关闭文件。
func (lm *LogManager) Close() error {
	if err := lm.FlushAll(); err != nil {
		lm.f.Close()
		return err
	}
	return lm.f.Close()
}

// CloseNoFlush 丢弃内存缓冲并关闭文件 (模拟崩溃，仅测试使用)。
func (lm *LogManager) CloseNoFlush() error { return lm.f.Close() }

// ReadFile 以只读方式解析一个 WAL 文件 (不修改它，撕裂的尾部被忽略)，返回有效记录。
func ReadFile(path string) ([]Record, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if len(data) < logHeaderSize || string(data[:8]) != logMagic {
		return nil, ErrBadLog
	}
	var recs []Record
	pos := logHeaderSize
	for pos+8 <= len(data) {
		bl := int(binary.LittleEndian.Uint32(data[pos:]))
		if pos+8+bl > len(data) {
			break
		}
		body := data[pos+8 : pos+8+bl]
		if crc32.ChecksumIEEE(body) != binary.LittleEndian.Uint32(data[pos+4:]) {
			break
		}
		r, ok := decodeBody(body)
		if !ok {
			break
		}
		recs = append(recs, r)
		pos += 8 + bl
	}
	return recs, nil
}

// WriteFile 把记录写成一个新的 WAL 文件 (LSN 必须已经分配好)；nextLSN 是下一个要分配的 LSN。
func WriteFile(path string, recs []Record, nextLSN uint64) error {
	var h [logHeaderSize]byte
	copy(h[:8], logMagic)
	binary.LittleEndian.PutUint64(h[8:], nextLSN)
	buf := h[:]
	for i := range recs {
		buf = append(buf, recs[i].encode()...)
	}
	return os.WriteFile(path, buf, 0o644)
}
