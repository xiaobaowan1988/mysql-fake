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

	"mysqlfake/storage"
)

type RecType uint8

const (
	RecUpdate RecType = 1 // 页内 [Offset, Offset+len) 字节区间的变化
	RecCommit RecType = 2
	RecAbort  RecType = 3
)

// Record 是一条 WAL 日志。Update 记录携带 Before/After 镜像 (等长)。
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
const (
	logMagic      = "MYFAKWAL"
	logHeaderSize = 16
	recFixed      = 8 + 8 + 1 + 4 + 2 + 2
)

func (r *Record) encode() []byte {
	n := len(r.After)
	body := make([]byte, recFixed, recFixed+2*n)
	binary.LittleEndian.PutUint64(body[0:], r.LSN)
	binary.LittleEndian.PutUint64(body[8:], r.TxnID)
	body[16] = byte(r.Type)
	binary.LittleEndian.PutUint32(body[17:], uint32(r.PageID))
	binary.LittleEndian.PutUint16(body[21:], r.Offset)
	binary.LittleEndian.PutUint16(body[23:], uint16(n))
	body = append(body, r.Before...)
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
	if len(body) != recFixed+2*n {
		return Record{}, false
	}
	r := Record{
		LSN:    binary.LittleEndian.Uint64(body[0:]),
		TxnID:  binary.LittleEndian.Uint64(body[8:]),
		Type:   RecType(body[16]),
		PageID: storage.PageID(binary.LittleEndian.Uint32(body[17:])),
		Offset: binary.LittleEndian.Uint16(body[21:]),
	}
	if n > 0 {
		r.Before = bytes.Clone(body[recFixed : recFixed+n])
		r.After = bytes.Clone(body[recFixed+n:])
	}
	return r, true
}

var ErrBadLog = errors.New("wal: not a log file")

type LogManager struct {
	mu         sync.Mutex
	f          *os.File
	buf        []byte
	nextLSN    uint64
	flushedLSN uint64
	recovered  []Record // 打开时读到的有效记录
}

// Open 打开 (或创建) 日志文件，读取全部有效记录并截掉损坏的尾部。
func Open(path string) (*LogManager, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	lm := &LogManager{f: f}
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
	r.LSN = lm.nextLSN
	lm.nextLSN++
	lm.buf = append(lm.buf, r.encode()...)
	return r.LSN
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
		lm.buf = lm.buf[:0]
	}
	if err := lm.f.Sync(); err != nil {
		return err
	}
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
func (lm *LogManager) Truncate() error {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	lm.buf = lm.buf[:0]
	lm.recovered = nil
	return lm.resetFile()
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
