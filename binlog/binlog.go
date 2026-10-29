// Package binlog 实现行级逻辑日志 (Binlog)。
// 一条记录 (Txn) 是一个已提交事务产生的全部行事件；事件是幂等的:
// 按序重放 Put/Delete 的最终结果与原库一致，因此可用于复制、重启重放和 PITR。
package binlog

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"sync"
	"time"
)

type Op uint8

const (
	OpPut    Op = 1 // 写入 (插入或覆盖) 一行
	OpDelete Op = 2 // 删除一行
	OpDDL    Op = 3 // DDL 语句文本
)

type Event struct {
	Op    Op
	Table string
	Key   []byte // 主键编码
	Row   []byte // OpPut: 行数据 (按当时表结构序列化的元组)
	SQL   string // OpDDL
}

// Txn 是一个已提交事务。
type Txn struct {
	Seq      uint64 // 本地 Binlog 序号 (追加时分配，从 1 开始)
	TxnID    uint64
	Origin   int   // 产生该事务的节点 (集群中使用)
	TimeNano int64 // 提交时间 (Unix 纳秒)
	Events   []Event
}

func putBytes(b, v []byte) []byte {
	b = binary.AppendUvarint(b, uint64(len(v)))
	return append(b, v...)
}

// Encode 序列化事务 (不含 Seq，Seq 由本地日志分配)。
func (t *Txn) Encode() []byte {
	b := binary.AppendUvarint(nil, t.TxnID)
	b = binary.AppendUvarint(b, uint64(t.Origin))
	b = binary.AppendVarint(b, t.TimeNano)
	b = binary.AppendUvarint(b, uint64(len(t.Events)))
	for _, e := range t.Events {
		b = append(b, byte(e.Op))
		b = putBytes(b, []byte(e.Table))
		b = putBytes(b, e.Key)
		b = putBytes(b, e.Row)
		b = putBytes(b, []byte(e.SQL))
	}
	return b
}

var ErrCorrupt = errors.New("binlog: corrupt record")

type reader struct {
	b   []byte
	err error
}

func (r *reader) uvarint() uint64 {
	v, n := binary.Uvarint(r.b)
	if n <= 0 {
		r.err = ErrCorrupt
		return 0
	}
	r.b = r.b[n:]
	return v
}

func (r *reader) varint() int64 {
	v, n := binary.Varint(r.b)
	if n <= 0 {
		r.err = ErrCorrupt
		return 0
	}
	r.b = r.b[n:]
	return v
}

func (r *reader) bytes() []byte {
	n := r.uvarint()
	if r.err != nil || uint64(len(r.b)) < n {
		r.err = ErrCorrupt
		return nil
	}
	v := append([]byte(nil), r.b[:n]...)
	r.b = r.b[n:]
	return v
}

func DecodeTxn(b []byte) (*Txn, error) {
	r := &reader{b: b}
	t := &Txn{TxnID: r.uvarint(), Origin: int(r.uvarint()), TimeNano: r.varint()}
	n := r.uvarint()
	for i := uint64(0); i < n && r.err == nil; i++ {
		if len(r.b) < 1 {
			return nil, ErrCorrupt
		}
		e := Event{Op: Op(r.b[0])}
		r.b = r.b[1:]
		e.Table = string(r.bytes())
		e.Key = r.bytes()
		e.Row = r.bytes()
		e.SQL = string(r.bytes())
		t.Events = append(t.Events, e)
	}
	if r.err != nil {
		return nil, r.err
	}
	return t, nil
}

// Log 是追加写的 Binlog 文件。记录格式: len(4) crc(4) | seq(8) | txn。
type Log struct {
	mu      sync.Mutex
	f       *os.File
	size    int64
	nextSeq uint64
	NoSync  bool // 测试用: 不 fsync
}

func Open(path string) (*Log, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	l := &Log{f: f, nextSeq: 1}
	data, err := io.ReadAll(f)
	if err != nil {
		f.Close()
		return nil, err
	}
	pos := 0
	for {
		t, n := parseRecord(data[pos:])
		if t == nil {
			break
		}
		l.nextSeq = t.Seq + 1
		pos += n
	}
	if pos < len(data) {
		if err := f.Truncate(int64(pos)); err != nil {
			f.Close()
			return nil, err
		}
	}
	l.size = int64(pos)
	return l, nil
}

func parseRecord(b []byte) (*Txn, int) {
	if len(b) < 8 {
		return nil, 0
	}
	n := int(binary.LittleEndian.Uint32(b))
	if n < 8 || 8+n > len(b) || crc32.ChecksumIEEE(b[8:8+n]) != binary.LittleEndian.Uint32(b[4:]) {
		return nil, 0
	}
	t, err := DecodeTxn(b[16 : 8+n])
	if err != nil {
		return nil, 0
	}
	t.Seq = binary.LittleEndian.Uint64(b[8:])
	return t, 8 + n
}

// Append 分配序号并追加一条记录 (默认 fsync)。
func (l *Log) Append(t *Txn) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if t.TimeNano == 0 {
		t.TimeNano = time.Now().UnixNano()
	}
	t.Seq = l.nextSeq
	body := binary.LittleEndian.AppendUint64(nil, t.Seq)
	body = append(body, t.Encode()...)
	rec := binary.LittleEndian.AppendUint32(nil, uint32(len(body)))
	rec = binary.LittleEndian.AppendUint32(rec, crc32.ChecksumIEEE(body))
	rec = append(rec, body...)
	if _, err := l.f.WriteAt(rec, l.size); err != nil {
		return err
	}
	if !l.NoSync {
		if err := l.f.Sync(); err != nil {
			return err
		}
	}
	l.size += int64(len(rec))
	l.nextSeq++
	return nil
}

// LastSeq 返回已写入的最大序号 (空日志为 0)。
func (l *Log) LastSeq() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.nextSeq - 1
}

// ReadFrom 返回序号 >= seq 的全部记录。
func (l *Log) ReadFrom(seq uint64) ([]*Txn, error) {
	l.mu.Lock()
	size := l.size
	l.mu.Unlock()
	data := make([]byte, size)
	if _, err := l.f.ReadAt(data, 0); err != nil && err != io.EOF {
		return nil, err
	}
	var out []*Txn
	for pos := 0; pos < len(data); {
		t, n := parseRecord(data[pos:])
		if t == nil {
			return nil, ErrCorrupt
		}
		if t.Seq >= seq {
			out = append(out, t)
		}
		pos += n
	}
	return out, nil
}

func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.f.Close()
}
