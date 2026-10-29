package raft

import (
	"encoding/binary"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
)

// store 持久化 Raft 状态: raft.state (term/votedFor，原子替换) 与 raft.log (追加写的日志)。
type store struct {
	dir  string
	logf *os.File
	size int64
}

func openStore(dir string) (*store, uint64, int, []Entry, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, 0, 0, nil, err
	}
	s := &store{dir: dir}
	var term uint64
	var voted int
	if b, err := os.ReadFile(filepath.Join(dir, "raft.state")); err == nil && len(b) == 16 {
		term = binary.LittleEndian.Uint64(b)
		voted = int(int64(binary.LittleEndian.Uint64(b[8:])))
	}
	f, err := os.OpenFile(filepath.Join(dir, "raft.log"), os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, 0, 0, nil, err
	}
	s.logf = f
	data, err := io.ReadAll(f)
	if err != nil {
		return nil, 0, 0, nil, err
	}
	var entries []Entry
	pos := 0
	for {
		e, n := parseEntry(data[pos:])
		if n == 0 {
			break
		}
		entries = append(entries, e)
		pos += n
	}
	if pos < len(data) {
		if err := f.Truncate(int64(pos)); err != nil {
			return nil, 0, 0, nil, err
		}
	}
	s.size = int64(pos)
	return s, term, voted, entries, nil
}

func encodeEntry(e Entry) []byte {
	body := binary.LittleEndian.AppendUint64(nil, e.Term)
	body = binary.LittleEndian.AppendUint64(body, e.Index)
	if e.Noop {
		body = append(body, 1)
	} else {
		body = append(body, 0)
	}
	body = append(body, e.Data...)
	out := binary.LittleEndian.AppendUint32(nil, uint32(len(body)))
	out = binary.LittleEndian.AppendUint32(out, crc32.ChecksumIEEE(body))
	return append(out, body...)
}

func parseEntry(b []byte) (Entry, int) {
	if len(b) < 8 {
		return Entry{}, 0
	}
	n := int(binary.LittleEndian.Uint32(b))
	if n < 17 || 8+n > len(b) || crc32.ChecksumIEEE(b[8:8+n]) != binary.LittleEndian.Uint32(b[4:]) {
		return Entry{}, 0
	}
	body := b[8 : 8+n]
	return Entry{
		Term:  binary.LittleEndian.Uint64(body),
		Index: binary.LittleEndian.Uint64(body[8:]),
		Noop:  body[16] == 1,
		Data:  append([]byte(nil), body[17:]...),
	}, 8 + n
}

func (s *store) saveState(term uint64, voted int) error {
	b := binary.LittleEndian.AppendUint64(nil, term)
	b = binary.LittleEndian.AppendUint64(b, uint64(int64(voted)))
	tmp := filepath.Join(s.dir, "raft.state.tmp")
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(b); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	f.Close()
	return os.Rename(tmp, filepath.Join(s.dir, "raft.state"))
}

func (s *store) append(es []Entry) error {
	var buf []byte
	for _, e := range es {
		buf = append(buf, encodeEntry(e)...)
	}
	if _, err := s.logf.WriteAt(buf, s.size); err != nil {
		return err
	}
	s.size += int64(len(buf))
	return s.logf.Sync()
}

// rewrite 用给定条目整体替换日志 (冲突截断时使用)。
func (s *store) rewrite(es []Entry) error {
	if err := s.logf.Truncate(0); err != nil {
		return err
	}
	s.size = 0
	return s.append(es)
}

func (s *store) close() error { return s.logf.Close() }
