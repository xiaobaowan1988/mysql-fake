package wal

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestLogAppendReopenTorn(t *testing.T) {
	path := filepath.Join(t.TempDir(), "x.wal")
	lm, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	l1 := lm.Append(&Record{Type: RecUpdate, PageID: 5, Offset: 10, Before: []byte("aaa"), After: []byte("bbb")})
	l2 := lm.Append(&Record{Type: RecCommit, TxnID: 9})
	if l1 != 1 || l2 != 2 {
		t.Fatalf("lsn %d %d", l1, l2)
	}
	if lm.FlushedLSN() != 0 {
		t.Fatal("nothing should be flushed yet")
	}
	if err := lm.Flush(l2); err != nil {
		t.Fatal(err)
	}
	lm.Append(&Record{Type: RecUpdate, PageID: 6, After: []byte("z"), Before: []byte("y")}) // 未刷盘，丢失
	lm.CloseNoFlush()

	lm, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	recs := lm.Records()
	if len(recs) != 2 || recs[0].PageID != 5 || !bytes.Equal(recs[0].After, []byte("bbb")) ||
		recs[0].Offset != 10 || recs[1].TxnID != 9 || recs[1].Type != RecCommit {
		t.Fatalf("bad records: %+v", recs)
	}
	if lm.NextLSN() != 3 {
		t.Fatalf("nextLSN=%d", lm.NextLSN())
	}
	lm.Close()

	// 尾部撕裂: 追加半条垃圾数据，应被截断且不影响已有记录
	f, _ := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	f.Write([]byte{200, 0, 0, 0, 1, 2, 3, 4, 5})
	f.Close()
	lm, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(lm.Records()) != 2 {
		t.Fatal("torn tail should be ignored")
	}
	// 截断后 LSN 继续递增，并且重新打开后保持
	lm.Append(&Record{Type: RecCommit, TxnID: 1})
	lm.FlushAll()
	if err := lm.Truncate(); err != nil {
		t.Fatal(err)
	}
	lsn := lm.Append(&Record{Type: RecCommit, TxnID: 2})
	if lsn != 4 {
		t.Fatalf("LSN must not go back after truncate, got %d", lsn)
	}
	lm.Close()
	lm, _ = Open(path)
	defer lm.Close()
	if len(lm.Records()) != 1 || lm.Records()[0].LSN != 4 || lm.NextLSN() != 5 {
		t.Fatalf("after truncate: %+v next=%d", lm.Records(), lm.NextLSN())
	}
}

func TestBadLogFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bad.wal")
	os.WriteFile(path, []byte("this is not a wal file at all"), 0o644)
	if _, err := Open(path); err != ErrBadLog {
		t.Fatalf("got %v", err)
	}
}
