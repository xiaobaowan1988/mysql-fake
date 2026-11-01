package wal

import (
	"bytes"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestUndoRecordRoundTrip(t *testing.T) {
	cases := []UndoInfo{
		{Kind: 1, Root: 7, Key: []byte("k")}, // 插入: 没有旧值、没有索引项
		{Kind: 2, Root: 9, Key: []byte{0x80, 0, 0, 0, 0, 0, 0, 5}, OldValue: bytes.Repeat([]byte{0xAB}, 1041)},
		{Kind: 3, Root: 3, Key: []byte("pk"), OldValue: []byte("old"), Idx: []UndoIdx{{Root: 11, Key: []byte("ik1")}, {Root: 12, Key: bytes.Repeat([]byte{1}, 256)}}},
		{Kind: 2, Root: 1}, // 全空
	}
	path := filepath.Join(t.TempDir(), "u.wal")
	lm, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	for i, c := range cases {
		c := c
		lm.AppendUndo(&Record{TxnID: uint64(100 + i), After: c.Marshal()})
	}
	lm.Append(&Record{Type: RecUpdate, PageID: 5, Offset: 10, Before: []byte("aaa"), After: []byte("bbb")}) // 与 Update 记录混排
	lm.Append(&Record{Type: RecCommit, TxnID: 100})
	if err := lm.FlushAll(); err != nil {
		t.Fatal(err)
	}
	lm.Close()

	recs, err := ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != len(cases)+2 {
		t.Fatalf("got %d records", len(recs))
	}
	for i, c := range cases {
		r := recs[i]
		if r.Type != RecUndo || r.TxnID != uint64(100+i) || len(r.Before) != 0 {
			t.Fatalf("record %d: %+v", i, r)
		}
		got, ok := ParseUndo(&r)
		if !ok {
			t.Fatalf("record %d: ParseUndo failed", i)
		}
		if got.Kind != c.Kind || got.Root != c.Root || !bytes.Equal(got.Key, c.Key) || !bytes.Equal(got.OldValue, c.OldValue) ||
			len(got.Idx) != len(c.Idx) || (len(c.Idx) > 0 && !reflect.DeepEqual(got.Idx, c.Idx)) {
			t.Fatalf("record %d: got %+v want %+v", i, got, c)
		}
	}
	if u := recs[len(cases)]; u.Type != RecUpdate || string(u.Before) != "aaa" || string(u.After) != "bbb" {
		t.Fatalf("update record after undo records damaged: %+v", u)
	}
}

func TestUndoRecordTornTailIsDropped(t *testing.T) {
	path := filepath.Join(t.TempDir(), "u.wal")
	lm, _ := Open(path)
	u := UndoInfo{Kind: 2, Root: 4, Key: []byte("key"), OldValue: []byte("value")}
	lm.AppendUndo(&Record{TxnID: 1, After: u.Marshal()})
	lm.AppendUndo(&Record{TxnID: 1, After: u.Marshal()})
	lm.FlushAll()
	lm.Close()
	st, _ := os.Stat(path)
	os.Truncate(path, st.Size()-5) // 最后一条残缺
	recs, _ := ReadFile(path)
	if len(recs) != 1 {
		t.Fatalf("torn undo record must be ignored, got %d records", len(recs))
	}
}

func TestParseUndoRejectsGarbage(t *testing.T) {
	good := (&UndoInfo{Kind: 2, Root: 4, Key: []byte("key"), OldValue: []byte("v"), Idx: []UndoIdx{{Root: 5, Key: []byte("i")}}}).Marshal()
	for n := 0; n < len(good); n++ { // 任何截断都必须被拒绝，不能 panic
		if _, ok := ParseUndo(&Record{Type: RecUndo, After: good[:n]}); ok {
			t.Fatalf("truncated payload (%d/%d bytes) accepted", n, len(good))
		}
	}
	if _, ok := ParseUndo(&Record{Type: RecUndo, After: append(bytes.Clone(good), 0)}); ok {
		t.Fatal("trailing bytes accepted")
	}
	if _, ok := ParseUndo(&Record{Type: RecUpdate, After: good}); ok {
		t.Fatal("non-undo record accepted")
	}
}

func rec(typ RecType, lsn, txn uint64) Record { return Record{LSN: lsn, TxnID: txn, Type: typ} }

func TestAnalyze(t *testing.T) {
	ids := func(ls []Loser) []uint64 {
		var out []uint64
		for _, l := range ls {
			out = append(out, l.TxnID)
		}
		return out
	}
	cases := []struct {
		name string
		recs []Record
		want []uint64
	}{
		{"empty", nil, nil},
		{"committed", []Record{rec(RecUndo, 1, 5), rec(RecUndo, 2, 5), rec(RecCommit, 3, 5)}, nil},
		{"aborted", []Record{rec(RecUndo, 1, 5), rec(RecAbort, 2, 5)}, nil},
		{"loser", []Record{rec(RecUndo, 1, 5), rec(RecUpdate, 2, 0)}, []uint64{5}},
		{"mixed", []Record{
			rec(RecUndo, 1, 5), rec(RecUndo, 2, 6), rec(RecUndo, 3, 7),
			rec(RecCommit, 4, 6), rec(RecAbort, 5, 7), rec(RecUndo, 6, 5)}, []uint64{5}},
		{"order by first undo", []Record{rec(RecUndo, 1, 9), rec(RecUndo, 2, 3), rec(RecUndo, 3, 9)}, []uint64{9, 3}},
		{"commit without undo is ignored", []Record{rec(RecCommit, 1, 8), rec(RecUpdate, 2, 0)}, nil},
		{"prefix truncated: commit but undo dropped", []Record{rec(RecCommit, 10, 5), rec(RecUndo, 11, 6)}, []uint64{6}},
	}
	for _, c := range cases {
		if got := ids(Analyze(c.recs)); !reflect.DeepEqual(got, c.want) {
			t.Errorf("%s: got %v want %v", c.name, got, c.want)
		}
	}
	ls := Analyze([]Record{rec(RecUndo, 1, 9), rec(RecUndo, 2, 3), rec(RecUndo, 3, 9)})
	if len(ls[0].Undos) != 2 || ls[0].Undos[0].LSN != 1 || ls[0].Undos[1].LSN != 3 {
		t.Fatalf("undo records of loser 9: %+v", ls[0].Undos)
	}
	var lsns []uint64
	for _, r := range PendingUndos(ls) {
		lsns = append(lsns, r.LSN)
	}
	if !reflect.DeepEqual(lsns, []uint64{3, 2, 1}) {
		t.Fatalf("PendingUndos must be in descending LSN order, got %v", lsns)
	}
}

func lsnsOf(recs []Record) []uint64 {
	var out []uint64
	for _, r := range recs {
		out = append(out, r.LSN)
	}
	return out
}

func upd(lm *LogManager, n int) {
	for i := 0; i < n; i++ {
		lm.Append(&Record{Type: RecUpdate, PageID: 1, Before: []byte("a"), After: []byte("b")})
	}
}

// 活跃写事务的 Undo 记录 (及其之后的全部日志) 不能被 Fuzzy Checkpoint 的截断丢掉；事务结束后才能丢。
func TestTruncateBeforeKeepsActiveTxnUndo(t *testing.T) {
	path := filepath.Join(t.TempDir(), "f.wal")
	lm, _ := Open(path)
	upd(lm, 3)                                                                                         // LSN 1..3
	lm.AppendUndo(&Record{TxnID: 7, After: (&UndoInfo{Kind: 2, Root: 1, Key: []byte("k")}).Marshal()}) // LSN 4: 事务 7 的第一条
	upd(lm, 2)                                                                                         // LSN 5..6
	lm.AppendUndo(&Record{TxnID: 7, After: (&UndoInfo{Kind: 2, Root: 1, Key: []byte("k")}).Marshal()}) // LSN 7: 同一事务的第二条，不改变下界

	n, err := lm.TruncateBefore(100) // 请求丢掉全部
	if err != nil || n != 3 {
		t.Fatalf("dropped %d (err %v), want 3: LSN 1..3 only", n, err)
	}
	recs, _ := ReadFile(path)
	if got := lsnsOf(recs); !reflect.DeepEqual(got, []uint64{4, 5, 6, 7}) {
		t.Fatalf("kept %v, want [4 5 6 7]", got)
	}
	if ls := Analyze(recs); len(ls) != 1 || ls[0].TxnID != 7 || len(ls[0].Undos) != 2 {
		t.Fatalf("the active txn must still be recoverable as a loser after truncation: %+v", ls)
	}

	lm.Append(&Record{Type: RecCommit, TxnID: 7})
	lm.EndTxn(7)
	if _, err := lm.TruncateBefore(100); err != nil {
		t.Fatal(err)
	}
	recs, _ = ReadFile(path)
	if len(recs) != 0 {
		t.Fatalf("after the txn ended everything should be droppable, kept %v", lsnsOf(recs))
	}
	lm.Close()
}

// 下界取所有活跃写事务中最小的那个；最早的事务结束后，下界前移。
func TestUndoFloorIsMinimumOverActiveTxns(t *testing.T) {
	path := filepath.Join(t.TempDir(), "m.wal")
	lm, _ := Open(path)
	u := (&UndoInfo{Kind: 1, Root: 1, Key: []byte("k")}).Marshal()
	upd(lm, 1)                                  // 1
	lm.AppendUndo(&Record{TxnID: 20, After: u}) // 2
	upd(lm, 1)                                  // 3
	lm.AppendUndo(&Record{TxnID: 10, After: u}) // 4
	lm.AppendUndo(&Record{TxnID: 20, After: u}) // 5
	lm.TruncateBefore(100)
	recs, _ := ReadFile(path)
	if got := lsnsOf(recs); got[0] != 2 {
		t.Fatalf("floor must be txn 20's first undo (LSN 2), kept %v", got)
	}
	lm.Append(&Record{Type: RecCommit, TxnID: 20})
	lm.EndTxn(20)
	lm.TruncateBefore(100)
	recs, _ = ReadFile(path)
	if got := lsnsOf(recs); got[0] != 4 {
		t.Fatalf("after txn 20 ended the floor must move to txn 10's first undo (LSN 4), kept %v", got)
	}
	lm.Close()
}

// Sharp Checkpoint 的 Truncate: 没有活跃写事务时清空；有活跃写事务时保留它们的日志，
// 并且缓冲区里尚未落盘的记录不能被丢掉。
func TestTruncateKeepsActiveTxnUndoAndUnflushedTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "s.wal")
	lm, _ := Open(path)
	upd(lm, 2)                                                                                         // 1..2
	lm.AppendUndo(&Record{TxnID: 5, After: (&UndoInfo{Kind: 1, Root: 1, Key: []byte("k")}).Marshal()}) // 3
	upd(lm, 1)                                                                                         // 4: 留在缓冲区里，没有 Flush
	if err := lm.Truncate(); err != nil {
		t.Fatal(err)
	}
	recs, _ := ReadFile(path)
	if got := lsnsOf(recs); !reflect.DeepEqual(got, []uint64{3, 4}) {
		t.Fatalf("kept %v, want [3 4] (undo record and the unflushed record after it)", got)
	}
	lm.Append(&Record{Type: RecAbort, TxnID: 5})
	lm.EndTxn(5)
	if err := lm.Truncate(); err != nil {
		t.Fatal(err)
	}
	if recs, _ = ReadFile(path); len(recs) != 0 {
		t.Fatalf("no active txn: log must be empty, got %v", lsnsOf(recs))
	}
	if lm.NextLSN() != 6 { // 1..4 加上 Abort 的 5
		t.Fatalf("LSNs must keep increasing across truncation, next=%d", lm.NextLSN())
	}
	lm.Close()
}
