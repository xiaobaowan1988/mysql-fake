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
