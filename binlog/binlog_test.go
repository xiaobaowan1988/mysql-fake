package binlog

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func sample(id uint64) *Txn {
	return &Txn{TxnID: id, Origin: 2, Events: []Event{
		{Op: OpPut, Table: "t", Key: []byte{1, 2}, Row: []byte("row")},
		{Op: OpDelete, Table: "t", Key: []byte{3}},
		{Op: OpDDL, SQL: "CREATE TABLE x (a INT PRIMARY KEY)"},
	}}
}

func TestEncodeDecode(t *testing.T) {
	in := sample(7)
	in.TimeNano = -5
	out, err := DecodeTxn(in.Encode())
	if err != nil {
		t.Fatal(err)
	}
	// nil 与空切片等价
	for i := range out.Events {
		if len(out.Events[i].Row) == 0 {
			out.Events[i].Row = nil
		}
		if len(out.Events[i].Key) == 0 {
			out.Events[i].Key = nil
		}
		if len(in.Events[i].Row) == 0 {
			in.Events[i].Row = nil
		}
		if len(in.Events[i].Key) == 0 {
			in.Events[i].Key = nil
		}
	}
	if !reflect.DeepEqual(in, out) {
		t.Fatalf("%+v\n%+v", in, out)
	}
	if _, err := DecodeTxn(in.Encode()[:5]); err == nil {
		t.Fatal("expected corrupt")
	}
}

func TestLogAppendReadReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "x.binlog")
	l, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	l.NoSync = true
	for i := uint64(1); i <= 5; i++ {
		if err := l.Append(sample(i * 10)); err != nil {
			t.Fatal(err)
		}
	}
	if l.LastSeq() != 5 {
		t.Fatal(l.LastSeq())
	}
	l.Close()

	// 尾部撕裂
	f, _ := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	f.Write([]byte{9, 9, 9, 9, 1})
	f.Close()

	l, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	if l.LastSeq() != 5 {
		t.Fatalf("lastSeq=%d", l.LastSeq())
	}
	recs, err := l.ReadFrom(3)
	if err != nil || len(recs) != 3 || recs[0].Seq != 3 || recs[2].TxnID != 50 || recs[0].TimeNano == 0 {
		t.Fatalf("%v %v", recs, err)
	}
	if err := l.Append(sample(60)); err != nil || l.LastSeq() != 6 {
		t.Fatal("append after reopen failed")
	}
}
