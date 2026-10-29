package backup

import (
	"os"
	"path/filepath"
	"testing"

	"mysqlfake/storage"
	"mysqlfake/wal"
)

func TestManifestRoundTripAndValidation(t *testing.T) {
	dir := t.TempDir()
	m := &Manifest{Version: 1, ID: "abc", Kind: "incremental", ParentID: "xyz", PageSize: storage.PageSize, EndLSN: 42, BinlogSeqEnd: 7}
	if err := WriteManifest(dir, m); err != nil {
		t.Fatal(err)
	}
	got, err := ReadManifest(dir)
	if err != nil || *got != *m {
		t.Fatalf("%+v %v", got, err)
	}
	m.PageSize = 8192
	WriteManifest(dir, m)
	if _, err := ReadManifest(dir); err == nil {
		t.Fatal("a manifest from a different page size must be rejected")
	}
	if _, err := ReadManifest(t.TempDir()); err == nil {
		t.Fatal("missing manifest")
	}
}

func TestIncrementalPageFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), IncFile)
	w, err := CreateInc(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []storage.PageID{7, 3, 0} {
		p := &storage.Page{ID: id}
		p.Data[100] = byte(id) + 1
		if err := w.Write(p); err != nil {
			t.Fatal(err)
		}
	}
	w.Close()
	var ids []storage.PageID
	if err := ForEachIncPage(path, func(p *storage.Page) error {
		if p.Data[100] != byte(p.ID)+1 {
			t.Fatalf("page %d corrupted", p.ID)
		}
		ids = append(ids, p.ID)
		return nil
	}); err != nil || len(ids) != 3 || ids[0] != 7 || ids[2] != 0 {
		t.Fatalf("%v %v", ids, err)
	}
	// 截断的文件要报错
	b, _ := os.ReadFile(path)
	os.WriteFile(path, b[:len(b)-100], 0o644)
	if err := ForEachIncPage(path, func(*storage.Page) error { return nil }); err == nil {
		t.Fatal("truncated incremental file must be an error")
	}
}

func TestMergeWALsDedupesAndSorts(t *testing.T) {
	dir := t.TempDir()
	mk := func(name string, lsns ...uint64) string {
		var recs []wal.Record
		for _, l := range lsns {
			recs = append(recs, wal.Record{LSN: l, Type: wal.RecUpdate, PageID: 1, Before: []byte{0}, After: []byte{byte(l)}})
		}
		p := filepath.Join(dir, name)
		if err := wal.WriteFile(p, recs, lsns[len(lsns)-1]+1); err != nil {
			t.Fatal(err)
		}
		return p
	}
	a, b := mk("a", 5, 6, 7), mk("b", 7, 8, 9)
	out := filepath.Join(dir, "merged")
	if err := MergeWALs([]string{b, a}, out); err != nil {
		t.Fatal(err)
	}
	recs, err := wal.ReadFile(out)
	if err != nil || len(recs) != 5 {
		t.Fatalf("%d records, %v", len(recs), err)
	}
	for i, r := range recs {
		if r.LSN != uint64(5+i) {
			t.Fatalf("record %d has LSN %d", i, r.LSN)
		}
	}
	lm, err := wal.Open(out) // 合并结果必须是合法的 WAL，LSN 继续递增
	if err != nil {
		t.Fatal(err)
	}
	defer lm.Close()
	if lm.NextLSN() != 10 {
		t.Fatalf("nextLSN=%d", lm.NextLSN())
	}
}
